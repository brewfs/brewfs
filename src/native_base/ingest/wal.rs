//! The local session journal: `BNWL` records with CRC and strict sequence
//! (spec 08 §9).
//!
//! Record layout (all fixed-width integers little-endian, matching the
//! container CRC convention from spec 02):
//!
//! ```text
//! "BNWL"[4] + record_len:u32 + sequence:u64 + kind:u16 + reserved:u16
//!         + payload + crc32c:u32
//! ```
//!
//! `record_len` covers the entire record including itself and the trailing
//! CRC, so the minimum is 24 bytes (empty payload) and the maximum is 16
//! MiB. The CRC covers everything before it. Sequences start at 1 and
//! increase strictly.
//!
//! Replay is bounded (spec 08 §9): only a *final* record that is truncated
//! or fails its CRC may be dropped. Damage in the middle — a bad record
//! with more bytes after it, a sequence gap, or a bad magic/length — stops
//! the replay with [`IngestError::CorruptJournal`]; records are never
//! skipped silently.
//!
//! Checkpoints switch WAL generations: the checkpoint is written
//! temp → fsync → rename → fsync(parent), and only then does the journal
//! restart as an empty file whose first record is a
//! [`Kind::GENERATION_BEGIN`] marker. On replay, a checkpoint whose
//! generation is newer than the journal's marker means the journal file
//! still holds pre-switch records that the checkpoint already compacted —
//! those records are ignored and the generation is re-switched.

use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::native_base::wire::container::crc32c;

use super::error::IngestError;

pub const MAGIC: [u8; 4] = *b"BNWL";
/// Header before the payload: magic(4) + len(4) + sequence(8) + kind(2) +
/// reserved(2).
pub const RECORD_HEADER_LEN: usize = 20;
/// Magic + header + CRC with an empty payload.
pub const MIN_RECORD_LEN: u32 = 24;
pub const MAX_RECORD_LEN: u32 = 16 << 20;

/// Well-known record kinds. The journal layer only owns
/// [`Kind::GENERATION_BEGIN`]; the session layer defines the rest.
pub mod kind {
    pub const GENERATION_BEGIN: u16 = 0;
}

/// One parsed journal record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecord {
    pub sequence: u64,
    pub kind: u16,
    pub payload: Vec<u8>,
}

/// Why a replay stopped at the tail. `Dropped` is the bounded recovery the
/// spec allows; everything else is an error already returned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayOutcome {
    /// All records parsed.
    Complete,
    /// The final record was truncated or CRC-bad and was dropped.
    DroppedTail { dropped_sequence: Option<u64> },
}

/// The result of a journal replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Replay {
    pub records: Vec<WalRecord>,
    pub outcome: ReplayOutcome,
}

/// Replay a journal file. `path` may be missing — that is an empty journal,
/// not an error (a session that never appended).
pub fn replay(path: &Path) -> Result<Replay, IngestError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Replay {
                records: Vec::new(),
                outcome: ReplayOutcome::Complete,
            });
        }
        Err(err) => return Err(IngestError::Backend(format!("read journal: {err}"))),
    };
    replay_bytes(&bytes)
}

/// Replay rules over an in-memory journal image (used to test the
/// truncation/CRC rules without touching disks).
pub fn replay_bytes(bytes: &[u8]) -> Result<Replay, IngestError> {
    let mut records = Vec::new();
    let mut offset = 0usize;
    let mut expected_sequence: u64 = 0;
    while offset < bytes.len() {
        let record = match parse_record(&bytes[offset..]) {
            Ok(record) => record,
            Err(TailFault::Truncated) => {
                // An incomplete record necessarily runs to EOF: the only
                // truncation case the spec allows to drop (spec 08 §9).
                return Ok(Replay {
                    records,
                    outcome: ReplayOutcome::DroppedTail {
                        dropped_sequence: None,
                    },
                });
            }
            Err(TailFault::CrcBad {
                record_len,
                sequence,
            }) => {
                // A CRC-bad record is droppable only when nothing follows
                // its declared extent.
                return if offset + record_len >= bytes.len() {
                    Ok(Replay {
                        records,
                        outcome: ReplayOutcome::DroppedTail {
                            dropped_sequence: sequence,
                        },
                    })
                } else {
                    Err(IngestError::CorruptJournal(format!(
                        "record at offset {offset}: CRC mismatch with more data following"
                    )))
                };
            }
            Err(TailFault::BadMagic) => {
                return Err(IngestError::CorruptJournal(format!(
                    "record at offset {offset}: bad magic"
                )));
            }
            Err(TailFault::BadLength(len)) => {
                return Err(IngestError::CorruptJournal(format!(
                    "record at offset {offset}: record_len {len} out of bounds"
                )));
            }
        };
        let consumed = record.consumed;
        expected_sequence += 1;
        if record.record.sequence != expected_sequence {
            return Err(IngestError::CorruptJournal(format!(
                "sequence gap: expected {expected_sequence}, found {}",
                record.record.sequence
            )));
        }
        records.push(record.record);
        offset += consumed;
    }
    Ok(Replay {
        records,
        outcome: ReplayOutcome::Complete,
    })
}

/// Why a record could not be parsed.
enum TailFault {
    /// Not enough bytes for even the fixed part / declared length.
    Truncated,
    /// Bad magic — never droppable, even at the tail: the spec allows
    /// dropping only truncation and CRC errors.
    BadMagic,
    /// Declared length out of bounds — never droppable.
    BadLength(usize),
    /// CRC mismatch; the extent is known from the header, so a *final*
    /// faulty record can still be dropped.
    CrcBad {
        record_len: usize,
        sequence: Option<u64>,
    },
}

struct ParsedRecord {
    record: WalRecord,
    consumed: usize,
}

fn parse_record(bytes: &[u8]) -> Result<ParsedRecord, TailFault> {
    if bytes.len() < RECORD_HEADER_LEN {
        return Err(TailFault::Truncated);
    }
    if bytes[0..4] != MAGIC {
        return Err(TailFault::BadMagic);
    }
    let record_len = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
    if record_len < MIN_RECORD_LEN as usize || record_len > MAX_RECORD_LEN as usize {
        return Err(TailFault::BadLength(record_len));
    }
    if bytes.len() < record_len {
        return Err(TailFault::Truncated);
    }
    let sequence = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    let kind = u16::from_le_bytes(bytes[16..18].try_into().unwrap());
    let payload_len = record_len - MIN_RECORD_LEN as usize;
    let payload = bytes[RECORD_HEADER_LEN..RECORD_HEADER_LEN + payload_len].to_vec();
    let stored_crc = u32::from_le_bytes(bytes[record_len - 4..record_len].try_into().unwrap());
    let computed = crc32c(&bytes[..record_len - 4]);
    if stored_crc != computed {
        return Err(TailFault::CrcBad {
            record_len,
            sequence: Some(sequence),
        });
    }
    Ok(ParsedRecord {
        record: WalRecord {
            sequence,
            kind,
            payload,
        },
        consumed: record_len,
    })
}

/// Encode one record. Sequences are assigned by the writer; this is the
/// pure format half used by tests and by [`WalWriter::append`].
pub fn encode_record(sequence: u64, kind: u16, payload: &[u8]) -> Vec<u8> {
    let record_len = MIN_RECORD_LEN as usize + payload.len();
    assert!(
        (record_len as u32) <= MAX_RECORD_LEN,
        "journal record exceeds 16 MiB"
    );
    let mut out = Vec::with_capacity(record_len);
    out.extend_from_slice(&MAGIC);
    out.extend_from_slice(&(record_len as u32).to_le_bytes());
    out.extend_from_slice(&sequence.to_le_bytes());
    out.extend_from_slice(&kind.to_le_bytes());
    out.extend_from_slice(&[0u8; 2]); // reserved
    out.extend_from_slice(payload);
    let crc = crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

/// An append-only journal file with strictly increasing sequences and
/// fsync-on-append durability.
pub struct WalWriter {
    file: File,
    path: PathBuf,
    next_sequence: u64,
}

impl WalWriter {
    /// Open (creating if needed) the journal at `path`, continuing after
    /// the last valid record. A damaged tail is dropped here under the same
    /// bounded rule as [`replay`]; middle damage refuses to open.
    pub fn open(path: &Path) -> Result<WalWriter, IngestError> {
        let replayed = replay(path)?;
        let next_sequence = replayed.records.last().map(|r| r.sequence).unwrap_or(0) + 1;
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)
            .map_err(|err| IngestError::Backend(format!("open journal: {err}")))?;
        if let ReplayOutcome::DroppedTail { .. } = replayed.outcome {
            // Re-write the file without the damaged tail.
            let mut kept = Vec::new();
            for record in &replayed.records {
                kept.extend_from_slice(&encode_record(
                    record.sequence,
                    record.kind,
                    &record.payload,
                ));
            }
            file.set_len(0).map_err(err("truncate journal"))?;
            file.seek(SeekFrom::Start(0)).map_err(err("seek journal"))?;
            file.write_all(&kept).map_err(err("rewrite journal"))?;
            file.sync_all().map_err(err("fsync journal"))?;
        }
        Ok(WalWriter {
            file,
            path: path.to_path_buf(),
            next_sequence,
        })
    }

    pub fn append(&mut self, kind: u16, payload: &[u8]) -> Result<u64, IngestError> {
        let sequence = self.next_sequence;
        let encoded = encode_record(sequence, kind, payload);
        self.file
            .write_all(&encoded)
            .map_err(err("append journal"))?;
        // The record is only depended on after it is durable (spec 08 §3).
        self.file.sync_all().map_err(err("fsync journal"))?;
        self.next_sequence += 1;
        Ok(sequence)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn next_sequence(&self) -> u64 {
        self.next_sequence
    }
}

fn err(context: &'static str) -> impl Fn(std::io::Error) -> IngestError {
    move |e| IngestError::Backend(format!("{context}: {e}"))
}

/// A checkpoint: `generation:u64 + state_len:u32 + state`, CRC-protected,
/// written temp → fsync → rename → fsync(parent) before the journal
/// switches generations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    pub generation: u64,
    pub state: Vec<u8>,
}

const CHECKPOINT_HEADER_LEN: usize = 12; // generation(8) + state_len(4)

impl Checkpoint {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(CHECKPOINT_HEADER_LEN + self.state.len() + 4);
        out.extend_from_slice(&self.generation.to_le_bytes());
        out.extend_from_slice(&(self.state.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.state);
        let crc = crc32c(&out);
        out.extend_from_slice(&crc.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Checkpoint, IngestError> {
        if bytes.len() < CHECKPOINT_HEADER_LEN + 4 {
            return Err(IngestError::CorruptJournal("checkpoint too short".into()));
        }
        let body = &bytes[..bytes.len() - 4];
        let stored_crc = u32::from_le_bytes(bytes[bytes.len() - 4..].try_into().unwrap());
        if crc32c(body) != stored_crc {
            return Err(IngestError::CorruptJournal(
                "checkpoint CRC mismatch".into(),
            ));
        }
        let generation = u64::from_le_bytes(body[0..8].try_into().unwrap());
        let state_len = u32::from_le_bytes(body[8..12].try_into().unwrap()) as usize;
        if body.len() != CHECKPOINT_HEADER_LEN + state_len {
            return Err(IngestError::CorruptJournal(
                "checkpoint length mismatch".into(),
            ));
        }
        Ok(Checkpoint {
            generation,
            state: body[CHECKPOINT_HEADER_LEN..].to_vec(),
        })
    }
}

/// Read the checkpoint at `path`, if one exists and parses. A damaged
/// checkpoint is an error, not a silent skip.
pub fn read_checkpoint(path: &Path) -> Result<Option<Checkpoint>, IngestError> {
    match fs::read(path) {
        Ok(bytes) => Ok(Some(Checkpoint::decode(&bytes)?)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(IngestError::Backend(format!("read checkpoint: {err}"))),
    }
}

/// Write `checkpoint` durably: temp file → fsync → rename over the target
/// → fsync the parent directory (spec 08 §9).
pub fn write_checkpoint(path: &Path, checkpoint: &Checkpoint) -> Result<(), IngestError> {
    let tmp = path.with_extension("tmp");
    {
        let mut file = File::create(&tmp).map_err(err("create checkpoint tmp"))?;
        file.write_all(&checkpoint.encode())
            .map_err(err("write checkpoint"))?;
        file.sync_all().map_err(err("fsync checkpoint"))?;
    }
    fs::rename(&tmp, path).map_err(err("rename checkpoint"))?;
    fsync_parent(path)?;
    Ok(())
}

fn fsync_parent(path: &Path) -> Result<(), IngestError> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    #[cfg(unix)]
    {
        let dir = File::open(parent).map_err(err("open parent dir"))?;
        dir.sync_all().map_err(err("fsync parent dir"))?;
    }
    #[cfg(not(unix))]
    {
        let _ = parent;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(sequence: u64, kind: u16, payload: &[u8]) -> Vec<u8> {
        encode_record(sequence, kind, payload)
    }

    #[test]
    fn record_format_bounds_and_crc() {
        let empty = record(1, kind::GENERATION_BEGIN, b"");
        assert_eq!(empty.len(), MIN_RECORD_LEN as usize);
        assert_eq!(&empty[0..4], b"BNWL");
        assert_eq!(
            u32::from_le_bytes(empty[4..8].try_into().unwrap()),
            MIN_RECORD_LEN
        );
        // CRC covers everything before the trailing 4 bytes.
        let stored = u32::from_le_bytes(empty[20..24].try_into().unwrap());
        assert_eq!(stored, crc32c(&empty[..20]));

        let big = record(1, 9, &[0xab; 1000]);
        assert_eq!(big.len(), 1000 + MIN_RECORD_LEN as usize);
        let replayed = replay_bytes(&big).unwrap();
        assert_eq!(replayed.records.len(), 1);
        assert_eq!(replayed.records[0].payload, vec![0xab; 1000]);
    }

    #[test]
    fn replay_accepts_full_journal_with_strict_sequences() {
        let mut image = Vec::new();
        image.extend_from_slice(&record(1, 0, b"gen"));
        image.extend_from_slice(&record(2, 5, b"one"));
        image.extend_from_slice(&record(3, 5, b"two"));
        let replay = replay_bytes(&image).unwrap();
        assert_eq!(replay.outcome, ReplayOutcome::Complete);
        assert_eq!(replay.records.len(), 3);
        assert_eq!(replay.records[2].sequence, 3);
    }

    #[test]
    fn truncated_final_record_is_dropped() {
        let mut image = Vec::new();
        image.extend_from_slice(&record(1, 0, b"gen"));
        image.extend_from_slice(&record(2, 5, b"one"));
        let mut truncated = image.clone();
        truncated.extend_from_slice(&record(3, 5, b"two")[..15]); // partial
        let replay = replay_bytes(&truncated).unwrap();
        assert_eq!(
            replay.outcome,
            ReplayOutcome::DroppedTail {
                dropped_sequence: None
            }
        );
        assert_eq!(replay.records.len(), 2);
    }

    #[test]
    fn crc_bad_final_record_is_dropped_but_middle_corruption_stops() {
        let mut image = Vec::new();
        image.extend_from_slice(&record(1, 0, b"gen"));
        image.extend_from_slice(&record(2, 5, b"one"));
        image.extend_from_slice(&record(3, 5, b"two"));

        // Corrupt the LAST record's payload: droppable.
        let mut bad_last = image.clone();
        let last_start = record(1, 0, b"gen").len() + record(2, 5, b"one").len();
        bad_last[last_start + 22] ^= 0xff;
        let replay = replay_bytes(&bad_last).unwrap();
        assert_eq!(
            replay.outcome,
            ReplayOutcome::DroppedTail {
                dropped_sequence: Some(3)
            }
        );
        assert_eq!(replay.records.len(), 2);

        // Corrupt the MIDDLE record: more data follows, replay must stop.
        let mut bad_middle = image.clone();
        let middle_start = record(1, 0, b"gen").len();
        bad_middle[middle_start + 22] ^= 0xff;
        let err = replay_bytes(&bad_middle).unwrap_err();
        assert!(matches!(err, IngestError::CorruptJournal(_)), "{err}");
    }

    #[test]
    fn sequence_gap_is_corrupt_journal() {
        let mut image = Vec::new();
        image.extend_from_slice(&record(1, 0, b"gen"));
        image.extend_from_slice(&record(3, 5, b"skipped-two"));
        let err = replay_bytes(&image).unwrap_err();
        assert!(matches!(err, IngestError::CorruptJournal(_)));
    }

    #[test]
    fn bad_magic_or_length_in_the_middle_is_corrupt_journal() {
        let mut image = Vec::new();
        image.extend_from_slice(&record(1, 0, b"gen"));
        image.extend_from_slice(&record(2, 5, b"one"));
        // Overwrite the second record's magic.
        image[record(1, 0, b"gen").len()] = b'X';
        let err = replay_bytes(&image).unwrap_err();
        assert!(matches!(err, IngestError::CorruptJournal(_)));
    }

    #[test]
    fn garbage_at_the_tail_is_not_droppable() {
        // Spec 08 §9 allows dropping only truncation and CRC errors; bytes
        // that are not a record at all must stop the replay.
        let mut image = Vec::new();
        image.extend_from_slice(&record(1, 0, b"gen"));
        image.extend_from_slice(&record(2, 5, b"one"));
        image.extend_from_slice(b"XXXXtrailing-garbage-not-a-record");
        let err = replay_bytes(&image).unwrap_err();
        assert!(matches!(err, IngestError::CorruptJournal(_)));
    }

    #[test]
    fn writer_assigns_sequences_and_recovers_damaged_tail() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("journal.wal");
        {
            let mut writer = WalWriter::open(&path).unwrap();
            assert_eq!(writer.append(0, b"gen").unwrap(), 1);
            assert_eq!(writer.append(5, b"one").unwrap(), 2);
            assert_eq!(writer.append(5, b"tail").unwrap(), 3);
        }
        // Truncate mid-record.
        let full = fs::read(&path).unwrap();
        let mut truncated = full.clone();
        truncated.truncate(full.len() - 10);
        fs::write(&path, &truncated).unwrap();

        let mut writer = WalWriter::open(&path).unwrap();
        assert_eq!(writer.next_sequence(), 3);
        assert_eq!(writer.append(5, b"two").unwrap(), 3);
        drop(writer);

        let replay = replay(&path).unwrap();
        assert_eq!(replay.outcome, ReplayOutcome::Complete);
        assert_eq!(replay.records.len(), 3);
        assert_eq!(replay.records[2].payload, b"two");
    }

    #[test]
    fn checkpoint_roundtrip_and_durability_dance() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("checkpoint.bin");
        assert!(read_checkpoint(&path).unwrap().is_none());

        let checkpoint = Checkpoint {
            generation: 4,
            state: b"compacted".to_vec(),
        };
        write_checkpoint(&path, &checkpoint).unwrap();
        assert_eq!(read_checkpoint(&path).unwrap().unwrap(), checkpoint);
        // The temp file is gone after the rename.
        assert!(!path.with_extension("tmp").exists());

        let mut corrupted = checkpoint.encode();
        corrupted[0] ^= 0xff;
        fs::write(&path, &corrupted).unwrap();
        assert!(matches!(
            read_checkpoint(&path).unwrap_err(),
            IngestError::CorruptJournal(_)
        ));
    }
}
