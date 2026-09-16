//! Ingest sources: the unified input contract (spec 08 §1–§2).
//!
//! A source is inventoried once into sorted [`SourceEntry`] records (raw
//! name, kind, attributes, hardlink group, sparse ranges, and the
//! [`SourceToken`] every later `read_range` goes through), and can be
//! revalidated against the captured identity before anything is published.
//!
//! P1 sources are local directories and uncompressed seekable tars
//! (spec 08 §1). Compressed tar and ZIP are refused as
//! [`IngestError::UnsupportedSource`]. Neither source follows symlinks;
//! both reject detected source changes; neither may use a stable pathname
//! as proof that inode content is unchanged — identity comes from
//! `(device, inode, size, mtime)` for local files and from the member
//! index plus the archive digest for tars.

use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::native_base::wire::error::{WireError, WireResult};
use crate::native_base::wire::uvarint::{Reader, Writer};

use super::error::IngestError;

/// The source consistency policy (spec 08 §1). `BestEffortDetected` must
/// be explicitly enabled and is disclosed in the session provenance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsistencyPolicy {
    /// The source is an immutable snapshot or an application-frozen
    /// directory.
    SnapshotBacked,
    /// Size/mtime/ctime-style detection only: catches some changes, is
    /// not a proof under arbitrary concurrent modification.
    BestEffortDetected,
}

impl ConsistencyPolicy {
    pub fn as_u8(self) -> u8 {
        match self {
            ConsistencyPolicy::SnapshotBacked => 0,
            ConsistencyPolicy::BestEffortDetected => 1,
        }
    }

    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(ConsistencyPolicy::SnapshotBacked),
            1 => Some(ConsistencyPolicy::BestEffortDetected),
            _ => None,
        }
    }
}

/// The kind of one inventory entry. Symlinks are recorded, never followed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryKind {
    File { size: u64 },
    Directory,
    Symlink { target: Vec<u8> },
}

/// Fixed attributes captured at inventory time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceAttributes {
    pub mode: u32,
    pub mtime_ns: i64,
}

/// One inventoried source entry, in sorted path order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceEntry {
    /// The name exactly as the source presented it.
    pub raw_name: Vec<u8>,
    /// The validated, normalized relative path (no leading `/`, no `..`).
    pub path: Vec<u8>,
    pub kind: EntryKind,
    pub attributes: SourceAttributes,
    /// Stable source-inode identity shared by hardlink group members.
    pub hardlink_group: Option<u64>,
    /// Byte ranges inside the file extent that are stored as holes. Hole
    /// bytes still read as zeros through [`IngestSource::read_range`];
    /// the ranges are a planning hint, not a correctness exemption.
    pub sparse_ranges: Vec<(u64, u64)>,
    pub token: SourceToken,
}

/// Where a token's bytes come from, plus the identity digest captured at
/// inventory time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceToken {
    /// Local filesystem path (relative to the source root or the archive
    /// member name) — never used as an S3 key or host absolute path.
    pub location: Vec<u8>,
    /// SHA-256 over the identity facts captured at inventory.
    pub identity: [u8; 32],
}

impl SourceToken {
    pub fn encode(&self, w: &mut Writer) {
        w.bytes(&self.location);
        w.put(&self.identity);
    }

    pub fn decode(r: &mut Reader<'_>) -> WireResult<SourceToken> {
        let location = r.bytes("source token")?.to_vec();
        let identity: [u8; 32] = r.take(32, "source token")?.try_into().unwrap();
        Ok(SourceToken { location, identity })
    }
}

impl SourceEntry {
    pub fn encode(&self, w: &mut Writer) {
        w.bytes(&self.raw_name);
        w.bytes(&self.path);
        match &self.kind {
            EntryKind::File { size } => {
                w.u8(0);
                w.u64(*size);
            }
            EntryKind::Directory => w.u8(1),
            EntryKind::Symlink { target } => {
                w.u8(2);
                w.bytes(target);
            }
        }
        w.u32(self.attributes.mode);
        // mtime_ns as i64 reinterpret: encode the two's complement bits.
        w.u64(self.attributes.mtime_ns as u64);
        match self.hardlink_group {
            Some(group) => {
                w.u8(1);
                w.u64(group);
            }
            None => w.u8(0),
        }
        w.uvarint(self.sparse_ranges.len() as u64);
        for (offset, len) in &self.sparse_ranges {
            w.u64(*offset);
            w.u64(*len);
        }
        self.token.encode(w);
    }

    pub fn decode(r: &mut Reader<'_>) -> WireResult<SourceEntry> {
        let raw_name = r.bytes("entry raw name")?.to_vec();
        let path = r.bytes("entry path")?.to_vec();
        let kind = match r.u8("entry kind")? {
            0 => EntryKind::File {
                size: r.u64("file size")?,
            },
            1 => EntryKind::Directory,
            2 => EntryKind::Symlink {
                target: r.bytes("symlink target")?.to_vec(),
            },
            other => return Err(WireError::invalid("entry kind", format!("unknown {other}"))),
        };
        let mode = r.u32("entry mode")?;
        let mtime_ns = r.u64("entry mtime")? as i64;
        let hardlink_group = if r.u8("hardlink flag")? == 1 {
            Some(r.u64("hardlink group")?)
        } else {
            None
        };
        let sparse_count = r.uvarint("sparse count")? as usize;
        let mut sparse_ranges = Vec::with_capacity(sparse_count.min(4096));
        for _ in 0..sparse_count {
            let offset = r.u64("sparse offset")?;
            let len = r.u64("sparse len")?;
            sparse_ranges.push((offset, len));
        }
        let token = SourceToken::decode(r)?;
        Ok(SourceEntry {
            raw_name,
            path,
            kind,
            attributes: SourceAttributes { mode, mtime_ns },
            hardlink_group,
            sparse_ranges,
            token,
        })
    }
}

/// Encode a sorted inventory for `inventory.bin`.
pub fn encode_inventory(policy: ConsistencyPolicy, entries: &[SourceEntry]) -> Vec<u8> {
    let mut w = Writer::new();
    w.u8(policy.as_u8());
    w.uvarint(entries.len() as u64);
    for entry in entries {
        entry.encode(&mut w);
    }
    w.into_bytes()
}

/// Decode an inventory image; trailing bytes are refused.
pub fn decode_inventory(bytes: &[u8]) -> WireResult<(ConsistencyPolicy, Vec<SourceEntry>)> {
    let mut r = Reader::new(bytes);
    let policy = ConsistencyPolicy::from_u8(r.u8("policy")?)
        .ok_or_else(|| WireError::invalid("consistency policy", "unknown discriminator"))?;
    let count = r.uvarint("entry count")? as usize;
    let mut entries = Vec::with_capacity(count.min(1 << 20));
    for _ in 0..count {
        entries.push(SourceEntry::decode(&mut r)?);
    }
    if !r.is_empty() {
        return Err(WireError::invalid("inventory", "trailing bytes"));
    }
    Ok((policy, entries))
}

/// Validate one member path (spec 08 §2): relative, no `..`, no empty
/// component, no NUL, not empty.
pub fn validate_member_path(path: &[u8]) -> Result<(), IngestError> {
    if path.is_empty() {
        return Err(IngestError::PathEscape("empty path".into()));
    }
    if path.contains(&0) {
        return Err(IngestError::PathEscape("path contains NUL".into()));
    }
    if path[0] == b'/' {
        return Err(IngestError::PathEscape(format!(
            "absolute path: {}",
            String::from_utf8_lossy(path)
        )));
    }
    for component in path.split(|b| *b == b'/') {
        if component.is_empty() {
            return Err(IngestError::PathEscape(format!(
                "empty path component: {}",
                String::from_utf8_lossy(path)
            )));
        }
        if component == b".." {
            return Err(IngestError::PathEscape(format!(
                "`..` component: {}",
                String::from_utf8_lossy(path)
            )));
        }
    }
    Ok(())
}

/// The unified source contract (spec 08 §2).
pub trait IngestSource {
    fn policy(&self) -> ConsistencyPolicy;

    /// Produce the sorted, bounded inventory. Every entry's path is
    /// validated; duplicates are refused.
    fn inventory(&mut self) -> Result<Vec<SourceEntry>, IngestError>;

    /// Read exactly `buf.len()` bytes at `offset` for `token`, or fail.
    /// Holes read as zeros; short reads are errors, never padded.
    fn read_range(
        &self,
        token: &SourceToken,
        offset: u64,
        buf: &mut [u8],
    ) -> Result<(), IngestError>;

    /// Re-check the captured identity of every entry. Any change refuses
    /// publication (spec 08 §1).
    fn revalidate(&self, entries: &[SourceEntry]) -> Result<(), IngestError>;
}

// ---------------------------------------------------------------------------
// Local directory source
// ---------------------------------------------------------------------------

/// A local directory walked without following symlinks. Hardlinks are
/// grouped by `(device, inode)` — the stable source inode identity
/// (spec 08 §2).
pub struct LocalDirSource {
    root: PathBuf,
    policy: ConsistencyPolicy,
}

impl LocalDirSource {
    pub fn new(root: &Path, policy: ConsistencyPolicy) -> LocalDirSource {
        LocalDirSource {
            root: root.to_path_buf(),
            policy,
        }
    }

    fn walk(
        &self,
        dir: &Path,
        prefix: &[u8],
        out: &mut Vec<SourceEntry>,
        inode_groups: &mut BTreeMap<(u64, u64), u64>,
    ) -> Result<(), IngestError> {
        let mut children: Vec<(Vec<u8>, PathBuf)> = fs::read_dir(dir)
            .map_err(|err| IngestError::Backend(format!("read_dir: {err}")))?
            .map(|entry| {
                let entry =
                    entry.map_err(|err| IngestError::Backend(format!("readdir entry: {err}")))?;
                let name = entry
                    .file_name()
                    .into_string()
                    .map_err(|_| IngestError::PathEscape("file name is not valid UTF-8".into()))?;
                Ok((name.into_bytes(), entry.path()))
            })
            .collect::<Result<_, IngestError>>()?;
        children.sort();

        for (name, path) in children {
            let mut raw_name = prefix.to_vec();
            if !raw_name.is_empty() {
                raw_name.push(b'/');
            }
            raw_name.extend_from_slice(&name);
            let metadata = fs::symlink_metadata(&path)
                .map_err(|err| IngestError::Backend(format!("stat {}: {err}", path.display())))?;
            use std::os::unix::fs::MetadataExt;
            let file_type = metadata.file_type();
            let attributes = SourceAttributes {
                mode: metadata.mode(),
                mtime_ns: metadata.mtime(),
            };
            let (kind, hardlink_group) = if file_type.is_dir() {
                (EntryKind::Directory, None)
            } else if file_type.is_symlink() {
                let target = fs::read_link(&path)
                    .map_err(|err| IngestError::Backend(format!("readlink: {err}")))?;
                (
                    EntryKind::Symlink {
                        target: target.to_string_lossy().into_owned().into_bytes(),
                    },
                    None,
                )
            } else if file_type.is_file() {
                let key = (metadata.dev(), metadata.ino());
                let group = match inode_groups.get(&key) {
                    Some(group) => *group,
                    None => {
                        let group = inode_groups.len() as u64 + 1;
                        inode_groups.insert(key, group);
                        group
                    }
                };
                (
                    EntryKind::File {
                        size: metadata.size(),
                    },
                    Some(group),
                )
            } else {
                return Err(IngestError::DevicePayload(format!(
                    "{} is a device/socket/fifo",
                    String::from_utf8_lossy(&raw_name)
                )));
            };
            let token = SourceToken {
                location: raw_name.clone(),
                identity: local_identity(&metadata),
            };
            let entry = SourceEntry {
                raw_name,
                path: Vec::new(), // filled by the caller after validation
                kind,
                attributes,
                hardlink_group,
                // Local holes read as zeros through read_range; no sparse
                // claim is made for local files.
                sparse_ranges: Vec::new(),
                token,
            };
            out.push(entry.clone());
            if let EntryKind::Directory = entry.kind {
                let mut child_prefix = prefix.to_vec();
                if !child_prefix.is_empty() {
                    child_prefix.push(b'/');
                }
                child_prefix.extend_from_slice(&name);
                self.walk(&path, &child_prefix, out, inode_groups)?;
            }
        }
        Ok(())
    }

    fn resolve(&self, location: &[u8]) -> PathBuf {
        let mut path = self.root.clone();
        for component in location.split(|b| *b == b'/') {
            path.push(String::from_utf8_lossy(component).as_ref());
        }
        path
    }
}

fn local_identity(metadata: &std::fs::Metadata) -> [u8; 32] {
    use std::os::unix::fs::MetadataExt;
    let mut hasher = Sha256::new();
    hasher.update(metadata.dev().to_be_bytes());
    hasher.update(metadata.ino().to_be_bytes());
    hasher.update(metadata.size().to_be_bytes());
    hasher.update(metadata.mtime().to_be_bytes());
    hasher.finalize().into()
}

impl IngestSource for LocalDirSource {
    fn policy(&self) -> ConsistencyPolicy {
        self.policy
    }

    fn inventory(&mut self) -> Result<Vec<SourceEntry>, IngestError> {
        if !self.root.is_dir() {
            return Err(IngestError::UnsupportedSource(format!(
                "local source {} is not a directory",
                self.root.display()
            )));
        }
        let mut entries = Vec::new();
        let mut inode_groups = BTreeMap::new();
        self.walk(&self.root.clone(), b"", &mut entries, &mut inode_groups)?;
        // Validate, normalize (raw name *is* the relative path for a local
        // walk), sort, and refuse duplicates.
        let mut seen = std::collections::BTreeSet::new();
        for entry in &mut entries {
            validate_member_path(&entry.raw_name)?;
            if !seen.insert(entry.raw_name.clone()) {
                return Err(IngestError::DuplicatePath(
                    String::from_utf8_lossy(&entry.raw_name).into_owned(),
                ));
            }
            entry.path = entry.raw_name.clone();
        }
        entries.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(entries)
    }

    fn read_range(
        &self,
        token: &SourceToken,
        offset: u64,
        buf: &mut [u8],
    ) -> Result<(), IngestError> {
        let path = self.resolve(&token.location);
        let mut file = fs::File::open(&path)
            .map_err(|err| IngestError::Backend(format!("open {}: {err}", path.display())))?;
        file.seek(SeekFrom::Start(offset))
            .map_err(|err| IngestError::Backend(format!("seek: {err}")))?;
        file.read_exact(buf)
            .map_err(|err| IngestError::Backend(format!("read_exact: {err}")))?;
        Ok(())
    }

    fn revalidate(&self, entries: &[SourceEntry]) -> Result<(), IngestError> {
        for entry in entries {
            let path = self.resolve(&entry.token.location);
            let metadata = match fs::symlink_metadata(&path) {
                Ok(metadata) => metadata,
                Err(err) => {
                    return Err(IngestError::SourceChanged(format!(
                        "{}: stat failed: {err}",
                        String::from_utf8_lossy(&entry.path)
                    )));
                }
            };
            if local_identity(&metadata) != entry.token.identity {
                return Err(IngestError::SourceChanged(
                    String::from_utf8_lossy(&entry.path).into_owned(),
                ));
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Seekable (uncompressed) tar source
// ---------------------------------------------------------------------------

/// One member of the archive index.
#[derive(Debug, Clone)]
struct TarMember {
    /// Data offset (after the 512-byte header) and stored size.
    data_offset: u64,
    stored_size: u64,
    /// Data extents `(logical offset, len)` in stored order. Regular
    /// members have exactly one extent covering the whole logical size;
    /// GNU sparse members have the archive's extent list and read the
    /// gaps as zeros.
    sparse: Vec<(u64, u64)>,
    logical_size: u64,
}

/// An uncompressed, seekable tar. The archive file identity (size, mtime,
/// full digest) is captured at inventory; revalidation recomputes it, so
/// any in-place change of the archive is detected.
pub struct SeekableTarSource {
    path: PathBuf,
    policy: ConsistencyPolicy,
    /// Member name -> index record, built at inventory.
    members: BTreeMap<Vec<u8>, TarMember>,
    /// The captured archive identity.
    archive_identity: [u8; 32],
}

fn tar_identity(path: &Path) -> Result<([u8; 32], u64), IngestError> {
    let metadata =
        fs::metadata(path).map_err(|err| IngestError::Backend(format!("stat tar: {err}")))?;
    let mut file =
        fs::File::open(path).map_err(|err| IngestError::Backend(format!("open tar: {err}")))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 << 10];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|err| IngestError::Backend(format!("read tar: {err}")))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    use std::os::unix::fs::MetadataExt;
    let mut identity = Sha256::new();
    identity.update(hasher.finalize());
    identity.update(metadata.len().to_be_bytes());
    identity.update(metadata.mtime().to_be_bytes());
    Ok((identity.finalize().into(), metadata.len()))
}

fn parse_octal(field: &[u8]) -> Option<u64> {
    let text = field.split(|b| *b == 0 || *b == b' ').next()?;
    if text.is_empty() {
        return Some(0);
    }
    let mut value = 0u64;
    for byte in text {
        if !b"01234567".contains(byte) {
            return None;
        }
        value = value.checked_mul(8)?.checked_add((byte - b'0') as u64)?;
    }
    Some(value)
}

fn tar_string(field: &[u8]) -> Vec<u8> {
    let end = field.iter().position(|b| *b == 0).unwrap_or(field.len());
    field[..end].to_vec()
}

impl SeekableTarSource {
    /// Open an uncompressed tar. Compressed inputs are recognized by
    /// magic and refused (spec 08 §1).
    pub fn new(path: &Path, policy: ConsistencyPolicy) -> Result<SeekableTarSource, IngestError> {
        let mut magic = [0u8; 4];
        {
            let mut file = fs::File::open(path)
                .map_err(|err| IngestError::Backend(format!("open tar: {err}")))?;
            file.read_exact(&mut magic)
                .map_err(|err| IngestError::Backend(format!("read tar magic: {err}")))?;
        }
        // gzip / bzip2 / xz / zstd magics.
        let compressed = matches!(&magic, [0x1f, 0x8b, ..])
            || matches!(&magic, [b'B', b'Z', b'h', ..])
            || matches!(&magic, [0xfd, b'7', b'z', b'X', ..])
            || matches!(&magic, [0x28, 0xb5, 0x2f, 0xfd, ..]);
        if compressed {
            return Err(IngestError::UnsupportedSource(
                "compressed tar requires a spool; not supported in P1".into(),
            ));
        }
        let (archive_identity, _) = tar_identity(path)?;
        Ok(SeekableTarSource {
            path: path.to_path_buf(),
            policy,
            members: BTreeMap::new(),
            archive_identity,
        })
    }

    fn member_token(&self, name: &[u8], member: &TarMember) -> SourceToken {
        let mut hasher = Sha256::new();
        hasher.update(self.archive_identity);
        hasher.update(name);
        hasher.update(member.data_offset.to_be_bytes());
        hasher.update(member.logical_size.to_be_bytes());
        for (offset, len) in &member.sparse {
            hasher.update(offset.to_be_bytes());
            hasher.update(len.to_be_bytes());
        }
        SourceToken {
            location: name.to_vec(),
            identity: hasher.finalize().into(),
        }
    }

    fn read_member_range(
        &self,
        member: &TarMember,
        offset: u64,
        buf: &mut [u8],
    ) -> Result<(), IngestError> {
        if buf.is_empty() {
            return Ok(());
        }
        if offset >= member.logical_size || offset + buf.len() as u64 > member.logical_size {
            return Err(IngestError::Backend(format!(
                "tar member range [{offset}, {}) beyond logical size {}",
                offset + buf.len() as u64,
                member.logical_size
            )));
        }
        // Map the logical range onto stored extents: bytes inside sparse
        // holes read as zeros, bytes inside extents come from the archive.
        // Extents are packed in stored order — the stored bytes of each
        // extent directly follow the previous extent's, the logical hole
        // between them is not stored.
        let mut done = 0usize;
        while done < buf.len() {
            let logical = offset + done as u64;
            let remaining = buf.len() - done;
            // Find the extent covering `logical`, along with the stored
            // offset of that extent's first byte.
            let mut in_extent = None;
            let mut stored_start = 0u64;
            for (start, len) in &member.sparse {
                if logical >= *start && logical < *start + *len {
                    in_extent = Some((*start, *len));
                    break;
                }
                stored_start += len;
            }
            match in_extent {
                Some((start, len)) => {
                    // Extent bytes from the archive, in stored order.
                    let extent_pos = logical - start;
                    let take = (remaining as u64).min(len - extent_pos) as usize;
                    let stored_offset = member.data_offset + stored_start + extent_pos;
                    let mut file = fs::File::open(&self.path)
                        .map_err(|err| IngestError::Backend(format!("open tar: {err}")))?;
                    file.seek(SeekFrom::Start(stored_offset))
                        .map_err(|err| IngestError::Backend(format!("seek tar: {err}")))?;
                    file.read_exact(&mut buf[done..done + take])
                        .map_err(|err| IngestError::Backend(format!("read tar member: {err}")))?;
                    done += take;
                }
                None => {
                    // Inside a hole: zeros until the next extent or EOF.
                    let next_extent = member
                        .sparse
                        .iter()
                        .map(|(start, _)| start)
                        .filter(|start| **start > logical)
                        .min()
                        .copied()
                        .unwrap_or(member.logical_size);
                    let take = (remaining as u64).min(next_extent - logical) as usize;
                    buf[done..done + take].fill(0);
                    done += take;
                }
            }
        }
        Ok(())
    }
}

impl IngestSource for SeekableTarSource {
    fn policy(&self) -> ConsistencyPolicy {
        self.policy
    }

    fn inventory(&mut self) -> Result<Vec<SourceEntry>, IngestError> {
        let mut file = fs::File::open(&self.path)
            .map_err(|err| IngestError::Backend(format!("open tar: {err}")))?;
        let archive_len = file
            .metadata()
            .map_err(|err| IngestError::Backend(format!("stat tar: {err}")))?
            .len();
        let mut members: BTreeMap<Vec<u8>, TarMember> = BTreeMap::new();
        let mut entries: Vec<SourceEntry> = Vec::new();
        // Hardlink members reference a data-carrying member seen earlier;
        // map member name -> hardlink group.
        let mut hardlink_groups: BTreeMap<Vec<u8>, u64> = BTreeMap::new();
        let mut next_group = 0u64;

        let mut offset = 0u64;
        let mut header = [0u8; 512];
        loop {
            if offset + 512 > archive_len {
                break;
            }
            file.seek(SeekFrom::Start(offset))
                .map_err(|err| IngestError::Backend(format!("seek tar: {err}")))?;
            file.read_exact(&mut header)
                .map_err(|err| IngestError::Backend(format!("read tar header: {err}")))?;
            if header.iter().all(|b| *b == 0) {
                // End-of-archive marker; two zero blocks by convention, one
                // is enough for us to stop.
                break;
            }
            if &header[257..262] != b"ustar" {
                return Err(IngestError::UnsupportedSource(format!(
                    "not a ustar archive at offset {offset}"
                )));
            }
            let name = tar_string(&header[0..100]);
            let size = parse_octal(&header[124..136])
                .ok_or_else(|| IngestError::UnsupportedSource("bad tar size field".into()))?;
            let mtime = parse_octal(&header[136..148])
                .ok_or_else(|| IngestError::UnsupportedSource("bad tar mtime field".into()))?;
            let mode = parse_octal(&header[100..108]).unwrap_or(0o644) as u32;
            let typeflag = header[156];
            let linkname = tar_string(&header[157..257]);
            let prefix = tar_string(&header[345..500]);
            let mut full_name = prefix;
            if !full_name.is_empty() && !name.is_empty() {
                full_name.push(b'/');
            }
            full_name.extend_from_slice(&name);

            let data_offset = offset + 512;
            // Stored bytes are padded to 512.
            let stored_blocks = size.div_ceil(512);
            let next_offset = data_offset + stored_blocks * 512;

            match typeflag {
                b'0' | 0 => {
                    // Regular member: one extent covering the whole
                    // logical size, so reads take the extent path.
                    let member = TarMember {
                        data_offset,
                        stored_size: size,
                        sparse: vec![(0, size)],
                        logical_size: size,
                    };
                    validate_member_path(&full_name)?;
                    let token = self.member_token(&full_name, &member);
                    let group = {
                        next_group += 1;
                        next_group
                    };
                    hardlink_groups.insert(full_name.clone(), group);
                    members.insert(full_name.clone(), member.clone());
                    entries.push(SourceEntry {
                        raw_name: full_name.clone(),
                        path: full_name.clone(),
                        kind: EntryKind::File { size },
                        attributes: SourceAttributes {
                            mode,
                            mtime_ns: mtime as i64,
                        },
                        hardlink_group: Some(group),
                        sparse_ranges: Vec::new(),
                        token,
                    });
                }
                b'5' => {
                    validate_member_path(&full_name)?;
                    members.insert(
                        full_name.clone(),
                        TarMember {
                            data_offset,
                            stored_size: 0,
                            sparse: Vec::new(),
                            logical_size: 0,
                        },
                    );
                    entries.push(SourceEntry {
                        raw_name: full_name.clone(),
                        path: full_name.clone(),
                        kind: EntryKind::Directory,
                        attributes: SourceAttributes {
                            mode,
                            mtime_ns: mtime as i64,
                        },
                        hardlink_group: None,
                        sparse_ranges: Vec::new(),
                        token: SourceToken {
                            location: full_name.clone(),
                            identity: [0u8; 32],
                        },
                    });
                }
                b'2' => {
                    validate_member_path(&full_name)?;
                    entries.push(SourceEntry {
                        raw_name: full_name.clone(),
                        path: full_name.clone(),
                        kind: EntryKind::Symlink { target: linkname },
                        attributes: SourceAttributes {
                            mode,
                            mtime_ns: mtime as i64,
                        },
                        hardlink_group: None,
                        sparse_ranges: Vec::new(),
                        token: SourceToken {
                            location: full_name.clone(),
                            identity: [0u8; 32],
                        },
                    });
                }
                b'1' => {
                    // Hardlink to a previously seen member.
                    validate_member_path(&full_name)?;
                    let group = *hardlink_groups.get(&linkname).ok_or_else(|| {
                        IngestError::UnresolvedHardlink(format!(
                            "{} -> {} (target not seen before the link)",
                            String::from_utf8_lossy(&full_name),
                            String::from_utf8_lossy(&linkname)
                        ))
                    })?;
                    entries.push(SourceEntry {
                        raw_name: full_name.clone(),
                        path: full_name.clone(),
                        kind: EntryKind::File {
                            size: members.get(&linkname).map(|m| m.logical_size).unwrap_or(0),
                        },
                        attributes: SourceAttributes {
                            mode,
                            mtime_ns: mtime as i64,
                        },
                        hardlink_group: Some(group),
                        sparse_ranges: Vec::new(),
                        token: SourceToken {
                            // Reads go through the *target* member.
                            location: linkname.clone(),
                            identity: members
                                .get(&linkname)
                                .map(|m| self.member_token(&linkname, m).identity)
                                .unwrap_or([0u8; 32]),
                        },
                    });
                }
                b'S' => {
                    // Old-GNU sparse member: up to four extents in the
                    // fixed header, `isextended` must be clear (P1 bound).
                    if header[482] != 0 {
                        return Err(IngestError::UnsupportedSource(
                            "GNU sparse tar with extension headers is not supported in P1".into(),
                        ));
                    }
                    let real_size = parse_octal(&header[483..495]).ok_or_else(|| {
                        IngestError::UnsupportedSource("bad sparse real size".into())
                    })?;
                    let mut sparse = Vec::new();
                    let mut stored_cursor = 0u64;
                    for i in 0..4 {
                        let offset_field = &header[386 + i * 24..386 + i * 24 + 12];
                        let num_field = &header[386 + i * 24 + 12..386 + i * 24 + 24];
                        let extent_offset = parse_octal(offset_field).ok_or_else(|| {
                            IngestError::UnsupportedSource("bad sparse offset".into())
                        })?;
                        let extent_num = parse_octal(num_field).ok_or_else(|| {
                            IngestError::UnsupportedSource("bad sparse num".into())
                        })?;
                        if extent_num == 0 {
                            continue;
                        }
                        sparse.push((extent_offset, extent_num));
                        stored_cursor += extent_num;
                    }
                    if stored_cursor != size {
                        return Err(IngestError::UnsupportedSource(
                            "sparse extents do not cover the stored size".into(),
                        ));
                    }
                    validate_member_path(&full_name)?;
                    let member = TarMember {
                        data_offset,
                        stored_size: size,
                        sparse,
                        logical_size: real_size,
                    };
                    let token = self.member_token(&full_name, &member);
                    next_group += 1;
                    hardlink_groups.insert(full_name.clone(), next_group);
                    members.insert(full_name.clone(), member.clone());
                    entries.push(SourceEntry {
                        raw_name: full_name.clone(),
                        path: full_name.clone(),
                        kind: EntryKind::File { size: real_size },
                        attributes: SourceAttributes {
                            mode,
                            mtime_ns: mtime as i64,
                        },
                        hardlink_group: Some(next_group),
                        sparse_ranges: member.sparse.clone(),
                        token,
                    });
                }
                b'3' | b'4' | b'6' => {
                    return Err(IngestError::DevicePayload(format!(
                        "tar member {} has device/fifo type flag",
                        String::from_utf8_lossy(&full_name)
                    )));
                }
                other => {
                    return Err(IngestError::UnsupportedSource(format!(
                        "tar type flag {other:#x} is not supported in P1"
                    )));
                }
            }
            offset = next_offset;
        }

        // Normalize: entries sorted, duplicates refused.
        let mut seen = std::collections::BTreeSet::new();
        for entry in &entries {
            if !seen.insert(entry.path.clone()) {
                return Err(IngestError::DuplicatePath(
                    String::from_utf8_lossy(&entry.path).into_owned(),
                ));
            }
        }
        entries.sort_by(|a, b| a.path.cmp(&b.path));
        self.members = members;
        Ok(entries)
    }

    fn read_range(
        &self,
        token: &SourceToken,
        offset: u64,
        buf: &mut [u8],
    ) -> Result<(), IngestError> {
        let member = self.members.get(&token.location).ok_or_else(|| {
            IngestError::Backend(format!(
                "unknown tar member {}",
                String::from_utf8_lossy(&token.location)
            ))
        })?;
        self.read_member_range(member, offset, buf)
    }

    fn revalidate(&self, entries: &[SourceEntry]) -> Result<(), IngestError> {
        let (identity, _) = tar_identity(&self.path)?;
        if identity != self.archive_identity {
            return Err(IngestError::SourceChanged(
                "archive identity changed after inventory".into(),
            ));
        }
        for entry in entries {
            let member = self.members.get(&entry.token.location).ok_or_else(|| {
                IngestError::SourceChanged(format!(
                    "member {} missing from the archive index",
                    String::from_utf8_lossy(&entry.token.location)
                ))
            })?;
            if self.member_token(&entry.token.location, member).identity != entry.token.identity {
                return Err(IngestError::SourceChanged(
                    String::from_utf8_lossy(&entry.path).into_owned(),
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_tar(dir: &Path, name: &str, members: &[(&[u8], &[u8])]) -> PathBuf {
        let path = dir.join(name);
        let mut image = Vec::new();
        for (member_name, data) in members {
            let mut header = [0u8; 512];
            header[..member_name.len()].copy_from_slice(member_name);
            header[100..108].copy_from_slice(b"0000644\x00");
            let size_field = format!("{:011o}\x00", data.len());
            header[124..136].copy_from_slice(size_field.as_bytes());
            let mtime_field = format!("{:011o}\x00", 0u64);
            header[136..148].copy_from_slice(mtime_field.as_bytes());
            header[156] = b'0';
            header[257..262].copy_from_slice(b"ustar");
            header[263..265].copy_from_slice(b"00");
            image.extend_from_slice(&header);
            image.extend_from_slice(data);
            let pad = (512 - data.len() % 512) % 512;
            image.extend(std::iter::repeat_n(0u8, pad));
        }
        image.extend(std::iter::repeat_n(0u8, 1024));
        fs::write(&path, &image).unwrap();
        path
    }

    #[test]
    fn member_path_validation_rejects_escapes() {
        assert!(validate_member_path(b"a/b.txt").is_ok());
        assert!(validate_member_path(b"a").is_ok());
        assert!(matches!(
            validate_member_path(b"/etc/passwd"),
            Err(IngestError::PathEscape(_))
        ));
        assert!(matches!(
            validate_member_path(b"a/../../etc"),
            Err(IngestError::PathEscape(_))
        ));
        assert!(matches!(
            validate_member_path(b"a//b"),
            Err(IngestError::PathEscape(_))
        ));
        assert!(matches!(
            validate_member_path(b""),
            Err(IngestError::PathEscape(_))
        ));
    }

    #[test]
    fn local_source_inventory_hardlinks_and_no_symlink_follow() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("a.txt"), b"alpha").unwrap();
        fs::write(dir.path().join("b.txt"), b"beta").unwrap();
        fs::hard_link(dir.path().join("a.txt"), dir.path().join("hard.txt")).unwrap();
        std::os::unix::fs::symlink("a.txt", dir.path().join("link")).unwrap();
        fs::create_dir(dir.path().join("sub")).unwrap();
        fs::write(dir.path().join("sub/c.txt"), b"gamma").unwrap();

        let mut source = LocalDirSource::new(dir.path(), ConsistencyPolicy::SnapshotBacked);
        let entries = source.inventory().unwrap();
        let paths: Vec<&[u8]> = entries.iter().map(|e| e.path.as_slice()).collect();
        assert_eq!(
            paths,
            vec![
                b"a.txt" as &[u8],
                b"b.txt",
                b"hard.txt",
                b"link",
                b"sub",
                b"sub/c.txt"
            ]
        );

        let a = entries.iter().find(|e| e.path == b"a.txt").unwrap();
        let hard = entries.iter().find(|e| e.path == b"hard.txt").unwrap();
        assert_eq!(a.hardlink_group, hard.hardlink_group);
        assert!(a.hardlink_group.is_some());
        let b = entries.iter().find(|e| e.path == b"b.txt").unwrap();
        assert_ne!(a.hardlink_group, b.hardlink_group);

        let link = entries.iter().find(|e| e.path == b"link").unwrap();
        match &link.kind {
            EntryKind::Symlink { target } => assert_eq!(target, b"a.txt"),
            other => panic!("expected symlink, got {other:?}"),
        }

        // Exact range reads.
        let mut buf = [0u8; 5];
        source.read_range(&a.token, 0, &mut buf).unwrap();
        assert_eq!(&buf, b"alpha");
        let mut tail = [0u8; 2];
        source.read_range(&a.token, 3, &mut tail).unwrap();
        assert_eq!(&tail, b"ha");
        // Beyond EOF is an error, not a short/padded read.
        let mut beyond = [0u8; 3];
        assert!(source.read_range(&a.token, 4, &mut beyond).is_err());

        // Unchanged source revalidates.
        source.revalidate(&entries).unwrap();
        // Change one file: refused.
        fs::write(dir.path().join("b.txt"), b"BETA!").unwrap();
        assert!(matches!(
            source.revalidate(&entries),
            Err(IngestError::SourceChanged(_))
        ));
    }

    #[test]
    fn local_source_rejects_devices() {
        let dir = tempfile::tempdir().unwrap();
        let fifo = dir.path().join("pipe");
        let _ = std::process::Command::new("mkfifo").arg(&fifo).status();
        if !fifo.exists() {
            return; // no mkfifo on this platform
        }
        let mut source = LocalDirSource::new(dir.path(), ConsistencyPolicy::SnapshotBacked);
        assert!(matches!(
            source.inventory(),
            Err(IngestError::DevicePayload(_))
        ));
    }

    #[test]
    fn tar_source_reads_members_and_detects_change() {
        let dir = tempfile::tempdir().unwrap();
        let tar = write_tar(
            dir.path(),
            "plain.tar",
            &[
                (b"one.txt", b"first" as &[u8]),
                (b"d/two.txt", b"second-member"),
            ],
        );
        let mut source = SeekableTarSource::new(&tar, ConsistencyPolicy::SnapshotBacked).unwrap();
        let entries = source.inventory().unwrap();
        assert_eq!(entries.len(), 2);
        let one = entries.iter().find(|e| e.path == b"one.txt").unwrap();
        let two = entries.iter().find(|e| e.path == b"d/two.txt").unwrap();
        let mut buf = [0u8; 5];
        source.read_range(&one.token, 0, &mut buf).unwrap();
        assert_eq!(&buf, b"first");
        let mut buf2 = [0u8; 13];
        source.read_range(&two.token, 0, &mut buf2).unwrap();
        assert_eq!(&buf2, b"second-member");

        source.revalidate(&entries).unwrap();
        // Rewrite the archive in place: identity changes.
        let mut image = fs::read(&tar).unwrap();
        let marker = image.windows(5).position(|w| w == b"first").unwrap();
        image[marker..marker + 5].copy_from_slice(b"FIRSX");
        fs::write(&tar, &image).unwrap();
        assert!(matches!(
            source.revalidate(&entries),
            Err(IngestError::SourceChanged(_))
        ));
    }

    #[test]
    fn tar_source_rejects_escapes_and_duplicates() {
        let dir = tempfile::tempdir().unwrap();
        // Absolute path member.
        let abs = write_tar(dir.path(), "abs.tar", &[(b"/etc/passwd", b"x")]);
        let mut source = SeekableTarSource::new(&abs, ConsistencyPolicy::SnapshotBacked).unwrap();
        assert!(matches!(
            source.inventory(),
            Err(IngestError::PathEscape(_))
        ));

        // `..` member.
        let dotdot = write_tar(dir.path(), "dot.tar", &[(b"../escape", b"x")]);
        let mut source =
            SeekableTarSource::new(&dotdot, ConsistencyPolicy::SnapshotBacked).unwrap();
        assert!(matches!(
            source.inventory(),
            Err(IngestError::PathEscape(_))
        ));

        // Two members normalizing to the same path (prefix/suffix split).
        let mut image = Vec::new();
        for (prefix, name) in [
            (b"dir" as &[u8], b"same" as &[u8]),
            (b"dir/same\x00" as &[u8], b"" as &[u8]),
        ] {
            let mut header = [0u8; 512];
            header[..name.len()].copy_from_slice(name);
            header[100..108].copy_from_slice(b"0000644\x00");
            header[124..136].copy_from_slice(b"00000000000\x00");
            header[156] = b'0';
            header[257..262].copy_from_slice(b"ustar");
            header[263..265].copy_from_slice(b"00");
            header[345..345 + prefix.len()].copy_from_slice(prefix);
            image.extend_from_slice(&header);
        }
        image.extend(std::iter::repeat_n(0u8, 1024));
        let dup = dir.path().join("dup.tar");
        fs::write(&dup, &image).unwrap();
        let mut source = SeekableTarSource::new(&dup, ConsistencyPolicy::SnapshotBacked).unwrap();
        assert!(matches!(
            source.inventory(),
            Err(IngestError::DuplicatePath(_))
        ));
    }

    #[test]
    fn tar_source_hardlinks_share_identity_and_compressed_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        // one.txt carries data; two.txt is a hardlink to it.
        let mut image = Vec::new();
        let mut push = |name: &[u8], typeflag: u8, linkname: &[u8], data: &[u8]| {
            let mut header = [0u8; 512];
            header[..name.len()].copy_from_slice(name);
            header[100..108].copy_from_slice(b"0000644\x00");
            let size_field = format!("{:011o}\x00", data.len());
            header[124..136].copy_from_slice(size_field.as_bytes());
            header[136..148].copy_from_slice(b"00000000000\x00");
            header[156] = typeflag;
            header[157..157 + linkname.len()].copy_from_slice(linkname);
            header[257..262].copy_from_slice(b"ustar");
            header[263..265].copy_from_slice(b"00");
            image.extend_from_slice(&header);
            image.extend_from_slice(data);
            let pad = (512 - data.len() % 512) % 512;
            image.extend(std::iter::repeat_n(0u8, pad));
        };
        push(b"one.txt", b'0', b"", b"shared-data");
        push(b"two.txt", b'1', b"one.txt", b"");
        image.extend(std::iter::repeat_n(0u8, 1024));
        let tar = dir.path().join("hard.tar");
        fs::write(&tar, &image).unwrap();

        let mut source = SeekableTarSource::new(&tar, ConsistencyPolicy::SnapshotBacked).unwrap();
        let entries = source.inventory().unwrap();
        let one = entries.iter().find(|e| e.path == b"one.txt").unwrap();
        let two = entries.iter().find(|e| e.path == b"two.txt").unwrap();
        assert_eq!(one.hardlink_group, two.hardlink_group);
        // The link member reads through the target.
        let mut buf = [0u8; 11];
        source.read_range(&two.token, 0, &mut buf).unwrap();
        assert_eq!(&buf, b"shared-data");

        // Unresolved hardlink: target never seen.
        let mut image2 = Vec::new();
        let mut push2 = |name: &[u8], typeflag: u8, linkname: &[u8]| {
            let mut header = [0u8; 512];
            header[..name.len()].copy_from_slice(name);
            header[100..108].copy_from_slice(b"0000644\x00");
            header[124..136].copy_from_slice(b"00000000000\x00");
            header[136..148].copy_from_slice(b"00000000000\x00");
            header[156] = typeflag;
            header[157..157 + linkname.len()].copy_from_slice(linkname);
            header[257..262].copy_from_slice(b"ustar");
            header[263..265].copy_from_slice(b"00");
            image2.extend_from_slice(&header);
        };
        push2(b"lonely.txt", b'1', b"missing.txt");
        image2.extend(std::iter::repeat_n(0u8, 1024));
        let unresolved = dir.path().join("unresolved.tar");
        fs::write(&unresolved, &image2).unwrap();
        let mut source =
            SeekableTarSource::new(&unresolved, ConsistencyPolicy::SnapshotBacked).unwrap();
        assert!(matches!(
            source.inventory(),
            Err(IngestError::UnresolvedHardlink(_))
        ));

        // Compressed tar is refused by magic.
        let mut gz = Vec::new();
        gz.extend_from_slice(&[0x1f, 0x8b, 0x08, 0x00]);
        gz.extend(std::iter::repeat_n(0u8, 64));
        let gz_path = dir.path().join("compressed.tgz");
        fs::write(&gz_path, &gz).unwrap();
        assert!(matches!(
            SeekableTarSource::new(&gz_path, ConsistencyPolicy::SnapshotBacked),
            Err(IngestError::UnsupportedSource(_))
        ));
    }

    #[test]
    fn tar_sparse_member_reads_holes_as_zeros() {
        let dir = tempfile::tempdir().unwrap();
        // GNU sparse: logical size 1024, one extent [0,5) with 5 stored
        // bytes; [5,1024) is a hole.
        let data = b"head!";
        let mut header = [0u8; 512];
        header[..15].copy_from_slice(b"sparse-file.bin");
        header[100..108].copy_from_slice(b"0000644\x00");
        let size_field = format!("{:011o}\x00", data.len());
        header[124..136].copy_from_slice(size_field.as_bytes());
        header[136..148].copy_from_slice(b"00000000000\x00");
        header[156] = b'S';
        header[257..262].copy_from_slice(b"ustar");
        header[263..265].copy_from_slice(b"00");
        header[386..398].copy_from_slice(b"00000000000\x00"); // extent offset 0
        header[398..410].copy_from_slice(format!("{:011o}\x00", data.len()).as_bytes());
        header[482] = 0; // isextended = false
        header[483..495].copy_from_slice(format!("{:011o}\x00", 1024u64).as_bytes());

        let mut image = Vec::new();
        image.extend_from_slice(&header);
        image.extend_from_slice(data);
        image.extend(std::iter::repeat_n(0u8, (512 - data.len() % 512) % 512));
        image.extend(std::iter::repeat_n(0u8, 1024));
        let tar = dir.path().join("sparse.tar");
        fs::write(&tar, &image).unwrap();

        let mut source = SeekableTarSource::new(&tar, ConsistencyPolicy::SnapshotBacked).unwrap();
        let entries = source.inventory().unwrap();
        let entry = entries
            .iter()
            .find(|e| e.path == b"sparse-file.bin")
            .unwrap();
        match &entry.kind {
            EntryKind::File { size } => assert_eq!(*size, 1024),
            other => panic!("expected file, got {other:?}"),
        }
        assert_eq!(entry.sparse_ranges, vec![(0, data.len() as u64)]);
        // Read spanning extent + hole: data then zeros.
        let mut buf = [0u8; 16];
        source.read_range(&entry.token, 0, &mut buf).unwrap();
        assert_eq!(&buf[..5], data);
        assert!(buf[5..].iter().all(|b| *b == 0));
        // Read entirely inside the hole.
        let mut hole = [0u8; 8];
        source.read_range(&entry.token, 512, &mut hole).unwrap();
        assert!(hole.iter().all(|b| *b == 0));
    }

    #[test]
    fn inventory_encoding_roundtrip() {
        let entry = SourceEntry {
            raw_name: b"dir/file.txt".to_vec(),
            path: b"dir/file.txt".to_vec(),
            kind: EntryKind::File { size: 42 },
            attributes: SourceAttributes {
                mode: 0o644,
                mtime_ns: -1700000000,
            },
            hardlink_group: Some(7),
            sparse_ranges: vec![(0, 10)],
            token: SourceToken {
                location: b"dir/file.txt".to_vec(),
                identity: [9u8; 32],
            },
        };
        let entries = vec![entry.clone()];
        let bytes = encode_inventory(ConsistencyPolicy::BestEffortDetected, &entries);
        let (policy, decoded) = decode_inventory(&bytes).unwrap();
        assert_eq!(policy, ConsistencyPolicy::BestEffortDetected);
        assert_eq!(decoded, entries);
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(decode_inventory(&trailing).is_err());
    }
}
