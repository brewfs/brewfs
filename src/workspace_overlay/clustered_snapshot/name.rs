use std::cmp::Ordering;
use std::fmt;

/// Raw POSIX name bytes used by packed-metadata-v2 indexes.
///
/// Names are intentionally not represented as `String`: byte ordering is the
/// on-disk ordering and no Unicode normalization or lossy conversion is
/// allowed.  The initial FUSE profile caps a component at 255 bytes.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NameBytes(Vec<u8>);

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NameError {
    Empty,
    ContainsNul,
    ContainsSlash,
    DotComponent,
    TooLong { len: usize, max: usize },
}

impl fmt::Display for NameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("packed metadata name is empty"),
            Self::ContainsNul => f.write_str("packed metadata name contains NUL"),
            Self::ContainsSlash => f.write_str("packed metadata name contains slash"),
            Self::DotComponent => f.write_str("packed metadata name is a dot component"),
            Self::TooLong { len, max } => {
                write!(f, "packed metadata name is {len} bytes, maximum is {max}")
            }
        }
    }
}

impl std::error::Error for NameError {}

impl NameBytes {
    pub const MAX_LEN: usize = 255;

    pub fn new(bytes: impl Into<Vec<u8>>) -> Result<Self, NameError> {
        let bytes = bytes.into();
        if bytes.is_empty() {
            return Err(NameError::Empty);
        }
        if bytes.contains(&0) {
            return Err(NameError::ContainsNul);
        }
        if bytes.contains(&b'/') {
            return Err(NameError::ContainsSlash);
        }
        if bytes == b"." || bytes == b".." {
            return Err(NameError::DotComponent);
        }
        if bytes.len() > Self::MAX_LEN {
            return Err(NameError::TooLong {
                len: bytes.len(),
                max: Self::MAX_LEN,
            });
        }
        Ok(Self(bytes))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    pub fn starts_with(&self, prefix: &[u8]) -> bool {
        self.0.starts_with(prefix)
    }
}

impl fmt::Debug for NameBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("NameBytes").field(&self.0).finish()
    }
}

impl AsRef<[u8]> for NameBytes {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl From<NameBytes> for Vec<u8> {
    fn from(name: NameBytes) -> Self {
        name.into_bytes()
    }
}

/// Explicit byte-wise comparison helper used at range boundaries.
pub fn compare_raw_name(left: &[u8], right: &[u8]) -> Ordering {
    left.cmp(right)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_names_are_sorted_by_bytes_without_utf8_conversion() {
        let mut names = vec![
            NameBytes::new(vec![0xff]).unwrap(),
            NameBytes::new(vec![0x80]).unwrap(),
            NameBytes::new(b"z".to_vec()).unwrap(),
        ];
        names.sort();
        assert_eq!(names[0].as_bytes(), b"z");
        assert_eq!(names[1].as_bytes(), &[0x80]);
        assert_eq!(names[2].as_bytes(), &[0xff]);
    }

    #[test]
    fn name_profile_rejects_empty_and_overlong_components() {
        assert_eq!(NameBytes::new(Vec::new()), Err(NameError::Empty));
        assert!(matches!(
            NameBytes::new(vec![b'x'; NameBytes::MAX_LEN + 1]),
            Err(NameError::TooLong { .. })
        ));
    }

    #[test]
    fn name_profile_rejects_path_syntax_but_keeps_non_utf8_bytes() {
        assert_eq!(NameBytes::new(vec![0]), Err(NameError::ContainsNul));
        assert_eq!(
            NameBytes::new(b"a/b".to_vec()),
            Err(NameError::ContainsSlash)
        );
        assert_eq!(NameBytes::new(b".".to_vec()), Err(NameError::DotComponent));
        assert_eq!(NameBytes::new(b"..".to_vec()), Err(NameError::DotComponent));
        assert_eq!(NameBytes::new(vec![0xff]).unwrap().as_bytes(), &[0xff]);
    }
}
