pub(crate) trait NumCastExt {
    fn as_u32(&self) -> u32;

    fn as_usize(&self) -> usize;

    fn as_i64(&self) -> i64;
}

impl NumCastExt for u64 {
    #[inline]
    fn as_u32(&self) -> u32 {
        debug_assert!(*self <= u32::MAX as u64);
        *self as u32
    }

    #[inline]
    fn as_usize(&self) -> usize {
        debug_assert!(*self <= usize::MAX as u64);
        *self as usize
    }

    #[inline]
    fn as_i64(&self) -> i64 {
        debug_assert!(*self <= i64::MAX as u64);
        *self as i64
    }
}

/// Portable `makedev(major, minor)`.
///
/// Uses `libc::makedev` on Unix; on Windows (where libc does not export it)
/// falls back to the Linux glibc encoding, which is sufficient for the
/// special-node round-trip tests.
#[cfg(test)]
pub(crate) fn makedev(major: u32, minor: u32) -> u64 {
    #[cfg(unix)]
    {
        libc::makedev(major, minor)
    }
    #[cfg(not(unix))]
    {
        ((major as u64 & 0xfff) << 8)
            | (minor as u64 & 0xff)
            | ((major as u64 & !0xfff) << 32)
            | ((minor as u64 & !0xff) << 12)
    }
}
