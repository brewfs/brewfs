mod chain;
mod dentry;
mod extent;
mod inode;
mod xattr;

pub use chain::validate_layer_chain;
pub use dentry::{
    ResolvedDentry, resolve_dentry, resolve_dentry_state, resolve_directory,
    resolve_directory_state,
};
pub use extent::{ResolvedCoverage, ResolvedExtent, resolve_extent_coverage, resolve_extents};
pub use inode::{ResolvedInode, resolve_inode, resolve_inode_state};
pub use xattr::{
    ResolvedAcl, ResolvedXattr, resolve_acl, resolve_acl_state, resolve_xattr, resolve_xattr_state,
};

/// Non-persistent resolution state. Only Absent may fall through to an
/// authenticated packed lower; a mask is a terminal deletion in this view.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Resolution<T> {
    Present(T),
    Masked,
    Absent,
}

impl<T> Resolution<T> {
    /// Native terminal resolution has no further lower provider to consult.
    pub fn into_option(self) -> Option<T> {
        match self {
            Self::Present(value) => Some(value),
            Self::Masked | Self::Absent => None,
        }
    }

    pub fn map<U>(self, convert: impl FnOnce(T) -> U) -> Resolution<U> {
        match self {
            Self::Present(value) => Resolution::Present(convert(value)),
            Self::Masked => Resolution::Masked,
            Self::Absent => Resolution::Absent,
        }
    }
}
