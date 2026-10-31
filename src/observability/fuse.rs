//! Feature-only FUSE decorator. Forward every admission, cancellation and
//! lifecycle hook as well as the request handlers; never inherit defaults.
use asyncfuse::{
    Inode, Result, SetAttr,
    notify::Notify,
    raw::{Filesystem, Request, reply::*},
};
use bytes::Bytes;
use futures_util::stream::Stream;
use std::ffi::OsStr;

pub(crate) struct ObservedFilesystem<F>(pub F);

impl<F: Filesystem + Send + Sync> Filesystem for ObservedFilesystem<F> {
    fn reserve_input_buffer_memory(&self, _bytes: u64) -> Result<Option<ReplyMemoryGuard>> {
        self.0.reserve_input_buffer_memory(_bytes)
    }
    fn reserve_inline_root_memory(
        &self,
        _bytes: u64,
    ) -> Result<Option<asyncfuse::raw::reply::InlineRootPermit>> {
        self.0.reserve_inline_root_memory(_bytes)
    }
    fn reserve_inline_prepare_memory(
        &self,
        bytes: u64,
    ) -> Result<Option<asyncfuse::raw::reply::InlineRootPermit>> {
        self.0.reserve_inline_prepare_memory(bytes)
    }
    fn reserve_request_memory(&self, _bytes: u64) -> Result<Option<ReplyMemoryGuard>> {
        self.0.reserve_request_memory(_bytes)
    }
    fn reserve_reply_memory(&self, _bytes: u64) -> Result<Option<ReplyMemoryGuard>> {
        self.0.reserve_reply_memory(_bytes)
    }
    fn reserve_control_memory(&self, _bytes: u64) -> Result<Option<ReplyMemoryGuard>> {
        self.0.reserve_control_memory(_bytes)
    }
    async fn init(&self, req: Request) -> Result<ReplyInit> {
        crate::observe_fuse!(super::operations::FuseOp::Init, { self.0.init(req).await })
    }
    async fn init_with_notify(&self, req: Request, _notify: Notify) -> Result<ReplyInit> {
        crate::observe_fuse!(super::operations::FuseOp::Init, {
            self.0.init_with_notify(req, _notify).await
        })
    }
    async fn destroy(&self, req: Request) {
        let timer = super::fuse_timer(super::operations::FuseOp::Destroy);
        self.0.destroy(req).await;
        if let Some(timer) = timer {
            timer.finish(None);
        }
    }
    async fn prepare_unmount(&self) -> Result<()> {
        self.0.prepare_unmount().await
    }
    async fn lookup(&self, req: Request, parent: Inode, name: &OsStr) -> Result<ReplyEntry> {
        crate::observe_fuse!(super::operations::FuseOp::Lookup, {
            self.0.lookup(req, parent, name).await
        })
    }
    async fn forget(&self, req: Request, inode: Inode, nlookup: u64) {
        let timer = super::fuse_timer(super::operations::FuseOp::Forget);
        self.0.forget(req, inode, nlookup).await;
        if let Some(timer) = timer {
            timer.finish(None);
        }
    }
    async fn getattr(
        &self,
        req: Request,
        inode: Inode,
        fh: Option<u64>,
        flags: u32,
    ) -> Result<ReplyAttr> {
        crate::observe_fuse!(super::operations::FuseOp::Getattr, {
            self.0.getattr(req, inode, fh, flags).await
        })
    }
    async fn setattr(
        &self,
        req: Request,
        inode: Inode,
        fh: Option<u64>,
        set_attr: SetAttr,
    ) -> Result<ReplyAttr> {
        crate::observe_fuse!(super::operations::FuseOp::Setattr, {
            self.0.setattr(req, inode, fh, set_attr).await
        })
    }
    async fn readlink(&self, req: Request, inode: Inode) -> Result<ReplyData> {
        crate::observe_fuse!(super::operations::FuseOp::Readlink, {
            self.0.readlink(req, inode).await
        })
    }
    async fn symlink(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        link: &OsStr,
    ) -> Result<ReplyEntry> {
        crate::observe_fuse!(super::operations::FuseOp::Symlink, {
            self.0.symlink(req, parent, name, link).await
        })
    }
    async fn mknod(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        mode: u32,
        rdev: u32,
    ) -> Result<ReplyEntry> {
        crate::observe_fuse!(super::operations::FuseOp::Mknod, {
            self.0.mknod(req, parent, name, mode, rdev).await
        })
    }
    async fn mknod_with_umask(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        rdev: u32,
    ) -> Result<ReplyEntry> {
        crate::observe_fuse!(super::operations::FuseOp::Mknod, {
            self.0
                .mknod_with_umask(req, parent, name, mode, _umask, rdev)
                .await
        })
    }
    async fn mkdir(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        mode: u32,
        umask: u32,
    ) -> Result<ReplyEntry> {
        crate::observe_fuse!(super::operations::FuseOp::Mkdir, {
            self.0.mkdir(req, parent, name, mode, umask).await
        })
    }
    async fn unlink(&self, req: Request, parent: Inode, name: &OsStr) -> Result<()> {
        crate::observe_fuse!(super::operations::FuseOp::Unlink, {
            self.0.unlink(req, parent, name).await
        })
    }
    async fn rmdir(&self, req: Request, parent: Inode, name: &OsStr) -> Result<()> {
        crate::observe_fuse!(super::operations::FuseOp::Rmdir, {
            self.0.rmdir(req, parent, name).await
        })
    }
    async fn rename(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        new_parent: Inode,
        new_name: &OsStr,
    ) -> Result<()> {
        crate::observe_fuse!(super::operations::FuseOp::Rename, {
            self.0.rename(req, parent, name, new_parent, new_name).await
        })
    }
    async fn link(
        &self,
        req: Request,
        inode: Inode,
        new_parent: Inode,
        new_name: &OsStr,
    ) -> Result<ReplyEntry> {
        crate::observe_fuse!(super::operations::FuseOp::Link, {
            self.0.link(req, inode, new_parent, new_name).await
        })
    }
    async fn open(&self, req: Request, inode: Inode, flags: u32) -> Result<ReplyOpen> {
        crate::observe_fuse!(super::operations::FuseOp::Open, {
            self.0.open(req, inode, flags).await
        })
    }
    async fn read(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> Result<ReplyData> {
        crate::observe_fuse!(super::operations::FuseOp::Read, {
            self.0.read(req, inode, fh, offset, size).await
        })
    }
    async fn write(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        offset: u64,
        data: &[u8],
        write_flags: u32,
        flags: u32,
    ) -> Result<ReplyWrite> {
        crate::observe_fuse!(super::operations::FuseOp::Write, {
            self.0
                .write(req, inode, fh, offset, data, write_flags, flags)
                .await
        })
    }
    async fn statfs(&self, req: Request, inode: Inode) -> Result<ReplyStatFs> {
        crate::observe_fuse!(super::operations::FuseOp::Statfs, {
            self.0.statfs(req, inode).await
        })
    }
    async fn release(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        flags: u32,
        lock_owner: u64,
        flush: bool,
    ) -> Result<()> {
        crate::observe_fuse!(super::operations::FuseOp::Release, {
            self.0
                .release(req, inode, fh, flags, lock_owner, flush)
                .await
        })
    }
    async fn fsync(&self, req: Request, inode: Inode, fh: u64, datasync: bool) -> Result<()> {
        crate::observe_fuse!(super::operations::FuseOp::Fsync, {
            self.0.fsync(req, inode, fh, datasync).await
        })
    }
    async fn setxattr(
        &self,
        req: Request,
        inode: Inode,
        name: &OsStr,
        value: &[u8],
        flags: u32,
        position: u32,
    ) -> Result<()> {
        crate::observe_fuse!(super::operations::FuseOp::Setxattr, {
            self.0
                .setxattr(req, inode, name, value, flags, position)
                .await
        })
    }
    async fn getxattr(
        &self,
        req: Request,
        inode: Inode,
        name: &OsStr,
        size: u32,
    ) -> Result<ReplyXAttr> {
        crate::observe_fuse!(super::operations::FuseOp::Getxattr, {
            self.0.getxattr(req, inode, name, size).await
        })
    }
    async fn listxattr(&self, req: Request, inode: Inode, size: u32) -> Result<ReplyXAttr> {
        crate::observe_fuse!(super::operations::FuseOp::Listxattr, {
            self.0.listxattr(req, inode, size).await
        })
    }
    async fn removexattr(&self, req: Request, inode: Inode, name: &OsStr) -> Result<()> {
        crate::observe_fuse!(super::operations::FuseOp::Removexattr, {
            self.0.removexattr(req, inode, name).await
        })
    }
    async fn flush(&self, req: Request, inode: Inode, fh: u64, lock_owner: u64) -> Result<()> {
        crate::observe_fuse!(super::operations::FuseOp::Flush, {
            self.0.flush(req, inode, fh, lock_owner).await
        })
    }
    async fn opendir(&self, req: Request, inode: Inode, flags: u32) -> Result<ReplyOpen> {
        crate::observe_fuse!(super::operations::FuseOp::Opendir, {
            self.0.opendir(req, inode, flags).await
        })
    }
    async fn readdir<'a>(
        &'a self,
        req: Request,
        parent: Inode,
        fh: u64,
        offset: i64,
    ) -> Result<ReplyDirectory<impl Stream<Item = Result<DirectoryEntry>> + Send + 'a>> {
        crate::observe_fuse!(super::operations::FuseOp::Readdir, {
            self.0.readdir(req, parent, fh, offset).await
        })
    }
    async fn releasedir(&self, req: Request, inode: Inode, fh: u64, flags: u32) -> Result<()> {
        crate::observe_fuse!(super::operations::FuseOp::Releasedir, {
            self.0.releasedir(req, inode, fh, flags).await
        })
    }
    async fn fsyncdir(&self, req: Request, inode: Inode, fh: u64, datasync: bool) -> Result<()> {
        crate::observe_fuse!(super::operations::FuseOp::Fsyncdir, {
            self.0.fsyncdir(req, inode, fh, datasync).await
        })
    }
    async fn getlk(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        lock_owner: u64,
        start: u64,
        end: u64,
        r#type: u32,
        pid: u32,
    ) -> Result<ReplyLock> {
        crate::observe_fuse!(super::operations::FuseOp::Getlk, {
            self.0
                .getlk(req, inode, fh, lock_owner, start, end, r#type, pid)
                .await
        })
    }
    async fn setlk(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        lock_owner: u64,
        start: u64,
        end: u64,
        r#type: u32,
        pid: u32,
        block: bool,
    ) -> Result<()> {
        crate::observe_fuse!(super::operations::FuseOp::Setlk, {
            self.0
                .setlk(req, inode, fh, lock_owner, start, end, r#type, pid, block)
                .await
        })
    }
    async fn access(&self, req: Request, inode: Inode, mask: u32) -> Result<()> {
        crate::observe_fuse!(super::operations::FuseOp::Access, {
            self.0.access(req, inode, mask).await
        })
    }
    async fn create(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        mode: u32,
        flags: u32,
    ) -> Result<ReplyCreated> {
        crate::observe_fuse!(super::operations::FuseOp::Create, {
            self.0.create(req, parent, name, mode, flags).await
        })
    }
    async fn create_with_umask(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        flags: u32,
    ) -> Result<ReplyCreated> {
        crate::observe_fuse!(super::operations::FuseOp::Create, {
            self.0
                .create_with_umask(req, parent, name, mode, _umask, flags)
                .await
        })
    }
    async fn interrupt(&self, req: Request, unique: u64) -> Result<()> {
        crate::observe_fuse!(super::operations::FuseOp::Interrupt, {
            self.0.interrupt(req, unique).await
        })
    }
    fn supports_read_cancellation(&self) -> bool {
        self.0.supports_read_cancellation()
    }
    async fn bmap(
        &self,
        req: Request,
        inode: Inode,
        blocksize: u32,
        idx: u64,
    ) -> Result<ReplyBmap> {
        crate::observe_fuse!(super::operations::FuseOp::Bmap, {
            self.0.bmap(req, inode, blocksize, idx).await
        })
    }
    async fn ioctl(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        flags: u32,
        cmd: u32,
        arg: u64,
        in_data: &[u8],
        out_size: u32,
    ) -> Result<ReplyIoctl> {
        crate::observe_fuse!(super::operations::FuseOp::Ioctl, {
            self.0
                .ioctl(req, inode, fh, flags, cmd, arg, in_data, out_size)
                .await
        })
    }
    async fn poll(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        kh: Option<u64>,
        flags: u32,
        events: u32,
        notify: &Notify,
    ) -> Result<ReplyPoll> {
        crate::observe_fuse!(super::operations::FuseOp::Poll, {
            self.0.poll(req, inode, fh, kh, flags, events, notify).await
        })
    }
    async fn notify_reply(
        &self,
        req: Request,
        inode: Inode,
        offset: u64,
        data: Bytes,
    ) -> Result<()> {
        crate::observe_fuse!(super::operations::FuseOp::NotifyReply, {
            self.0.notify_reply(req, inode, offset, data).await
        })
    }
    async fn batch_forget(&self, req: Request, inodes: &[(Inode, u64)]) {
        let timer = super::fuse_timer(super::operations::FuseOp::BatchForget);
        self.0.batch_forget(req, inodes).await;
        if let Some(timer) = timer {
            timer.finish(None);
        }
    }
    async fn fallocate(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        offset: u64,
        length: u64,
        mode: u32,
    ) -> Result<()> {
        crate::observe_fuse!(super::operations::FuseOp::Fallocate, {
            self.0.fallocate(req, inode, fh, offset, length, mode).await
        })
    }
    async fn readdirplus<'a>(
        &'a self,
        req: Request,
        parent: Inode,
        fh: u64,
        offset: u64,
        lock_owner: u64,
    ) -> Result<ReplyDirectoryPlus<impl Stream<Item = Result<DirectoryEntryPlus>> + Send + 'a>>
    {
        crate::observe_fuse!(super::operations::FuseOp::Readdirplus, {
            self.0
                .readdirplus(req, parent, fh, offset, lock_owner)
                .await
        })
    }
    async fn rename2(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        new_parent: Inode,
        new_name: &OsStr,
        flags: u32,
    ) -> Result<()> {
        crate::observe_fuse!(super::operations::FuseOp::Rename, {
            self.0
                .rename2(req, parent, name, new_parent, new_name, flags)
                .await
        })
    }
    async fn lseek(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        offset: u64,
        whence: u32,
    ) -> Result<ReplyLSeek> {
        crate::observe_fuse!(super::operations::FuseOp::Lseek, {
            self.0.lseek(req, inode, fh, offset, whence).await
        })
    }
    async fn copy_file_range(
        &self,
        req: Request,
        inode: Inode,
        fh_in: u64,
        off_in: u64,
        inode_out: Inode,
        fh_out: u64,
        off_out: u64,
        length: u64,
        flags: u64,
    ) -> Result<ReplyCopyFileRange> {
        crate::observe_fuse!(super::operations::FuseOp::CopyFileRange, {
            self.0
                .copy_file_range(
                    req, inode, fh_in, off_in, inode_out, fh_out, off_out, length, flags,
                )
                .await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
    struct Fixture {
        reads: AtomicUsize,
        unmounts: AtomicUsize,
    }
    impl Filesystem for Fixture {
        async fn init(&self, _: Request) -> Result<ReplyInit> {
            Err(libc::EIO.into())
        }
        async fn destroy(&self, _: Request) {}
        async fn getlk(
            &self,
            _: Request,
            ino: Inode,
            fh: u64,
            owner: u64,
            start: u64,
            end: u64,
            kind: u32,
            pid: u32,
        ) -> Result<ReplyLock> {
            assert_eq!(
                (ino, fh, owner, start, end, kind, pid),
                (7, 8, 9, 10, 11, 12, 13)
            );
            Err(libc::ENOLCK.into())
        }
        async fn setlk(
            &self,
            _: Request,
            ino: Inode,
            fh: u64,
            owner: u64,
            start: u64,
            end: u64,
            kind: u32,
            pid: u32,
            block: bool,
        ) -> Result<()> {
            assert_eq!(
                (ino, fh, owner, start, end, kind, pid),
                (7, 8, 9, 10, 11, 12, 13)
            );
            assert!(block);
            Err(libc::EAGAIN.into())
        }
        fn reserve_reply_memory(&self, bytes: u64) -> Result<Option<ReplyMemoryGuard>> {
            assert_eq!(bytes, 123);
            Err(libc::ENOMEM.into())
        }
        fn supports_read_cancellation(&self) -> bool {
            true
        }
        async fn prepare_unmount(&self) -> Result<()> {
            self.unmounts.fetch_add(1, Relaxed);
            Ok(())
        }
        async fn read(
            &self,
            _: Request,
            ino: Inode,
            fh: u64,
            offset: u64,
            size: u32,
        ) -> Result<ReplyData> {
            assert_eq!((ino, fh, offset, size), (7, 8, 9, 10));
            self.reads.fetch_add(1, Relaxed);
            Err(libc::EIO.into())
        }
    }
    #[tokio::test]
    async fn decorator_preserves_admission_cancellation_errors_and_teardown() {
        let fs = ObservedFilesystem(Fixture {
            reads: AtomicUsize::new(0),
            unmounts: AtomicUsize::new(0),
        });
        assert_eq!(
            fs.reserve_reply_memory(123).err(),
            Some(libc::ENOMEM.into())
        );
        assert!(fs.supports_read_cancellation());
        assert_eq!(
            fs.read(Request::default(), 7, 8, 9, 10).await.err(),
            Some(libc::EIO.into())
        );
        assert_eq!(fs.0.reads.load(Relaxed), 1);
        assert_eq!(
            fs.getlk(Request::default(), 7, 8, 9, 10, 11, 12, 13)
                .await
                .err(),
            Some(libc::ENOLCK.into())
        );
        assert_eq!(
            fs.setlk(Request::default(), 7, 8, 9, 10, 11, 12, 13, true)
                .await
                .err(),
            Some(libc::EAGAIN.into())
        );
        fs.prepare_unmount().await.unwrap();
        assert_eq!(fs.0.unmounts.load(Relaxed), 1);
    }
}
