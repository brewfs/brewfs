use std::io;
use std::os::fd::AsFd;
use std::os::fd::BorrowedFd;
use std::sync::Arc;

use nix::errno::Errno;

use crate::dev_fuse::DevFuse;
use crate::passthrough::BackingId;

/// A raw communication channel to the FUSE kernel driver
#[derive(Debug, Clone)]
pub(crate) struct Channel(Arc<DevFuse>);

impl AsFd for Channel {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.0.as_fd()
    }
}

impl Channel {
    /// Create a new communication channel to the kernel driver by mounting the
    /// given path. The kernel driver will delegate filesystem operations of
    /// the given path to the channel.
    pub(crate) fn new(device: Arc<DevFuse>) -> Self {
        Self(device)
    }

    /// Receives data up to the capacity of the given buffer (can block).
    fn receive(&self, buffer: &mut [u8]) -> nix::Result<usize> {
        #[cfg(all(feature = "fuse-t", target_os = "macos"))]
        {
            // FUSE-T transports FUSE over a SOCK_STREAM socketpair. Unlike
            // /dev/fuse (one request per read()), a stream socket can coalesce
            // several requests into one read() or split one request across
            // several reads(). Read the fixed-size header first and then the
            // request body so each call returns exactly one complete FUSE
            // request; otherwise pipelined requests (e.g. NFS readahead) are
            // silently dropped and the mount hangs.
            use std::io::Read as _;

            fn read_exact(mut file: &std::fs::File, mut buf: &mut [u8]) -> nix::Result<()> {
                while !buf.is_empty() {
                    match file.read(buf) {
                        Ok(0) => return Err(nix::errno::Errno::EIO),
                        Ok(n) => buf = &mut buf[n..],
                        Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                        Err(e) => {
                            return Err(nix::errno::Errno::from_raw(
                                e.raw_os_error().unwrap_or(nix::errno::Errno::EIO as i32),
                            ));
                        }
                    }
                }
                Ok(())
            }

            const HDR_SIZE: usize = std::mem::size_of::<crate::ll::fuse_abi::fuse_in_header>();
            debug_assert_eq!(HDR_SIZE, 40);
            let mut hdr = [0u8; 40];
            read_exact(&self.0.0, &mut hdr)?;
            let len = u32::from_ne_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
            if len < HDR_SIZE || len > buffer.len() {
                return Err(nix::errno::Errno::EIO);
            }
            buffer[..HDR_SIZE].copy_from_slice(&hdr);
            read_exact(&self.0.0, &mut buffer[HDR_SIZE..len])?;
            Ok(len)
        }
        #[cfg(not(all(feature = "fuse-t", target_os = "macos")))]
        {
            nix::unistd::read(&self.0, buffer)
        }
    }

    /// Receives data up to the capacity of the given buffer (can block),
    /// retrying on errors that are safe to retry (ENOENT, EINTR, EAGAIN).
    ///
    /// - ENOENT: Operation interrupted. According to FUSE, this is safe to retry.
    /// - EINTR: Interrupted system call, retry.
    /// - EAGAIN: Explicitly instructed to try again.
    pub(crate) fn receive_retrying(&self, buffer: &mut [u8]) -> nix::Result<usize> {
        loop {
            match self.receive(buffer) {
                Ok(size) => return Ok(size),
                Err(Errno::ENOENT | Errno::EINTR | Errno::EAGAIN) => continue,
                Err(err) => return Err(err),
            }
        }
    }

    /// Returns a sender object for this channel. The sender object can be
    /// used to send to the channel. Multiple sender objects can be used
    /// and they can safely be sent to other threads.
    pub(crate) fn sender(&self) -> ChannelSender {
        // Since write/writev syscalls are threadsafe, we can simply create
        // a sender by using the same file and use it in other threads.
        ChannelSender(self.0.clone())
    }

    /// Clone the FUSE device fd using FUSE_DEV_IOC_CLONE ioctl.
    ///
    /// This creates a new fd that can read FUSE requests independently,
    /// enabling true parallel request processing. The kernel distributes
    /// requests across all cloned fds.
    ///
    /// Requires Linux 4.5+. Returns an error on older kernels or non-Linux.
    #[cfg(target_os = "linux")]
    pub(crate) fn clone_fd(&self) -> io::Result<Channel> {
        use std::os::fd::AsRawFd;

        let new_dev = DevFuse::open()?;

        let mut source_fd = self.0.as_raw_fd() as u32;
        // SAFETY: fuse_dev_ioc_clone is a valid ioctl for /dev/fuse
        unsafe {
            crate::ll::ioctl::fuse_dev_ioc_clone(new_dev.as_raw_fd(), &mut source_fd)?;
        }

        Ok(Channel::new(Arc::new(new_dev)))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ChannelSender(Arc<DevFuse>);

impl ChannelSender {
    pub(crate) fn send(&self, bufs: &[io::IoSlice<'_>]) -> io::Result<()> {
        let rc = nix::sys::uio::writev(&self.0, bufs)?;
        // writev is atomic, so do not need to check how many bytes are written.
        // libfuse does not do it either
        // https://github.com/libfuse/libfuse/blob/6278995cca991978abd25ebb2c20ebd3fc9e8a13/lib/fuse_lowlevel.c#L267
        debug_assert_eq!(bufs.iter().map(|b| b.len()).sum::<usize>(), rc);
        Ok(())
    }

    pub(crate) fn open_backing(&self, fd: BorrowedFd<'_>) -> std::io::Result<BackingId> {
        BackingId::create(&self.0, fd)
    }
}
