"""Two-level experimental FUSE namespace for observing positive and negative dentries."""

import argparse
import errno
import os
from pathlib import Path
import stat

import pyfuse3 as fuse
import redis
import trio

from directory import Directory


class Overlay(fuse.Operations):
    def __init__(self, client, reader, lower):
        super().__init__()
        self.client = client
        self.reader = reader
        self.lower = lower
        self.directory = {name: Directory(client, name) for name in lower}
        self.granted = set()
        self.writable = set()
        self.streams = {}
        self.next_stream = 1
        self.names = {name: set() for name in lower}
        self.ids = {(None, b""): fuse.ROOT_INODE}
        self.paths = {fuse.ROOT_INODE: (None, b"")}
        for name in lower:
            self.inode(name, b"")

    def inode(self, directory, name):
        key = (directory, name)
        if key not in self.ids:
            ino = len(self.ids) + 1
            self.ids[key] = ino
            self.paths[ino] = key
        return self.ids[key]

    def ttl(self, directory):
        if directory in self.granted:
            return 3600
        if directory in self.writable:
            return 1
        phase = self.directory[directory].grant(self.reader)
        if phase == "sealed":
            self.granted.add(directory)
            return 3600
        if phase == "writable":
            self.writable.add(directory)
            return 1
        return 0

    def count(self, operation, directory):
        self.client.hincrby(f"counts:{self.reader}", f"{operation}:{directory}", 1)

    def file(self, directory, name):
        if directory not in self.granted:
            data = self.client.hget(self.directory[directory].files, name)
            if data is not None:
                return data
        path = self.lower[directory] / os.fsdecode(name)
        if path.is_file():
            return path
        raise FileNotFoundError

    def attributes(self, directory, name):
        attr = fuse.EntryAttributes()
        ttl = self.ttl(directory) if directory else 3600
        attr.entry_timeout = attr.attr_timeout = ttl
        if directory and name and ttl == 3600:
            self.names[directory].add(name)
        if not name:
            attr.st_mode = stat.S_IFDIR | 0o555
            attr.st_nlink = 2
            if directory:
                attr.st_mtime_ns = self.lower[directory].stat().st_mtime_ns
                if ttl != 3600:
                    modified = self.client.hget(self.directory[directory].state, "modified_ns")
                    if modified:
                        attr.st_mtime_ns = int(modified)
        else:
            try:
                obj = self.file(directory, name)
            except FileNotFoundError:
                attr.st_ino = 0
                return attr
            attr.st_mode = stat.S_IFREG | 0o444
            attr.st_nlink = 1
            attr.st_size = len(obj) if isinstance(obj, bytes) else obj.stat().st_size
        attr.st_ino = self.inode(directory, name)
        attr.st_uid, attr.st_gid = os.getuid(), os.getgid()
        return attr

    async def lookup(self, parent_inode, name, ctx=None):
        if parent_inode == fuse.ROOT_INODE:
            directory = os.fsdecode(name)
            if directory not in self.lower:
                raise fuse.FUSEError(errno.ENOENT)
            return self.attributes(directory, b"")
        directory, parent_name = self.paths[parent_inode]
        if parent_name:
            raise fuse.FUSEError(errno.ENOTDIR)
        self.count("lookup", directory)
        return self.attributes(directory, name)

    async def getattr(self, inode, ctx=None):
        directory, name = self.paths[inode]
        self.count("getattr", directory)
        attr = self.attributes(directory, name)
        if attr.st_ino == 0:
            raise fuse.FUSEError(errno.ENOENT)
        return attr

    async def open(self, inode, flags, ctx=None):
        if flags & os.O_ACCMODE != os.O_RDONLY:
            raise fuse.FUSEError(errno.EROFS)
        return fuse.FileInfo(fh=inode, direct_io=True)

    async def opendir(self, inode, ctx=None):
        directory, name = self.paths[inode]
        if name:
            raise fuse.FUSEError(errno.ENOTDIR)
        if directory is None:
            names = {os.fsencode(key) for key in self.lower}
        else:
            ttl = self.ttl(directory)
            names = {os.fsencode(p.name) for p in self.lower[directory].iterdir()}
            if ttl != 3600:
                names.update(self.client.hkeys(self.directory[directory].files))
        handle = self.next_stream
        self.next_stream += 1
        self.streams[handle] = directory, sorted(names)
        return handle

    async def readdir(self, fh, start_id, token):
        directory, names = self.streams[fh]
        for index in range(start_id, len(names)):
            name = names[index]
            attr = (self.attributes(directory, name) if directory
                    else self.attributes(os.fsdecode(name), b""))
            if not fuse.readdir_reply(token, name, attr, index + 1):
                break

    async def releasedir(self, fh):
        del self.streams[fh]

    async def read(self, fh, offset, size):
        directory, name = self.paths[fh]
        obj = self.file(directory, name)
        if isinstance(obj, bytes):
            return obj[offset:offset + size]
        with obj.open("rb") as stream:
            stream.seek(offset)
            return stream.read(size)

    @staticmethod
    def invalidate(function, *args):
        try:
            function(*args)
        except FileNotFoundError:
            pass  # The kernel already evicted this entry.

    async def revoke(self, directory):
        # Stop issuing long replies on the FUSE loop before taking the snapshot.
        self.granted.remove(directory)
        entries = [(name, self.ids.get((directory, name))) for name in self.names[directory]]
        parent = self.ids[directory, b""]

        def notify():
            for name, inode in entries:
                self.invalidate(fuse.invalidate_entry, parent, name)
                if inode is not None:
                    self.invalidate(fuse.invalidate_inode, inode)
            self.invalidate(fuse.invalidate_entry, fuse.ROOT_INODE, os.fsencode(directory))
            self.invalidate(fuse.invalidate_inode, parent, True)

        await trio.to_thread.run_sync(notify)
        self.names[directory].clear()
        self.client.hset(f"counts:{self.reader}", f"revoked-names:{directory}", len(entries))
        self.client.srem(self.directory[directory].readers, self.reader)

    async def watch(self):
        while True:
            for name in tuple(self.granted):
                if self.client.hget(self.directory[name].state, "phase") == b"draining":
                    await self.revoke(name)
            await trio.sleep(0.02)


async def serve(overlay):
    async with trio.open_nursery() as nursery:
        nursery.start_soon(overlay.watch)
        await fuse.main()
        nursery.cancel_scope.cancel()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ("socket", "reader", "mount", "changed", "sibling"):
        parser.add_argument("--" + flag, required=True)
    opt = parser.parse_args()
    db = redis.Redis(unix_socket_path=opt.socket)
    fs = Overlay(db, opt.reader, {k: Path(getattr(opt, k)) for k in ("changed", "sibling")})
    fuse.init(fs, opt.mount, set(fuse.default_options) | {"fsname=overlay-ttl-experiment", "ro"})
    try:
        trio.run(serve, fs)
    finally:
        fuse.close()
        if not os.path.ismount(opt.mount):
            for d in fs.directory.values():
                db.srem(d.readers, opt.reader)
