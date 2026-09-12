package io.brewfs;

import com.sun.jna.Library;
import com.sun.jna.Memory;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;
import com.sun.jna.ptr.PointerByReference;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/** Thin, ownership-safe Java binding for the BrewFS C ABI v1. */
final class BrewFsNative {
  static final int ABI_VERSION = 1 << 16;
  static final int OPEN_READ = 1 << 0;
  static final int OPEN_WRITE = 1 << 1;
  static final int OPEN_CREATE = 1 << 2;
  static final int OPEN_TRUNCATE = 1 << 3;
  static final int OPEN_EXCLUSIVE = 1 << 4;
  static final int OPEN_APPEND = 1 << 5;

  static final int OK = 0;
  static final int END = 1;
  static final int BUFFER_TOO_SMALL = 2;
  static final int NOT_FOUND = -3;
  static final int ALREADY_EXISTS = -4;
  static final int NOT_A_DIRECTORY = -5;
  static final int IS_A_DIRECTORY = -6;
  static final int DIRECTORY_NOT_EMPTY = -7;
  static final int PERMISSION_DENIED = -8;
  static final int UNSUPPORTED = -11;
  static final int STALE_HANDLE = -12;

  /** Maximum single native I/O size (must stay in sync with the ABI max_io). */
  static final int MAX_IO = 16 * 1024 * 1024;

  interface Api extends Library {
    int brewfs_v1_abi_version();
    int brewfs_v1_client_open(BrewFsClientOptions options, PointerByReference output);
    int brewfs_v1_client_close(Pointer client);
    int brewfs_v1_open(Pointer client, byte[] path, long pathLength,
                       BrewFsOpenOptions options, PointerByReference output);
    int brewfs_v1_file_close(Pointer file);
    int brewfs_v1_read(Pointer file, byte[] buffer, long capacity, long[] output);
    int brewfs_v1_pread(Pointer file, long offset, byte[] buffer, long capacity, long[] output);
    int brewfs_v1_write(Pointer file, byte[] data, long length, long[] output);
    int brewfs_v1_pwrite(Pointer file, long offset, byte[] data, long length, long[] output);
    int brewfs_v1_seek(Pointer file, long offset);
    int brewfs_v1_flush(Pointer file);
    int brewfs_v1_fsync(Pointer file, byte dataOnly);
    int brewfs_v1_stat(Pointer client, byte[] path, long pathLength, BrewFsStat output);
    int brewfs_v1_mkdir(Pointer client, byte[] path, long pathLength);
    int brewfs_v1_mkdir_p(Pointer client, byte[] path, long pathLength);
    int brewfs_v1_delete(Pointer client, byte[] path, long pathLength, byte recursive);
    int brewfs_v1_rename(Pointer client, byte[] oldPath, long oldLength,
                         byte[] newPath, long newLength);
    int brewfs_v1_truncate(Pointer client, byte[] path, long pathLength, long size);
    int brewfs_v1_setattr(Pointer client, byte[] path, long pathLength, BrewFsSetAttr input);
    int brewfs_v1_readdir_open(Pointer client, byte[] path, long pathLength,
                               PointerByReference output);
    int brewfs_v1_readdir_next(Pointer dir, byte[] name, long capacity, long[] required,
                               long[] inode, int[] type, byte[] hasEntry);
    int brewfs_v1_readdir_close(Pointer dir);
    int brewfs_v1_get_xattr(Pointer client, byte[] path, long pathLength, byte[] name,
                            long nameLength, byte[] value, long capacity, long[] required);
    int brewfs_v1_set_xattr(Pointer client, byte[] path, long pathLength, byte[] name,
                            long nameLength, byte[] value, long valueLength, int flags);
    int brewfs_v1_remove_xattr(Pointer client, byte[] path, long pathLength,
                               byte[] name, long nameLength);
    int brewfs_v1_last_error(byte[] output, long capacity, long[] required);
  }

  @Structure.FieldOrder({"structSize", "flags", "dataDir", "dataDirLen", "metadataUrl",
      "metadataUrlLen", "chunkSize", "blockSize", "uid", "gid", "enforcePermissions", "reserved"})
  static class BrewFsClientOptions extends Structure {
    public int structSize = size();
    public int flags;
    public Pointer dataDir;
    public long dataDirLen;
    public Pointer metadataUrl;
    public long metadataUrlLen;
    public long chunkSize;
    public int blockSize;
    public int uid;
    public int gid;
    public byte enforcePermissions;
    public byte[] reserved = new byte[3];
  }

  @Structure.FieldOrder({"structSize", "flags", "mode"})
  static class BrewFsOpenOptions extends Structure {
    public int structSize = size();
    public int flags;
    public int mode;
  }

  @Structure.FieldOrder({"structSize", "fileType", "inode", "size", "blocks", "mode", "uid",
      "gid", "rdev", "nlink", "atimeNs", "mtimeNs", "ctimeNs"})
  static class BrewFsStat extends Structure {
    public int structSize = size();
    public int fileType;
    public long inode;
    public long size;
    public long blocks;
    public int mode;
    public int uid;
    public int gid;
    public int rdev;
    public int nlink;
    public long atimeNs;
    public long mtimeNs;
    public long ctimeNs;
  }

  @Structure.FieldOrder({"structSize", "validMask", "mode", "uid", "gid", "size", "atimeNs", "mtimeNs"})
  static class BrewFsSetAttr extends Structure {
    public int structSize = size();
    public int validMask;
    public int mode;
    public int uid;
    public int gid;
    public long size;
    public long atimeNs;
    public long mtimeNs;
  }

  private static final Api API = load();

  private static Api load() {
    String path = System.getProperty("brewfs.native.library.path");
    Api api = path == null || path.isEmpty()
        ? Native.load("brewfs", Api.class)
        : Native.load(path, Api.class);
    if ((api.brewfs_v1_abi_version() & 0xffff0000) != ABI_VERSION) {
      throw new LinkageError("unsupported BrewFS ABI version");
    }
    return api;
  }

  private BrewFsNative() {}

  static Pointer openClient(String dataDir, String metadataUrl, int uid, int gid,
                            boolean enforcePermissions) throws IOException {
    byte[] data = utf8(dataDir);
    byte[] meta = utf8(metadataUrl);
    Memory dataMemory = memory(data);
    Memory metaMemory = memory(meta);
    BrewFsClientOptions options = new BrewFsClientOptions();
    options.dataDir = dataMemory;
    options.dataDirLen = data.length;
    options.metadataUrl = metaMemory;
    options.metadataUrlLen = meta.length;
    options.chunkSize = 64L * 1024 * 1024;
    options.blockSize = 4 * 1024 * 1024;
    options.uid = uid;
    options.gid = gid;
    options.enforcePermissions = (byte) (enforcePermissions ? 1 : 0);
    options.write();
    PointerByReference output = new PointerByReference();
    check(API.brewfs_v1_client_open(options, output));
    return output.getValue();
  }

  static Pointer open(Pointer client, byte[] path, int flags, int mode) throws IOException {
    BrewFsOpenOptions options = new BrewFsOpenOptions();
    options.flags = flags;
    options.mode = mode;
    options.write();
    PointerByReference output = new PointerByReference();
    check(API.brewfs_v1_open(client, path, path.length, options, output));
    return output.getValue();
  }

  static int read(Pointer file, byte[] buffer, int length, long offset, boolean positioned)
      throws IOException {
    if (length < 0 || length > buffer.length) throw new IndexOutOfBoundsException();
    long[] output = new long[1];
    check(positioned ? API.brewfs_v1_pread(file, offset, buffer, length, output)
        : API.brewfs_v1_read(file, buffer, length, output));
    return checkedLength(output[0], length);
  }

  static int write(Pointer file, byte[] data, int length, long offset, boolean positioned)
      throws IOException {
    if (length < 0 || length > data.length) throw new IndexOutOfBoundsException();
    long[] output = new long[1];
    check(positioned ? API.brewfs_v1_pwrite(file, offset, data, length, output)
        : API.brewfs_v1_write(file, data, length, output));
    return checkedLength(output[0], length);
  }

  static void closeClient(Pointer client) throws IOException { check(API.brewfs_v1_client_close(client)); }
  static void closeFile(Pointer file) throws IOException { check(API.brewfs_v1_file_close(file)); }
  static void seek(Pointer file, long offset) throws IOException { check(API.brewfs_v1_seek(file, offset)); }
  static void flush(Pointer file) throws IOException { check(API.brewfs_v1_flush(file)); }
  static void hflush(Pointer file) throws IOException { check(API.brewfs_v1_fsync(file, (byte) 1)); }
  static void hsync(Pointer file) throws IOException { check(API.brewfs_v1_fsync(file, (byte) 0)); }

  static BrewFsStat stat(Pointer client, byte[] path) throws IOException {
    BrewFsStat stat = new BrewFsStat();
    stat.write();
    check(API.brewfs_v1_stat(client, path, path.length, stat));
    stat.read();
    return stat;
  }

  static void mkdir(Pointer client, byte[] path, boolean parents) throws IOException {
    check(parents ? API.brewfs_v1_mkdir_p(client, path, path.length)
        : API.brewfs_v1_mkdir(client, path, path.length));
  }

  static void delete(Pointer client, byte[] path, boolean recursive) throws IOException {
    check(API.brewfs_v1_delete(client, path, path.length, (byte) (recursive ? 1 : 0)));
  }

  static void rename(Pointer client, byte[] oldPath, byte[] newPath) throws IOException {
    check(API.brewfs_v1_rename(client, oldPath, oldPath.length, newPath, newPath.length));
  }

  static void truncate(Pointer client, byte[] path, long length) throws IOException {
    check(API.brewfs_v1_truncate(client, path, path.length, length));
  }

  static void setattr(Pointer client, byte[] path, BrewFsSetAttr input) throws IOException {
    input.write();
    check(API.brewfs_v1_setattr(client, path, path.length, input));
  }

  static Pointer openDir(Pointer client, byte[] path) throws IOException {
    PointerByReference output = new PointerByReference();
    check(API.brewfs_v1_readdir_open(client, path, path.length, output));
    return output.getValue();
  }

  static Entry next(Pointer dir) throws IOException {
    byte[] name = new byte[4096];
    long[] required = new long[1];
    long[] inode = new long[1];
    int[] type = new int[1];
    byte[] hasEntry = new byte[1];
    checkEnd(API.brewfs_v1_readdir_next(dir, name, name.length, required, inode, type, hasEntry));
    if (hasEntry[0] == 0) return null;
    return new Entry(new String(name, 0, checkedLength(required[0], name.length), StandardCharsets.UTF_8), inode[0], type[0]);
  }

  static void closeDir(Pointer dir) throws IOException { check(API.brewfs_v1_readdir_close(dir)); }

  static void check(int status) throws IOException {
    if (status != OK) throw error(status);
  }

  private static void checkEnd(int status) throws IOException {
    if (status != OK && status != END) throw error(status);
  }

  private static IOException error(int status) {
    String message = "BrewFS native operation failed (status=" + status + ")";
    try {
      long[] required = new long[1];
      API.brewfs_v1_last_error(new byte[0], 0, required);
      if (required[0] > 0 && required[0] < 1024 * 1024) {
        byte[] buffer = new byte[(int) required[0]];
        API.brewfs_v1_last_error(buffer, buffer.length, required);
        message += ": " + new String(buffer, StandardCharsets.UTF_8);
      }
    } catch (Throwable ignored) {
      // Keep the stable status when diagnostics are unavailable.
    }
    switch (status) {
      case NOT_FOUND: return new java.io.FileNotFoundException(message);
      case ALREADY_EXISTS: return new java.nio.file.FileAlreadyExistsException(message);
      case PERMISSION_DENIED: return new IOException(message, new java.security.AccessControlException(message));
      case UNSUPPORTED: return new IOException(message, new UnsupportedOperationException(message));
      default: return new IOException(message);
    }
  }

  private static int checkedLength(long length, int capacity) throws IOException {
    if (length < 0 || length > capacity || length > Integer.MAX_VALUE)
      throw new IOException("native length is out of range: " + length);
    return (int) length;
  }

  private static byte[] utf8(String value) {
    return value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
  }

  private static Memory memory(byte[] value) {
    Memory memory = new Memory(value.length == 0 ? 1 : value.length);
    if (value.length != 0) memory.write(0, value, 0, value.length);
    return memory;
  }

  static final class Entry {
    final String name;
    final long inode;
    final int fileType;
    Entry(String name, long inode, int fileType) {
      this.name = name;
      this.inode = inode;
      this.fileType = fileType;
    }
  }
}
