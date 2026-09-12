package io.brewfs;

import org.apache.hadoop.conf.Configuration;
import com.sun.jna.Pointer;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/** Experimental Hadoop FileSystem adapter backed by BrewFS C ABI v1. */
public final class BrewFsFileSystem extends FileSystem {
  private static final int SET_MODE = 1 << 0;
  private static final int SET_UID = 1 << 1;
  private static final int SET_GID = 1 << 2;
  private static final int SET_SIZE = 1 << 3;
  private static final int SET_ATIME = 1 << 4;
  private static final int SET_MTIME = 1 << 5;

  private PointerState nativeClient;
  private URI uri;
  private volatile Path workingDirectory;

  @Override
  public synchronized void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);
    PointerState previous = nativeClient;
    if (previous != null && previous.closed.compareAndSet(false, true)) {
      BrewFsNative.closeClient(previous.pointer);
    }
    this.uri = name;
    String dataDir = conf.get("brewfs.data.dir");
    if (dataDir == null || dataDir.isEmpty()) {
      throw new IOException("brewfs.data.dir must be configured for the experimental adapter");
    }
    // Default metadata URL (when unset) is a persistent SQLite catalog under
    // the data dir, applied by the native layer; an in-memory catalog would
    // make clients sharing a data dir invisible to each other.
    String metadataUrl = conf.get("brewfs.metadata.url");
    int uid = conf.getInt("brewfs.uid", 0);
    int gid = conf.getInt("brewfs.gid", 0);
    boolean enforce = conf.getBoolean("brewfs.enforce.permissions", false);
    nativeClient = new PointerState(BrewFsNative.openClient(dataDir, metadataUrl, uid, gid, enforce));
    workingDirectory = new Path(conf.get("fs.defaultFS.working.dir", "/"));
  }

  private Pointer client() throws IOException {
    if (nativeClient == null || nativeClient.pointer == null || nativeClient.closed.get()) {
      throw new IOException("BrewFS FileSystem is closed");
    }
    return nativeClient.pointer;
  }

  private static byte[] pathBytes(Path path) throws IOException {
    URI value = path.toUri();
    if (value.getScheme() != null && !"jfs".equalsIgnoreCase(value.getScheme())
        && !"brewfs".equalsIgnoreCase(value.getScheme())) {
      throw new IOException("unsupported scheme: " + value.getScheme());
    }
    String text = value.getPath();
    if (text == null || text.isEmpty()) text = "/";
    if (!text.startsWith("/")) text = "/" + text;
    return text.getBytes(StandardCharsets.UTF_8);
  }

  private Path qualify(Path path) {
    if (path.isAbsolute()) return path;
    return new Path(workingDirectory, path);
  }

  @Override
  public URI getUri() {
    return uri == null ? URI.create("jfs:///") : uri;
  }

  @Override
  public String getScheme() {
    return "jfs";
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    Pointer file = BrewFsNative.open(client(), pathBytes(qualify(path)), BrewFsNative.OPEN_READ, 0);
    return new FSDataInputStream(new NativeInputStream(file));
  }

  @Override
  public FSDataOutputStream create(Path path, FsPermission permission, boolean overwrite,
                                   int bufferSize, short replication, long blockSize,
                                   Progressable progress) throws IOException {
    int flags = BrewFsNative.OPEN_WRITE | BrewFsNative.OPEN_CREATE;
    flags |= overwrite ? BrewFsNative.OPEN_TRUNCATE : BrewFsNative.OPEN_EXCLUSIVE;
    Pointer file = BrewFsNative.open(client(), pathBytes(qualify(path)), flags,
        permission == null ? 0666 : permission.toShort() & 0xffff);
    return new FSDataOutputStream(new NativeOutputStream(file), statistics);
  }

  @Override
  public FSDataOutputStream append(Path path, int bufferSize, Progressable progress) throws IOException {
    Path qualified = qualify(path);
    Pointer file = BrewFsNative.open(client(), pathBytes(qualified),
        BrewFsNative.OPEN_WRITE | BrewFsNative.OPEN_APPEND, 0);
    // Native positioned writes are absolute, so an append stream must start
    // at the current end of the file.
    long end = BrewFsNative.stat(client(), pathBytes(qualified)).size;
    return new FSDataOutputStream(new NativeOutputStream(file, end), statistics);
  }

  @Override
  public boolean rename(Path source, Path destination) throws IOException {
    try {
      BrewFsNative.rename(client(), pathBytes(qualify(source)), pathBytes(qualify(destination)));
      return true;
    } catch (FileNotFoundException e) {
      // Hadoop contract: rename returns false when the source is missing.
      return false;
    }
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    try {
      BrewFsNative.delete(client(), pathBytes(qualify(path)), recursive);
      return true;
    } catch (FileNotFoundException e) {
      // Hadoop contract: delete returns false when the path is missing.
      return false;
    }
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    Path qualified = qualify(path);
    Pointer dir = BrewFsNative.openDir(client(), pathBytes(qualified));
    List<FileStatus> result = new ArrayList<FileStatus>();
    try {
      BrewFsNative.Entry entry;
      while ((entry = BrewFsNative.next(dir)) != null) {
        Path child = new Path(qualified, entry.name);
        result.add(getFileStatus(child));
      }
    } finally {
      BrewFsNative.closeDir(dir);
    }
    return result.toArray(new FileStatus[result.size()]);
  }

  @Override
  public boolean mkdirs(Path path, FsPermission permission) throws IOException {
    BrewFsNative.mkdir(client(), pathBytes(qualify(path)), true);
    if (permission != null) {
      BrewFsNative.BrewFsSetAttr attr = new BrewFsNative.BrewFsSetAttr();
      attr.validMask = SET_MODE;
      attr.mode = permission.toShort() & 0xffff;
      BrewFsNative.setattr(client(), pathBytes(qualify(path)), attr);
    }
    return true;
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    Path qualified = qualify(path);
    BrewFsNative.BrewFsStat stat = BrewFsNative.stat(client(), pathBytes(qualified));
    boolean directory = stat.fileType == 2;
    FsPermission permission = new FsPermission((short) (stat.mode & 07777));
    return new FileStatus(stat.size, directory, 1, 0, stat.mtimeNs / 1_000_000L,
        stat.atimeNs / 1_000_000L, permission, Integer.toString(stat.uid),
        Integer.toString(stat.gid), qualified);
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDirectory == null ? new Path("/") : workingDirectory;
  }

  @Override
  public void setWorkingDirectory(Path newDir) {
    workingDirectory = qualify(newDir);
  }

  @Override
  public Path getHomeDirectory() {
    return new Path("/user/" + System.getProperty("user.name", "unknown"));
  }

  @Override
  public void setPermission(Path path, FsPermission permission) throws IOException {
    BrewFsNative.BrewFsSetAttr attr = new BrewFsNative.BrewFsSetAttr();
    attr.validMask = SET_MODE;
    attr.mode = permission.toShort() & 0xffff;
    BrewFsNative.setattr(client(), pathBytes(qualify(path)), attr);
  }

  @Override
  public void close() throws IOException {
    PointerState state = nativeClient;
    if (state == null || !state.closed.compareAndSet(false, true)) return;
    BrewFsNative.closeClient(state.pointer);
    nativeClient = null;
    super.close();
  }

  private static final class PointerState {
    final Pointer pointer;
    final AtomicBoolean closed = new AtomicBoolean(false);
    PointerState(Pointer pointer) { this.pointer = pointer; }
  }

  private static final class NativeInputStream extends FSInputStream {
    private Pointer file;
    private long position;
    private boolean closed;

    NativeInputStream(Pointer file) { this.file = file; }

    @Override
    public int read() throws IOException {
      byte[] one = new byte[1];
      int count = read(one, 0, 1);
      return count < 0 ? -1 : one[0] & 0xff;
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
      check(buffer, offset, length);
      if (length == 0) return 0;
      int total = 0;
      byte[] chunk = new byte[Math.min(length, BrewFsNative.MAX_IO)];
      while (total < length) {
        int want = Math.min(length - total, chunk.length);
        int count = BrewFsNative.read(file, chunk, want, position, false);
        if (count <= 0) break;
        System.arraycopy(chunk, 0, buffer, offset + total, count);
        position += count;
        total += count;
        if (count < want) break; // EOF
      }
      return total == 0 ? -1 : total;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
      check(buffer, offset, length);
      if (length == 0) return 0;
      int total = 0;
      byte[] chunk = new byte[Math.min(length, BrewFsNative.MAX_IO)];
      while (total < length) {
        int want = Math.min(length - total, chunk.length);
        int count = BrewFsNative.read(file, chunk, want, position + total, true);
        if (count <= 0) break;
        System.arraycopy(chunk, 0, buffer, offset + total, count);
        total += count;
        if (count < want) break; // EOF
      }
      return total == 0 ? -1 : total;
    }

    @Override
    public void seek(long position) throws IOException {
      if (position < 0) throw new IOException("negative seek");
      BrewFsNative.seek(file, position);
      this.position = position;
    }

    @Override
    public long getPos() { return position; }

    @Override
    public boolean seekToNewSource(long targetPosition) { return false; }

    @Override
    public void close() throws IOException {
      if (!closed) {
        closed = true;
        BrewFsNative.closeFile(file);
        file = null;
      }
    }

    private static void check(byte[] buffer, int offset, int length) throws IOException {
      if (buffer == null || offset < 0 || length < 0 || offset > buffer.length - length)
        throw new IndexOutOfBoundsException();
    }
  }

  private static final class NativeOutputStream extends OutputStream
      implements org.apache.hadoop.fs.Syncable {
    private Pointer file;
    private long position;
    private boolean closed;

    NativeOutputStream(Pointer file) { this(file, 0); }

    NativeOutputStream(Pointer file, long position) {
      this.file = file;
      this.position = position;
    }

    @Override
    public void write(int value) throws IOException { write(new byte[] {(byte) value}, 0, 1); }

    @Override
    public void write(byte[] buffer, int offset, int length) throws IOException {
      if (buffer == null || offset < 0 || length < 0 || offset > buffer.length - length)
        throw new IndexOutOfBoundsException();
      // Respect the native max_io limit by splitting large buffers.
      byte[] chunk = new byte[Math.min(length, BrewFsNative.MAX_IO)];
      int sent = 0;
      while (sent < length) {
        int want = Math.min(length - sent, chunk.length);
        System.arraycopy(buffer, offset + sent, chunk, 0, want);
        int count = BrewFsNative.write(file, chunk, want, position, true);
        if (count <= 0) throw new IOException("native write made no progress");
        position += count;
        sent += count;
      }
    }

    @Override
    public void flush() throws IOException { BrewFsNative.flush(file); }
    @Override
    public void hflush() throws IOException { BrewFsNative.hflush(file); }
    @Override
    public void hsync() throws IOException { BrewFsNative.hsync(file); }

    @Override
    public void close() throws IOException {
      if (!closed) {
        closed = true;
        IOException failure = null;
        try { BrewFsNative.hsync(file); } catch (IOException e) { failure = e; }
        try { BrewFsNative.closeFile(file); } catch (IOException e) { if (failure == null) failure = e; }
        file = null;
        if (failure != null) throw failure;
      }
    }
  }
}
