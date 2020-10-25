package io.middlerim.queue;

import java.io.Closeable;
import java.nio.ByteBuffer;

public class Reader implements AutoCloseable {
  private static native long init(String configPath);
  private static native void close(long ptr);
  private static native void read(long ptr, long rowIndex, ByteBuffer buff);

  static {
    System.loadLibrary("middlerimq");
  }

  private final long ptr;

  public Reader(String configPath) {
    ptr = init(configPath);
  }

  @Override
  public void close() {
    close(ptr);
  }

  public void read(long rowIndex, ByteBuffer buff) {
    read(ptr, rowIndex, buff);
  }
}
