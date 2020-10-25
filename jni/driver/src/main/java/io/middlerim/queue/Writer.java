package io.middlerim.queue;

import java.nio.ByteBuffer;

public class Writer implements AutoCloseable {
  private static native long init(String configPath);
  private static native void close(long ptr);
  private static native long add(long writer, ByteBuffer message, long length);

  static {
    System.loadLibrary("middlerimq");
  }

  private final long ptr;

  public Writer(String configPath) {
    ptr = init(configPath);
  }

  @Override
  public void close() {
    close(ptr);
  }

  public long add(ByteBuffer message, long length) {
    return add(ptr, message, length);
  }
}
