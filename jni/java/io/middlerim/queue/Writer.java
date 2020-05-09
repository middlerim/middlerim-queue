package io.middlerim.queue;

import java.nio.ByteBuffer;

public class Writer {
  private static native long init(String configPath);

  private static native long add(long writer, ByteBuffer message);

  static {
    System.loadLibrary("middlerimq");
  }

  private final long writerPtr;

  public Writer(String configPath) {
    writerPtr = init(configPath);
  }

  public long add(ByteBuffer message) {
    return add(writerPtr, message);
  }
}
