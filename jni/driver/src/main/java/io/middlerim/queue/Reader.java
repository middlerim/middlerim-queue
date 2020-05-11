package io.middlerim.queue;

import java.nio.ByteBuffer;

public class Reader {
  private static native long init(String configPath);

  private static native void read(long reader, long rowIndex, ByteBuffer buff);

  static {
    System.loadLibrary("middlerimq");
  }

  private final long ptr;

  public Reader(String configPath) {
    ptr = init(configPath);
  }

  public void read(long rowIndex, ByteBuffer buff) {
    read(ptr, rowIndex, buff);
  }
}
