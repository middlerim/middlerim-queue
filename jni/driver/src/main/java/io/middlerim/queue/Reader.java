package io.middlerim.queue;

public class Reader {
  private static native long init(String configPath);

  private static native byte[] read(long reader, long rowIndex);

  static {
    System.loadLibrary("middlerimq");
  }

  private final long ptr;

  public Reader(String configPath) {
    ptr = init(configPath);
  }

  public byte[] read(long rowIndex) {
    return read(ptr, rowIndex);
  }
}
