package io.middlerim.queue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

class Example {
  public static void main(String[] args) {

    var writer = new Writer("../../middlerim-writer.toml");
    var reader = new Reader("../../middlerim-reader.toml");
    var count = 0;

    long start = System.currentTimeMillis();

    for (var i = 0; i < 10_000_000; i++) {
      var message = " 🐓 🏰 🥕 ";
      var bytes = message.getBytes(StandardCharsets.UTF_8);
      var buff = ByteBuffer.allocateDirect(bytes.length);
      buff.put(bytes);
      var rowIndex = writer.add(buff);
      var storedBytes = reader.read(rowIndex);
      var storedMessage = new String(storedBytes, StandardCharsets.UTF_8);
      if (i % 1_000_000 == 0) {
        System.out.print("\rRead message(index=" + rowIndex + "): " + storedMessage);
      }
      count++;
    }

    var finish = System.currentTimeMillis();
    var timeElapsed = (finish - start) / 1000;
    System.out.println("\n" + count / timeElapsed / 1000 + "K messages write&read/s. Total time: " + timeElapsed + "s");
  }
}
