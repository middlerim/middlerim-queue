package io.middlerim.queue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

class Example {
  public static void main(String[] args) {

    var writer = new Writer("./middlerim-writer.toml");
    var reader = new Reader("./middlerim-reader.toml");
    var count = 0;

    var start = System.currentTimeMillis();

    var max = 10_000_000;
    for (var i = 0; i <= max; i++) {
      String message;
      if (i == max) {
        message = "01234567";
      } else {
        message = String.valueOf(i);
      }
      var bytes = message.getBytes(StandardCharsets.UTF_8);
      var buff = ByteBuffer.allocateDirect(bytes.length);
      buff.put(bytes);

      var rowIndex = writer.add(buff);
      var storedBytes = reader.read(rowIndex);
      var storedMessage = new String(storedBytes, StandardCharsets.UTF_8);
      if (!storedMessage.equals(message)) {
        System.out.println();
        System.out.println(i + ":" + storedMessage + ":" + message + ":" + storedBytes.length + ":" + Arrays.toString(storedBytes));
        throw new RuntimeException("invalid message");
      }
      if (i % 1_000_000 == 0) {
        System.out.print("\rRead message(index=" + rowIndex + "): " + message);
      }
      count++;
    }

    var finish = System.currentTimeMillis();
    var timeElapsed = (finish - start) / 1000;
    System.out.println("\n" + count / timeElapsed / 1000 + "K messages write&read/s. Total time: " + timeElapsed + "s");
  }
}
