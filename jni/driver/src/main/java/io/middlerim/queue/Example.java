package io.middlerim.queue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

class Example {
  public static void main(String[] args) {

    var start = System.currentTimeMillis();
    var count = 0;

    try (
      var writer = new Writer("./middlerim-writer.toml");
      var reader = new Reader("./middlerim-reader.toml")
    ) {

      var max = 10_000_000;

      var maxRowSize = 524_288;
      var writerBuff = ByteBuffer.allocateDirect(maxRowSize);
      var readerBuff = ByteBuffer.allocateDirect(maxRowSize);

      for (var i = 0; i <= max; i++) {
        long rowIndex;
        String message;

          // Write
        {
          if (i == max) {
            message = " 🐓 🏰 🥕 ";
          } else {
            message = String.valueOf(i);
          }
          var bytes = message.getBytes(StandardCharsets.UTF_8);
          writerBuff.position(0);
          writerBuff.put(bytes);

          rowIndex = writer.add(writerBuff, bytes.length);
        }
        // Read
        {
          readerBuff.position(0);
          reader.read(rowIndex, readerBuff);
          var readerBytes = new byte[readerBuff.position()];
          readerBuff.get(0, readerBytes, 0, readerBytes.length);
          var storedMessage = new String(readerBytes, StandardCharsets.UTF_8);
          if (!storedMessage.equals(message)) {
            throw new RuntimeException("invalid message: row_index=" + rowIndex + ", stored message=" + storedMessage + ", expected message=" + message);
          }
          if (i % 1_000_000 == 0) {
            System.out.print("\rRead message(index=" + rowIndex + "): " + message);
          }
        }

        count++;
      }
    }

    var finish = System.currentTimeMillis();
    var timeElapsed = (finish - start) / 1000;
    System.out.println("\n" + count / timeElapsed / 1000 + "K messages write&read/s. Total time: " + timeElapsed + "s");
  }
}
