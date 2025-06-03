package io.middlerim.queue;

import org.junit.Test;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class SharedMemoryQueueTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    static {
        try {
            // Attempt to load the library from the standard library path.
            // This assumes the JNI library (e.g., libmiddlerimq.so) is placed in a directory
            // listed in java.library.path.
            // For Gradle builds, this is often handled by the build process copying the .so
            // into a location like build/libs/middlerim-queue/shared/... or similar.
            // The exact path might need configuration in build.gradle if not default.
            System.loadLibrary("middlerimq");
        } catch (UnsatisfiedLinkError e) {
            System.err.println("Native code library failed to load. Ensure libmiddlerimq.so (or .dylib/.dll) is in java.library.path");
            System.err.println("java.library.path: " + System.getProperty("java.library.path"));
            throw e;
        }
    }

    @Test
    public void testSendAndReceiveMessage() throws IOException {
        File dataDir = tempFolder.newFolder("shmem_data");
        File configFile = tempFolder.newFile("test_config.toml");
        String shmemFileName = "test_queue_java.ipc";

        String configContent = String.format(
                "[shmem]\n" + // Added [shmem] table
                "data_dir = \"%s\"\n" +
                "shmem_file_name = \"%s\"\n" +
                "max_rows = 10\n" +
                "max_row_size = 1024\n" +
                "max_slots = 5\n" +
                "max_slot_size = 65536\n",
                dataDir.getAbsolutePath().replace("\\", "\\\\"), // Escape backslashes for TOML string
                shmemFileName
        );
        Files.write(configFile.toPath(), configContent.getBytes(StandardCharsets.UTF_8));

        Writer writer = null;
        Reader reader = null;
        File shmemFile = new File(dataDir, shmemFileName);


        try {
            // Use public constructors which call init internally
            writer = new Writer(configFile.getAbsolutePath());
            reader = new Reader(configFile.getAbsolutePath());

            // Add asserts to check if the internal pointers are non-zero if possible,
            // though this would require exposing the ptr or a isValid() method.
            // For now, assume constructor success means init was okay. Subsequent calls will fail if ptr is 0.

            String messageToSend = "Hello JNI from Java!";
            byte[] messageBytes = messageToSend.getBytes(StandardCharsets.UTF_8);
            ByteBuffer sendBuffer = ByteBuffer.allocateDirect(messageBytes.length);
            sendBuffer.put(messageBytes);
            sendBuffer.flip(); // Prepare for reading from buffer by JNI

            System.out.println("Attempting to send message: '" + messageToSend + "'");
            long rowIndex = writer.add(sendBuffer, sendBuffer.limit());
            Assert.assertTrue("Writer add failed (returned negative)", rowIndex >= 0);
            System.out.println("Message sent to rowIndex: " + rowIndex);

            // Prepare buffer for reading
            // Max row size is 1024 as per config
            ByteBuffer readBuffer = ByteBuffer.allocateDirect(1024);

            System.out.println("Attempting to read message from rowIndex: " + rowIndex);
            // The JNI read method sets the position of the buffer.
            // It doesn't return length directly.
            reader.read(rowIndex, readBuffer);

            int messageLength = readBuffer.position(); // Position is set by JNI's read
            Assert.assertTrue("Read message length should be positive", messageLength > 0);

            byte[] receivedBytes = new byte[messageLength];
            readBuffer.flip(); // Prepare for reading from Java side
            readBuffer.get(receivedBytes);
            String receivedMessage = new String(receivedBytes, StandardCharsets.UTF_8);

            System.out.println("Received message: '" + receivedMessage + "'");
            Assert.assertEquals("Sent and received messages do not match", messageToSend, receivedMessage);
            System.out.println("Message matched!");

        } finally {
            if (writer != null) {
                System.out.println("Closing writer...");
                writer.close();
            }
            if (reader != null) {
                System.out.println("Closing reader...");
                reader.close();
            }
            // TemporaryFolder rule will handle deletion of dataDir and configFile
            // but shmem file might need explicit cleanup if not in tempFolder auto-cleanup path
            // However, dataDir IS in tempFolder, so shmemFile should be cleaned up too.
            System.out.println("Test finished. Shared memory file was at: " + shmemFile.getAbsolutePath());
             if (shmemFile.exists()) {
                 System.out.println("Note: Shared memory file still exists post-test. This might be okay if OS cleans it or if it's expected.");
             }
        }
    }
}
