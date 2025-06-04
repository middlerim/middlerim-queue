package io.middlerim.queue;

import org.junit.Test;
import org.junit.Assert;
// import org.junit.Rule; // Removed for manual temp dir
// import org.junit.rules.TemporaryFolder; // Removed for manual temp dir

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
// import java.nio.file.Paths; // Not used
import java.util.Comparator; // For manual cleanup

public class SharedMemoryQueueTest {

    // @Rule // Removed
    // public TemporaryFolder tempFolder = new TemporaryFolder(); // Removed

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
        Path tempDir = null;
        Writer writer = null;
        Reader reader = null;
        File shmemFileForLog = null; // For logging path at the end

        // Outer try for tempDir setup and cleanup
        try {
            tempDir = Files.createTempDirectory("jni_shmem_test_");
            System.out.println("[Java Test] Manually created tempDir: " + tempDir.toString());

            // Config file and shm file will be directly in tempDir's root.
            File configFile = tempDir.resolve("test_config.toml").toFile();
            String shmemFileNameOnly = "test_queue_java.ipc";

            shmemFileForLog = tempDir.resolve(shmemFileNameOnly).toFile(); // Place shm file in tempDir root
            String shmemAbsPath = shmemFileForLog.getAbsolutePath();
            System.out.println("[Java Test] Absolute shmem file path for TOML: " + shmemAbsPath);


            // TOML content for flink-based configuration
            String configContent = String.format(
                    "shmem_flink_path = \"%s\"\n" +
                    "use_flink_backing = true\n" +
                    "max_rows = 10\n" +
                    "max_row_size = 1024\n" +
                    "max_slots = 64\n" +
                    "max_slot_size = 65536\n",
                    shmemAbsPath.replace("\\", "\\\\")
            );
            Files.write(configFile.toPath(), configContent.getBytes(StandardCharsets.UTF_8));
            System.out.println("[Java Test] Config file created at: " + configFile.getAbsolutePath());
            System.out.println("[Java Test] TOML content for JNI:\n" + configContent);

            // Inner try for Writer/Reader usage and test assertions
            try {
                System.out.println("[Java Test] Initializing Writer with config: " + configFile.getAbsolutePath());
                writer = new Writer(configFile.getAbsolutePath());
                System.out.println("[Java Test] Initializing Reader with config: " + configFile.getAbsolutePath());
                reader = new Reader(configFile.getAbsolutePath());

                String messageToSend = "Hello JNI from Java!";
                byte[] messageBytes = messageToSend.getBytes(StandardCharsets.UTF_8);
                ByteBuffer sendBuffer = ByteBuffer.allocateDirect(messageBytes.length);
                sendBuffer.put(messageBytes);
                sendBuffer.flip();

                System.out.println("Attempting to send message: '" + messageToSend + "'");
                long rowIndex = writer.add(sendBuffer, sendBuffer.limit());
                Assert.assertTrue("Writer add failed (returned negative)", rowIndex >= 0);
                System.out.println("Message sent to rowIndex: " + rowIndex);

                ByteBuffer readBuffer = ByteBuffer.allocateDirect(1024);
                System.out.println("Attempting to read message from rowIndex: " + rowIndex);
                reader.read(rowIndex, readBuffer);

                int messageLength = readBuffer.position();
                Assert.assertTrue("Read message length should be positive", messageLength > 0);

                byte[] receivedBytes = new byte[messageLength];
                readBuffer.flip();
                readBuffer.get(receivedBytes);
                String receivedMessage = new String(receivedBytes, StandardCharsets.UTF_8);

                System.out.println("Received message: '" + receivedMessage + "'");
                Assert.assertEquals("Sent and received messages do not match", messageToSend, receivedMessage);
                System.out.println("Message matched!");

                System.out.println("[Java Test] Test finished. Shared memory file was at: " + (shmemFileForLog != null ? shmemFileForLog.getAbsolutePath() : "null"));
                if (shmemFileForLog != null && shmemFileForLog.exists()) {
                    System.out.println("[Java Test] Note: Shared memory file still exists post-test. This might be okay if OS cleans it or if it's expected before manual cleanup.");
                }

            } finally { // Inner finally (closes Writer and Reader)
                if (writer != null) {
                    System.out.println("[Java Test] Closing writer (inner finally)...");
                    writer.close();
                }
                if (reader != null) {
                    System.out.println("[Java Test] Closing reader (inner finally)...");
                    reader.close();
                }
            }
        } finally { // Outer finally (cleans up tempDir)
            if (tempDir != null) {
                System.out.println("[Java Test] Cleaning up manually created tempDir: " + tempDir.toString());
                try {
                    Files.walk(tempDir)
                        .sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(java.io.File::delete);
                     System.out.println("[Java Test] Manual tempDir cleanup successful for: " + tempDir.toString());
                } catch (IOException e) {
                    System.err.println("[Java Test] Failed to cleanup tempDir: " + tempDir.toString());
                    e.printStackTrace();
                }
            }
        }
    }
}
