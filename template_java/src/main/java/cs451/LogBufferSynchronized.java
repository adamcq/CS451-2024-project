package cs451;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class LogBufferSynchronized {
    private final List<String> buffer;
    private final int bufferSize;
    private final String filePath;
    private final BlockingQueue<String> logQueue;
    private final Thread loggingThread;
    private volatile boolean running = true;

    public LogBufferSynchronized(int bufferSize, String filePath) throws IOException {
        this.bufferSize = bufferSize;
        this.filePath = filePath;
        this.buffer = new ArrayList<>(bufferSize);
        this.logQueue = new LinkedBlockingQueue<>();
        new FileWriter(filePath, false).close();

        // Start the dedicated logging thread
        loggingThread = new Thread(() -> {
            while (running || !logQueue.isEmpty()) {
                try {
                    String message = logQueue.take(); // Block until a message is available
                    synchronized (this) {
                        log(message);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); // Restore the interrupt status
                }
            }
            close(); // Flush any remaining logs when the thread stops
        });
        loggingThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Inside Add Shutdown Hook");
            running = false;
            loggingThread.interrupt(); // Stop the logging thread
        }));
    }

    public void queueLog(String message) {
        logQueue.offer(message); // Add message to the queue
    }

    private synchronized void log(String message) {
        buffer.add(message);
        if (buffer.size() >= bufferSize) {
            flush();
        }
    }

    public synchronized void flush() {
        if (buffer.isEmpty()) {
            return;
        }
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true))) {
            for (String message : buffer) {
                writer.write(message);
                writer.newLine();
            }
            buffer.clear();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public synchronized void close() {
        flush(); // Write any remaining logs to the file
    }
}
