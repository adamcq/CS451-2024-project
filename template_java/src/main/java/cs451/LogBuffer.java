package cs451;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LogBuffer {
    private final List<String> buffer;
    private final int bufferSize;
    private final String filePath;

    public LogBuffer(int bufferSize, String filePath) throws IOException {
        this.bufferSize = bufferSize;
        this.filePath = filePath;
        this.buffer = new ArrayList<>(bufferSize);
        new FileWriter(filePath, false).close();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Inside Add Shutdown Hook");
            close();
        }));
    }

    public synchronized void log(String message) {
        buffer.add(message);
        if (buffer.size() >= bufferSize) {
            flush();
        }
    }

    public synchronized void flush() {
//        System.out.println("Flushing " + buffer.toString());
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
