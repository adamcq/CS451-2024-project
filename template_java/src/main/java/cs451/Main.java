package cs451;

import cs451.Parsers.Parser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.FileReader;
import java.net.DatagramSocket;

public class Main {
    private static final String SPACES_REGEX = "\\s+";
    private static final int MAX_PROCESSES = 128;
    private static final int MAX_MESSAGES = Integer.MAX_VALUE;

    private static void handleSignal() {
        //immediately stop network packet processing
        System.out.println("Immediately stopping network packet processing.");

        //write/flush output file if necessary
        System.out.println("Writing output.");
    }

    private static void initSignalHandlers(LogBuffer logBuffer, DatagramSocket socket, Thread[] threads) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                handleSignal();
                for (Thread thread : threads)
                    thread.interrupt();
                logBuffer.close();
                if (socket != null && !socket.isClosed())
                    socket.close();
            }
        });
    }

    private static Integer[] getFifoConfigInfo(String cfgPath) {
        Integer[] configInfo = {null};

        try(BufferedReader br = new BufferedReader(new FileReader(cfgPath))) {
            int lineNum = 1;
            for(String line; (line = br.readLine()) != null; lineNum++) {
                if (line.isBlank()) {
                    continue;
                }

                line = line.trim();
                configInfo[0] = Integer.parseInt(line);
            }
        } catch (IOException e) {
            System.err.println("Problem with the config file! " + e.getMessage());
            return configInfo;
        }

        return configInfo;
    }

    public static void main(String[] args) throws Exception {
//        perfectLinkMain(args);
        BEBMain(args);
    }

    private static void BEBMain(String[] args) throws Exception{
        Parser parser = new Parser(args);
        parser.parse();

        // read config
        String cfgPath = parser.config();
        Integer[] configInfo = getFifoConfigInfo(cfgPath);
        Integer numberOfMessages = configInfo[0];

        if (numberOfMessages == null) {
            System.err.println("Config file parsed incorrectly.");
            System.exit(1);
        }

        /* sender & receiver logic */
        RunConfig runConfig = new RunConfig(parser, numberOfMessages);

        // BEB
        BEB bestEffortBroadcast = new BEB(runConfig);
        Thread receiverThread = new Thread(bestEffortBroadcast::receive, "ReceiverThread");
//        Thread broadcastThread = new Thread(bestEffortBroadcast::broadcast, "BroadcastThread");

        Thread[] threads = new Thread[] {receiverThread};
        initSignalHandlers(runConfig.getLogBuffer(), runConfig.getSocket(), threads);

        receiverThread.start();
//        broadcastThread.start();
        bestEffortBroadcast.broadcast();

        // After a process finishes broadcasting,
        // it waits forever for the delivery of messages.
        while (true) {
            // Sleep for 1 hour
            Thread.sleep(60 * 60 * 1000);
        }
    }
}
