package cs451;

import cs451.Parsers.Parser;

import java.io.*;
import java.net.InetAddress;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;

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

    private static void initSignalHandlers(LogBuffer logBuffer) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                handleSignal();
                logBuffer.close();
            }
        });
    }

    private static Integer[] getPerfectLinkConfigInfo(String cfgPath) {
        Integer[] configInfo = {null, null};

        try(BufferedReader br = new BufferedReader(new FileReader(cfgPath))) {
            int lineNum = 1;
            for(String line; (line = br.readLine()) != null; lineNum++) {
                if (line.isBlank()) {
                    continue;
                }

                String[] splits = line.split(SPACES_REGEX);
                if (splits.length != 2) {
                    System.err.println("Problem with the line " + lineNum + " in the config file!");
                    return configInfo;
                }

                configInfo[0] = Integer.parseInt(splits[0]);
                configInfo[1] = Integer.parseInt(splits[1]);
            }
        } catch (IOException e) {
            System.err.println("Problem with the config file!");
            return configInfo;
        }

        return configInfo;
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
        // Create a HashMap from Integer ID to a pair (IP address, Port)
        HashMap<Integer, SimpleEntry<InetAddress, Integer>> idToAddressPort = new HashMap<>();

        for (Host host : parser.hosts()) {
            idToAddressPort.put(host.getId(), new SimpleEntry<>(InetAddress.getByName(host.getIp()), host.getPort()));
        }

        // init log buffer
        LogBuffer logBuffer;
        try {
            logBuffer = new LogBuffer(parser.output());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        initSignalHandlers(logBuffer);

        BEB bestEffortBroadcast = new BEB(idToAddressPort, parser.myId(), logBuffer, numberOfMessages);
//        new Thread(bestEffortBroadcast::receive).start();
//        new Thread(bestEffortBroadcast::broadcast).start();

//        link.shutdown(); // only when using threads in the link
//        System.exit(0);

        // After a process finishes broadcasting,
        // it waits forever for the delivery of messages.
        while (true) {
            // Sleep for 1 hour
            Thread.sleep(60 * 60 * 1000);
        }
    }

//    private static void perfectLinkMain(String[] args) throws Exception{
//        Parser parser = new Parser(args);
//        parser.parse();
//
//        initSignalHandlers();
//
//        // read config
//        String cfgPath = parser.config();
//        Integer[] configInfo = getPerfectLinkConfigInfo(cfgPath);
//        Integer numberOfMessages = configInfo[0];
//        Integer receiverId = configInfo[1];
//
//        if (receiverId == null || numberOfMessages == null) {
//            System.err.println("Config file parsed incorrectly.");
//            System.exit(1);
//        }
//
//        if (receiverId < 0 || receiverId > parser.hosts().size()) {
//            System.err.println("The receiving process defined in config is out of range");
//            System.exit(1);
//        }
//
//        /* sender & receiver logic */
//        // Create a HashMap from Integer ID to a pair (IP address, Port)
//        HashMap<Integer, SimpleEntry<InetAddress, Integer>> idToAddressPort = new HashMap<>();
//
//        for (Host host : parser.hosts()) {
//            idToAddressPort.put(host.getId(), new SimpleEntry<>(InetAddress.getByName(host.getIp()), host.getPort()));
//        }
//
//        System.out.println("Shut Down Hook Attached.");
//
//        PerfectLink link = new PerfectLink(idToAddressPort, receiverId, parser.myId(), parser.output(), numberOfMessages);
//        if (parser.myId() == receiverId) {
//            link.perfectReceiver.receive();
//        } else {
//            link.perfectSender.sendMessages(receiverId);
//        }
//
////        link.shutdown(); // only when using threads in the link
//        System.exit(0);
//
//        // After a process finishes broadcasting,
//        // it waits forever for the delivery of messages.
//        while (true) {
//            // Sleep for 1 hour
//            Thread.sleep(60 * 60 * 1000);
//        }
//    }
}
