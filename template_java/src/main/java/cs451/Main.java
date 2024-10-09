package cs451;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.Random;
import java.util.Timer;

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

    private static void initSignalHandlers() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                handleSignal();
            }
        });
    }

    public static void main(String[] args) throws Exception {
        Parser parser = new Parser(args);
        parser.parse();

        initSignalHandlers();

        // read config
        String cfgPath = parser.config();
        Integer[] configInfo = getConfigInfo(cfgPath);
        Integer numberOfMessages = configInfo[0];
        Integer receiverId = configInfo[1];

        if (receiverId == null || numberOfMessages == null) {
            System.err.println("Config file parsed incorrectly.");
            System.exit(1);
        }

        if (receiverId < 0 || receiverId > parser.hosts().size()) {
            System.err.println("The receiving process defined in config is out of range");
            System.exit(1);
        }

        /* sender & receiver logic */
        // Create a HashMap from Integer ID to a pair (IP address, Port)
        HashMap<Integer, SimpleEntry<InetAddress, Integer>> idToAddressPort = new HashMap<>();

        for (Host host : parser.hosts()) {
            idToAddressPort.put(host.getId(), new SimpleEntry<>(InetAddress.getByName(host.getIp()), host.getPort()));
        }

        PerfectLink link = new PerfectLink(idToAddressPort, receiverId, parser.myId());
        if (parser.myId() == receiverId) {
            link.receive();
        } else {
            link.sendMessages(numberOfMessages);
        }

        link.shutdown();

        // After a process finishes broadcasting,
        // it waits forever for the delivery of messages.
        while (true) {
            // Sleep for 1 hour
            Thread.sleep(60 * 60 * 1000);
        }
    }

    public static void main1(String[] args) throws Exception {
        Parser parser = new Parser(args);
        parser.parse();

        initSignalHandlers();

        // read config
        String cfgPath = parser.config();
        Integer[] configInfo = getConfigInfo(cfgPath);
        Integer numberOfMessages = configInfo[0];
        Integer receiverId = configInfo[1];

        if (receiverId == null || numberOfMessages == null) {
            System.err.println("Config file parsed incorrectly.");
            System.exit(1);
        }

        if (receiverId < 0 || receiverId > parser.hosts().size()) {
            System.err.println("The receiving process defined in config is out of range");
            System.exit(1);
        }

        // sender & receiver logic

        // initialize sender & receivers
        // Create a HashMap from Integer ID to a pair (IP address, Port)
        HashMap<Integer, SimpleEntry<InetAddress, Integer>> idToAddressPort = new HashMap<>();

        for (Host host : parser.hosts()) {
            idToAddressPort.put(host.getId(), new SimpleEntry<>(InetAddress.getByName(host.getIp()), host.getPort()));
        }

        SimpleEntry<InetAddress, Integer> addressPort= idToAddressPort.get(parser.myId());
        InetAddress ip = addressPort.getKey();
        Integer port = addressPort.getValue();

        SimpleEntry<InetAddress, Integer> receiverAddressPort= idToAddressPort.get(receiverId);
        InetAddress receiverIp = receiverAddressPort.getKey();
        Integer receiverPort = receiverAddressPort.getValue();


        PerfectLinkV1 perfectLink = new PerfectLinkV1();

        // receiver
        if (parser.myId() == receiverId) {
            PerfectReceiverV1 receiver = new PerfectReceiverV1(ip, perfectLink, receiverPort);

            // Starting receiver in a new thread
            new Thread(() -> {
                try {
                    receiver.receiveMessage();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
        }

        // sender

        StubbornSenderV1 sender = new StubbornSenderV1(parser.myId(), perfectLink, receiverIp, receiverPort);

        try {
            if (parser.myId() != receiverId) {
                int i = 0;
                while (true) {
                    if (i == numberOfMessages)
                        break;
                    if (sender.sendMessage(i+1))
                        i++;
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // After a process finishes broadcasting,
        // it waits forever for the delivery of messages.
        while (true) {
            // Sleep for 1 hour
            Thread.sleep(60 * 60 * 1000);
        }
    }

    public static void main2(String[] args) throws InterruptedException {
        Parser parser = new Parser(args);
        parser.parse();

        initSignalHandlers();

        // example
        long pid = ProcessHandle.current().pid();
//        System.out.println("My PID: " + pid + "\n");
//        System.out.println("From a new terminal type `kill -SIGINT " + pid + "` or `kill -SIGTERM " + pid + "` to stop processing packets\n");
//
//        System.out.println("My ID: " + parser.myId() + "\n");
//        System.out.println("List of resolved hosts is:");
//        System.out.println("==========================");
//        for (Host host: parser.hosts()) {
//            System.out.println(host.getId());
//            System.out.println("Human-readable IP: " + host.getIp());
//            System.out.println("Human-readable Port: " + host.getPort());
//            System.out.println();
//        }
//        System.out.println();
//
//        System.out.println("Path to output:");
//        System.out.println("===============");
//        System.out.println(parser.output() + "\n");
//
//        System.out.println("Path to config:");
//        System.out.println("===============");
//        System.out.println(parser.config() + "\n");
//
//        System.out.println("Doing some initialization\n");

        // read config
        String cfgPath = parser.config();
        Integer[] configInfo = getConfigInfo(cfgPath);
        Integer numberOfMessages = configInfo[0];
        Integer receiverId = configInfo[1];

        if (receiverId == null || numberOfMessages == null) {
            System.err.println("Config file parsed incorrectly.");
            System.exit(1);
        }

        if (receiverId < 0 || receiverId > parser.hosts().size()) {
            System.err.println("The receiving process defined in config is out of range");
            System.exit(1);
        }

//        System.out.println("receiverId = " + receiverId + " and nofMessages = " + numberOfMessages);

        // sender & receiver logic

        // Create a HashMap from Integer ID to a pair (IP address, Port)
        HashMap<Integer, SimpleEntry<String, Integer>> idToAddressPort = new HashMap<>();

        for (Host host : parser.hosts()) {
            idToAddressPort.put(host.getId(), new SimpleEntry<>(host.getIp(), host.getPort()));
        }

        SimpleEntry<String, Integer> addressPort= idToAddressPort.get(parser.myId());
        String ip = addressPort.getKey();
        Integer port = addressPort.getValue();

        SimpleEntry<String, Integer> receiverAddressPort= idToAddressPort.get(receiverId);
        String receiverIp = receiverAddressPort.getKey();
        Integer receiverPort = receiverAddressPort.getValue();

        if (parser.myId() == receiverId) {
            PerfectReceiver receiver = new PerfectReceiver(ip, port, idToAddressPort);

            // Starting receiver in a new thread
            Thread receiverThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    receiver.receiveData();
                }
            });
            receiverThread.start();
//            receiver.receiveData();
        }

//        System.out.println("Human-readable IP: " + ip);
//        System.out.println("Human-readable Port: " + port);
//        System.out.println();
//
//        System.out.println("Broadcasting and delivering messages...\n");
        // test sender
        if (parser.myId() != receiverId) {
            StubbornSender sender = new StubbornSender(receiverIp, receiverPort, ip, port);
            Thread.sleep(100); // Give receiver time to start
            for (int i = 0; i < numberOfMessages; i++) {
                sender.sendData(parser.myId(), i+1);
            }
        }

        // After a process finishes broadcasting,
        // it waits forever for the delivery of messages.
        while (true) {
            // Sleep for 1 hour
            Thread.sleep(60 * 60 * 1000);
        }
    }

    private static Integer[] getConfigInfo(String cfgPath) {
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
}
