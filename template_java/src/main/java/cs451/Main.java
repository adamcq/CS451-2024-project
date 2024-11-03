package cs451;

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

        System.out.println("Shut Down Hook Attached.");

        PerfectLinkMultiThread link = new PerfectLinkMultiThread(idToAddressPort, receiverId, parser.myId(), parser.output(), numberOfMessages);
        if (parser.myId() == receiverId) {
            link.receive();
        } else {
            link.sendMessages(numberOfMessages);
        }

        link.shutdown(); // only when using threads in the link
        System.exit(0);

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
