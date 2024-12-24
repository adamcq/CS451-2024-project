package cs451;

import cs451.Parsers.Parser;

import java.io.*;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
    private static Integer[] getPerfectConfigInfo(String cfgPath) {
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

    private static Integer[] getLatticeConfigInfo(String cfgPath) {
        Integer[] configInfo = {null, null, null};

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

    public static void redirectOutputToFile(String fileName) {
        try {
            // Create a file for the process
            File file = new File(fileName);

            // Create a PrintStream that writes to the file
            PrintStream fileOut = new PrintStream(new FileOutputStream(file));

            // Redirect standard output and error streams
            System.setOut(fileOut);
            System.setErr(fileOut);

            System.out.println("Console output is redirected to " + fileName);
        } catch (IOException e) {
            System.err.println("Failed to redirect console output: " + e.getMessage());
        }
    }


    public static void main(String[] args) throws Exception {
//        perfectLinkMain(args);
//        BEBMain(args);
//        testLinkMain(args);
        latticeAgreementMain(args);
    }

    public static void latticeAgreementMain(String[] args) {
        Parser parser = new Parser(args);
        parser.parse();

        redirectOutputToFile("./lattice/output/stdout_proc" +parser.myId() + ".log");

        try (BufferedReader reader = new BufferedReader(new FileReader(parser.config()))) {
            // Read the first line to initialize LatticeRunConfig
            String firstLine = reader.readLine();
            if (firstLine == null) {
                throw new IllegalArgumentException("File is empty");
            }

            String[] configValues = firstLine.split("\\s+");
            if (configValues.length < 3) {
                throw new IllegalArgumentException("First line must contain at least three numbers");
            }

            int numberOfIterations = Integer.parseInt(configValues[0]);
            int maxProposalSize = Integer.parseInt(configValues[1]);
            int uniqueNumbersCount = Integer.parseInt(configValues[2]);

            LatticeRunConfig runConfig = new LatticeRunConfig(parser, numberOfIterations, maxProposalSize, uniqueNumbersCount);

            // Process remaining lines iteratively
            for (int i = 0; i < numberOfIterations; i++) {
                String line = reader.readLine();
                if (line == null) {
                    throw new IllegalArgumentException("File contains fewer lines than expected");
                }

                String[] numberStrings = line.split("\\s+");
                Set<Integer> initialProposal = new HashSet<>();
                for (String numberString : numberStrings) {
                    initialProposal.add(Integer.parseInt(numberString));
                }
                System.out.println("Processing line " + (i + 1) + ": " + line);

                AcceptorState acceptorState = new AcceptorState();
                ProposerState proposerState = new ProposerState(initialProposal, runConfig);
                LatticeLink latticeLink = new LatticeLink(runConfig, proposerState, acceptorState);
                ProposerBEB proposerBEB = new ProposerBEB(latticeLink, proposerState, runConfig);
                Proposer proposer = new Proposer(proposerBEB, runConfig);

                Thread receiveThread = new Thread(latticeLink::receive, "receiveThread");
                receiveThread.start();
                proposerState.setProposer(proposer); // adding this last reference triggers initial broadcast

                // TODO implement proposerBEB
                // TODO handle multishot - I likely need to include the iteration in the payload as well
                // TODO multishot logic - do I start broadcasting the next one right after delivery? Do I wait?
            }

        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    public static void perfectLinkMain(String[] args) throws Exception {
        Parser parser = new Parser(args);
        parser.parse();

//        initSignalHandlers();

        // read config
        String cfgPath = parser.config();
        Integer[] configInfo = getPerfectConfigInfo(cfgPath);
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
        HashMap<Integer, AbstractMap.SimpleEntry<InetAddress, Integer>> idToAddressPort = new HashMap<>();

        for (Host host : parser.hosts()) {
            idToAddressPort.put(host.getId(), new AbstractMap.SimpleEntry<>(InetAddress.getByName(host.getIp()), host.getPort()));
        }

        System.out.println("Shut Down Hook Attached.");

        RunConfig runConfig = new RunConfig(parser, numberOfMessages);
//        PerfectLink link = new PerfectLink(runConfig);
//        if (parser.myId() == receiverId) {
//            link.receive();
//        } else {
//            link.sendMessages(numberOfMessages);
//        }

//        link.shutdown(); // only when using threads in the link
        System.exit(0);

        // After a process finishes broadcasting,
        // it waits forever for the delivery of messages.
        while (true) {
            // Sleep for 1 hour
            Thread.sleep(60 * 60 * 1000);
        }
    }

    private static void testLinkMain(String[] args) throws Exception{
        // EXPECTED ARGS: numberOfHosts followed by numberOfHosts integers (port values) followed by hostId

        Parser parser = new Parser(args);
        parser.parse();

        String cfgPath = parser.config();
        Scanner scanner = new Scanner(new File(cfgPath));
        int numberOfMessages = scanner.nextInt();

        RunConfig runConfig = new RunConfig(parser, numberOfMessages);
        BebState bebState = new BebState(runConfig);
        TestLink testLink = new TestLink(runConfig, bebState);
        BEB beb = new BEB(testLink, bebState);

//        ExecutorService processTasksExecutor = Executors.newFixedThreadPool(2);

        Thread receiveThread = new Thread(() -> beb.receive(), "receiveThread");
        Thread broadcastThread = new Thread(() -> beb.bebBroadcast(), "broadcastThread");
//        Thread processTasksThread = new Thread(() -> beb.processTasks(), "processTasksThread");


//        receiverExecutor.submit(() -> beb.receive());
        System.out.println("GOGOMAN tC SINF");
//        processTasksThread.start();
//        processTasksExecutor.submit(() -> beb.processTasks());
        receiveThread.start();
        broadcastThread.start();

        // After a process finishes broadcasting,
        // it waits forever for the delivery of messages.
        while (true) {
            // Sleep for 1 hour
            Thread.sleep(60 * 60 * 1000);
        }
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
        FifoBroadcast bestEffortBroadcast = new FifoBroadcast(runConfig);
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
