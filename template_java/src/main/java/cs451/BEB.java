package cs451;

import cs451.Parsers.Parser;

import java.io.File;
import java.io.IOException;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BEB {
    TestLink testLink;
    int sentMessages;
    int sentAcks;
    int sentTotal;
    int receivedAcks;
    int receivedMessages;
    int receivedTotal;
    BebState bebState;
    int broadcastTotal;
    public BEB(TestLink testLink, BebState bebState) throws SocketException {
        this.testLink = testLink;
        this.bebState = bebState;

//        testLink.runConfig.getSocket().setSoTimeout(100);
    }

    public void bebBroadcast() {
        while (true) {
            int updateSelfUntil = -1;
            for (int dstId = 1; dstId <= testLink.numberOfHosts; dstId++) {
                if (dstId == testLink.hostId)
                    continue;
                int counter = 0;
                int number = bebState.getAcked()[dstId - 1].nextClearBit(bebState.getStartSendFrom()[dstId - 1]);
                while (counter < bebState.getSendWindowSize()[dstId - 1]) {
                    number = bebState.getAcked()[dstId - 1].nextClearBit(number);
                    if (number > testLink.runConfig.getNumberOfMessages()) {
                        break;
                    }
                    sentMessages++;
//                System.out.println("send " + number);
                    broadcastTotal++;
                    testLink.send(number, dstId);
//                    while (!testLink.send(number, dstId));
//                        System.out.println("dstId " + dstId + " counter " + counter);;
                    number++;
                    counter++;
                }
//                try {
//                    Thread.sleep(0,5000);
//                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
//                }
                updateSelfUntil = Math.max(updateSelfUntil, number);
            }
            if (updateSelfUntil >= bebState.getAcked()[testLink.hostId - 1].nextClearBit(bebState.getStartSendFrom()[testLink.hostId - 1])) {
                bebState.getAcked()[testLink.hostId - 1].set(bebState.getStartSendFrom()[testLink.hostId - 1], updateSelfUntil);
                bebState.getStartSendFrom()[testLink.hostId - 1] = updateSelfUntil;
            }
        }
    }

    public void processTasks() {
        System.out.println("HELLOOOOO GOGOMANTV");
        testLink.processTasks();
    }

    public void receive() {
        testLink.receive();
    }

    public static void main(String[] args) throws Exception {
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

//        ExecutorService receiverExecutor = Executors.newFixedThreadPool(4);

        Thread receiveThread = new Thread(() -> beb.receive(), "receiveThread");
        Thread broadcastThread = new Thread(() -> beb.bebBroadcast(), "broadcastThread");
        Thread processTasksThread = new Thread(() -> beb.processTasks(), "processTasksThread");

//        receiverExecutor.submit(() -> beb.receive());
        System.out.println("GOGOMAN tC SINF");
        processTasksThread.start();
        receiveThread.start();
        broadcastThread.start();
    }
}
