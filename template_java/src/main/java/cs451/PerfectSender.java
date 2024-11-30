package cs451;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;

public class PerfectSender {
    int numberOfBatches;
    int MIN_WINDOW_SIZE = 1;
    private final int AWAIT_CUT_OFF_THRESHOLD = 1;
    private int toAdd = 1;
    private final int BATCH_SIZE = 8;
    private final int INCREMENT = 1;
    private final int numberOfMessages;
    int MAX_ACK_WAIT_TIME = 200;
    private LogBuffer logBuffer;
    private final HashMap<Integer, AbstractMap.SimpleEntry<InetAddress, Integer>> idToAddressPort;
    int senderId;
    DatagramSocket socket;
    enum Phase {SLOW_START, CONGESTION_AVOIDANCE}

    public PerfectSender(int senderId,
                         int numberOfMessages,
                         HashMap<Integer, AbstractMap.SimpleEntry<InetAddress, Integer>> idToAddressPort,
                         String outputPath) {
        this.numberOfMessages = numberOfMessages;
        this.idToAddressPort = idToAddressPort;
        this.senderId = senderId;

        // init log buffer
        try {
            this.logBuffer = new LogBuffer(outputPath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // init socket
        InetAddress senderAddress = idToAddressPort.get(senderId).getKey();
        int senderPort = idToAddressPort.get(senderId).getValue();

        try {
            socket = new DatagramSocket(senderPort, senderAddress); // this works for both sender and receiver, because we put senderId == receiverId for receiver in Main
        } catch (SocketException e) {
            System.err.println("Socket is USED!!!");
//            throw new RuntimeException(e);
        }

        // add socket shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Inside Socket Shutdown Hook");
            if (socket != null && !socket.isClosed())
                socket.close();
        }));
    }

    private int loadBatches(Deque<Integer> batches, int windowSize) {
        for (int i = batches.size(); i < windowSize; i++) {
            if (toAdd > numberOfBatches)
                return batches.size(); // end reached
            batches.add(toAdd++);
        }
        return batches.size();
    }


    public void sendMessages(int receiverId) {
        InetAddress receiverAddress = idToAddressPort.get(receiverId).getKey();
        int receiverPort = idToAddressPort.get(receiverId).getValue();

        int ackedCount = 0;
        Deque<Integer> batches = new ArrayDeque<>();
        Phase phase = Phase.SLOW_START;
        int windowSize = MIN_WINDOW_SIZE;

        int numberOfBatches = numberOfMessages / BATCH_SIZE;
        if (numberOfMessages % BATCH_SIZE != 0)
            numberOfBatches++;

        this.numberOfBatches = numberOfBatches;

        while (ackedCount < numberOfBatches) {
            // concurrent logic
            // have a queue batches
            // awaitAcks directly removes from the queue messages Acked
            // loadBatches loads the queue with up to windowSize elements from acked, where acked is clear
            // send the batches
            // await acks for X miliseconds
            // remove received acks from the queue one by one

            int batchesBefore = batches.size();
            windowSize = loadBatches(batches, windowSize); // windowSize can be shrunken in this method if it is the last batch
            int toLog = windowSize - batchesBefore;

            generateAndSendBatches(batches, toLog, receiverAddress, receiverPort); // toLog used for logging
            long rtt = awaitAcks(batches);

            // update rtt
            try {
                socket.setSoTimeout(Math.min(MAX_ACK_WAIT_TIME, (int)rtt)); // TODO the min() was added after M1 submission
            } catch (SocketException e) {
                throw new RuntimeException(e);
            }

            int acksReceived = windowSize - batches.size();
            ackedCount += acksReceived;

            // LOGIC FOR WINDOW SIZE MANAGEMENT IN PERFECT NETWORK
            if (ackedCount == 0) // initially wait
                continue;
            else if (batches.size() > AWAIT_CUT_OFF_THRESHOLD) {
                windowSize /= 2;
                windowSize = Math.max(MIN_WINDOW_SIZE, windowSize);
                phase = Phase.CONGESTION_AVOIDANCE;
            } else {
                if (phase.equals(Phase.SLOW_START))
                    windowSize *= 2;
                else
                    windowSize += INCREMENT;
            }
        }

        // after all was sent
        System.out.println("All " + numberOfMessages + " messages were sent by delivering " + ackedCount + " batches.");
//        if (socket != null && !socket.isClosed()) {
//            socket.close();
//        }
    }

    public void generateAndSendBatches(Deque<Integer> batches, int toLog, InetAddress receiverAddress, int receiverPort) {
        int remaining = batches.size();
        for (int batchNumber : batches) {
            int currentBatchSize = Math.min(BATCH_SIZE, numberOfMessages - (batchNumber - 1) * BATCH_SIZE);

            // Create and fill the batch array
            int[] batch = new int[currentBatchSize];
            for (int j = 0; j < currentBatchSize; j++) {
                batch[j] = (batchNumber - 1) * BATCH_SIZE + j + 1;
            }

            sendBatch(batchNumber, batch, remaining <= toLog, receiverAddress, receiverPort);
            remaining--;
        }
    }

    public void sendBatch(int batchNumber, int[] batch, boolean logBatch, InetAddress receiverAddress, int receiverPort) {

        // Convert senderId and messageNumber to a space-separated string format
        StringBuilder payload = new StringBuilder();

        // append message numbers to the batch
        for (int messageNumber : batch) {
            payload.append(messageNumber);
            payload.append(" ");
        }

        // Remove the last space
        if (payload.length() > 0) {
            payload.setLength(payload.length() - 1);
        }

        // payload e.g., 1,32,"42 43 44 45 46 47 48 49 50",
        byte[] payloadBytes = payload.toString().getBytes(StandardCharsets.UTF_8);

        // Get the current system time in milliseconds
        long millis = System.currentTimeMillis();

        // Create a ByteBuffer
        ByteBuffer buffer = ByteBuffer.allocate(8 + payloadBytes.length + 8); // integer, integer, string payload, long time

        // Place the payload bytes into the buffer
        buffer.putInt(senderId);
        buffer.putInt(batchNumber);
        buffer.put(payloadBytes);
        buffer.putLong(millis);

        // Create a packet to send data to the receiver's address
        byte[] sendData = buffer.array();
        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, receiverAddress, receiverPort);

        // send the packet
        try {
            assert socket != null;
            socket.send(sendPacket);
            if (logBatch) {
                for (int messageToSend : batch) {
                    // log the broadcast
                    logBuffer.log("b " + messageToSend);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Failed to send batch number " + batchNumber + ": " + e.getMessage());
            if (socket != null && !socket.isClosed()) {
//                socket.close();
            }
        }
    }

    public long awaitAcks(Deque<Integer> batches) {
        // Prepare a packet to receive ACK data
        byte[] ackData = new byte[16]; // senderId and batchNumber
        DatagramPacket ackPacket = new DatagramPacket(ackData, ackData.length);
        long rtt = 1;

        while (true) {
            try {
                if (batches.size() == 0)
                    return rtt;

                assert socket != null;
                socket.receive(ackPacket); // this is blocking until received

                // the below executes only if ACK is received before timeout
                byte[] data = ackPacket.getData();
                int length = ackPacket.getLength();

                // Ensure the length is at least 8 to read two integers
                if (length < 16) {
                    throw new IllegalArgumentException("Packet is too short to contain two integers and a long.");
                }
                int ackSenderId = ((data[0] & 0xFF) << 24) | ((data[1] & 0xFF) << 16) | ((data[2] & 0xFF) << 8) | (data[3] & 0xFF);
                int ackBatchNumber = ((data[4] & 0xFF) << 24) | ((data[5] & 0xFF) << 16) | ((data[6] & 0xFF) << 8) | (data[7] & 0xFF);
                long sendTime =
                        ((long) (data[length - 8] & 0xFF) << 56) |
                                ((long) (data[length - 7] & 0xFF) << 48) |
                                ((long) (data[length - 6] & 0xFF) << 40) |
                                ((long) (data[length - 5] & 0xFF) << 32) |
                                ((long) (data[length - 4] & 0xFF) << 24) |
                                ((long) (data[length - 3] & 0xFF) << 16) |
                                ((long) (data[length - 2] & 0xFF) << 8) |
                                ((long) (data[length - 1] & 0xFF));
                rtt = Math.max(rtt, System.currentTimeMillis() - sendTime);
//                System.out.println("rtt " + rtt);

                // remove ackBatchNumber from the batches queue
                if (ackSenderId == senderId) { // should be always true TODO remove this if
                    batches.remove(ackBatchNumber);
                }
                else
                    System.out.println("ERROR ACK meant for sender " + ackSenderId + " arrived to sender " + senderId);

            } catch (java.net.SocketTimeoutException e) {
                // Timeout occurred, stop processing received
                break;
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println("Error while waiting for ACKs: " + e.getMessage());
                if (socket != null && !socket.isClosed()) {
//                    socket.close();
                }
            }
        }
        return rtt;
    }
}
