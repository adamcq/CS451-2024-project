package cs451;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class PerfectLink {
    private final HashMap<Integer, AbstractMap.SimpleEntry<InetAddress, Integer>> idToAddressPort;
    private final int senderId;
    InetAddress receiverAddress;
    int receiverPort;
    DeliveredCompressed delivered;
    int MAX_WINDOW_SIZE = 65536; // 2^16
    private final int LOG_BUFFER_SIZE = 10000;
    private DatagramSocket socket;
    private final boolean isReceiver;
    private final int MAX_ACK_WAIT_TIME = 20;
    private final int UDP_PACKET_SIZE = 1024;
    private final int BATCH_SIZE = 8;
    private final int INCREMENT = 1;
    LogBuffer logBuffer;
    int numberOfMessages;
    int numberOfBatches;
    int MIN_WINDOW_SIZE = 1;
    int AWAIT_CUT_OFF_THRESHOLD = 1;
    int toAdd = 1;
    enum Phase {SLOW_START, CONGESTION_AVOIDANCE}

    public PerfectLink(HashMap<Integer, AbstractMap.SimpleEntry<InetAddress, Integer>> idToAddressPort, int receiverId, int senderId, String outputPath, int numberOfMessages) throws Exception {
        this.idToAddressPort = idToAddressPort;
        this.senderId = senderId;
        receiverAddress = idToAddressPort.get(receiverId).getKey();
        receiverPort = idToAddressPort.get(receiverId).getValue();
        isReceiver = (senderId == receiverId);
        this.numberOfMessages = numberOfMessages;
        if (isReceiver) {
            delivered = new DeliveredCompressed(idToAddressPort.size(), MAX_WINDOW_SIZE, numberOfMessages);
        }
        logBuffer = new LogBuffer(LOG_BUFFER_SIZE, outputPath);
        initSocket();
    }

    private void initSocket() {
        InetAddress senderAddress = idToAddressPort.get(senderId).getKey();
        int senderPort = idToAddressPort.get(senderId).getValue();

        try {
            socket = new DatagramSocket(senderPort, senderAddress); // this works for both sender and receiver, because we put senderId == receiverId for receiver in Main

            if (!isReceiver) {
                socket.setSoTimeout(MAX_ACK_WAIT_TIME);
            }
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }
    }
    private int loadBatches(Deque<Integer> batches, int windowSize) {
        for (int i = batches.size(); i < windowSize; i++) {
            if (toAdd > numberOfBatches)
                return batches.size(); // end reached
            batches.add(toAdd++);
        }
        return batches.size();
    }


    public void sendMessages(int numberOfMessages) {
        this.numberOfMessages = numberOfMessages;
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

            generateAndSendBatches(batches, toLog); // toLog used for logging
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
        if (socket != null && !socket.isClosed()) {
            socket.close();
        }
    }

    public void generateAndSendBatches(Deque<Integer> batches, int toLog) {
        int remaining = batches.size();
        for (int batchNumber : batches) {
            int currentBatchSize = Math.min(BATCH_SIZE, numberOfMessages - (batchNumber - 1) * BATCH_SIZE);

            // Create and fill the batch array
            int[] batch = new int[currentBatchSize];
            for (int j = 0; j < currentBatchSize; j++) {
                batch[j] = (batchNumber - 1) * BATCH_SIZE + j + 1;
            }

            sendBatch(batchNumber, batch, remaining <= toLog);
            remaining--;
        }
    }

    public void sendBatch(int batchNumber, int[] batch, boolean logBatch) {

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
                socket.close();
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
                    socket.close();
                }
            }
        }
        return rtt;
    }

    public void handleData(byte[] data, int length) {
        // Ensure the length is at least 8 to read two integers
        if (length < 8) {
            throw new IllegalArgumentException("Packet is too short to contain two integers.");
        }

        // Extract the first two integers
        int senderId = ((data[0] & 0xFF) << 24) |
                ((data[1] & 0xFF) << 16) |
                ((data[2] & 0xFF) << 8) |
                (data[3] & 0xFF);
        int batchNumber = ((data[4] & 0xFF) << 24) |
                ((data[5] & 0xFF) << 16) |
                ((data[6] & 0xFF) << 8) |
                (data[7] & 0xFF);

        // Read the remaining data as a UTF-8 string
        String message = new String(data, 8, length - 16, StandardCharsets.UTF_8);

        // Extract the last 8 bytes as a long (nanoTime)
        long sendTime = ((long) (data[length - 8] & 0xFF) << 56) |
                ((long) (data[length - 7] & 0xFF) << 48) |
                ((long) (data[length - 6] & 0xFF) << 40) |
                ((long) (data[length - 5] & 0xFF) << 32) |
                ((long) (data[length - 4] & 0xFF) << 24) |
                ((long) (data[length - 3] & 0xFF) << 16) |
                ((long) (data[length - 2] & 0xFF) << 8) |
                ((long) (data[length - 1] & 0xFF));

        // Split the payload by spaces
        String[] parts = message.split("\\s+");

        // Parse the remaining integers as messageNumbers
        for (String part : parts) {
            int messageNumber = Integer.parseInt(part);  // Direct parsing without trim
            markDelivered(senderId, messageNumber);      // Process each number directly
        }

        sendACK(senderId, batchNumber, sendTime);
    }

    public void receive() {
        try {
            // Prepare a packet to receive data
            byte[] receiveData = new byte[UDP_PACKET_SIZE]; // TODO ask if we can assume 1024 Bytes as maximum size of a packet
            DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);

            while (true) {
                // Receive the packet
                socket.receive(receivePacket);
                handleData(receivePacket.getData(), receivePacket.getLength());
            }
        } catch (Exception e) {
            System.err.println("Error in receive loop: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (socket != null && !socket.isClosed()) {
                System.out.println("Closing socket...");
                socket.close();
            }
        }
    }

    private void sendACK(int senderId, int batchNumber, long sendTime) { // TODO sendACK(int senderId, int batchNumber) - should be enough to identify originality

        // get sender's IP and port
        InetAddress senderAddress = idToAddressPort.get(senderId).getKey();
        int senderPort = idToAddressPort.get(senderId).getValue();

        // prepare ackData
        ByteBuffer buffer = ByteBuffer.allocate(16);

        // Put the senderId and batchNumber into the ByteBuffer
        buffer.putInt(senderId);
        buffer.putInt(batchNumber);
        buffer.putLong(sendTime);

        // Get the byte array from the ByteBuffer
        byte[] ackData = buffer.array();

        // Create ACK packet to send data back to the sender's address
        DatagramPacket ackPacket = new DatagramPacket(ackData, ackData.length, senderAddress, senderPort);

        try {
            socket.send(ackPacket);
        } catch (IOException e) {
            System.err.println("Failed to send ACK for batchNumber=" + batchNumber + ": " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private void markDelivered(int senderId, int messageNumber) {
        if (!delivered.isDelivered(senderId, messageNumber)) {
            logBuffer.log("d " + senderId + " " + messageNumber);
            delivered.setDelivered(senderId, messageNumber);
        }
    }
}