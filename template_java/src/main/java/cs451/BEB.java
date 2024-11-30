package cs451;

import javax.print.attribute.IntegerSyntax;
import javax.xml.crypto.Data;
import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

public class BEB {
    // BEB ARGS
    private int numberOfMessages;
    private HashMap<Integer, AbstractMap.SimpleEntry<InetAddress, Integer>> idToAddressPort;
    private final int processId;
    private String outputPath;
    private PerfectLink[] perfectLinks;
//    ExecutorService executor;

    // PERFECT SENDER ARGS

    int numberOfBatches;
    int MIN_WINDOW_SIZE = 1;
    private final int AWAIT_CUT_OFF_THRESHOLD = 1;
    private int[] toAdd;
    private final int BATCH_SIZE = 8;
    private final int INCREMENT = 1;
    int MAX_ACK_WAIT_TIME = 200;
    private LogBuffer logBuffer;
    DatagramSocket socket;
    DatagramSocket broadcastSocket;
    enum Phase {SLOW_START, CONGESTION_AVOIDANCE}
    ReentrantLock logMutex;

    // PERFECT RECEIVER ARGS
    private final DeliveredCompressed delivered;
    int MAX_WINDOW_SIZE = 65536; // 2^16
    private int UDP_PACKET_SIZE = 1024;

    public BEB(HashMap<Integer, AbstractMap.SimpleEntry<InetAddress, Integer>> idToAddressPort, int processId, String outputPath, int numberOfMessages) {
        this.numberOfMessages = numberOfMessages;
        this.outputPath = outputPath;
        this.processId = processId;
        this.idToAddressPort = idToAddressPort;
//        executor = Executors.newFixedThreadPool(2);

        this.delivered = new DeliveredCompressed(idToAddressPort.size(), MAX_WINDOW_SIZE, numberOfMessages);

        // init log buffer
        try {
            this.logBuffer = new LogBuffer(outputPath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // init socket
        InetAddress broadcasterAddress = idToAddressPort.get(processId).getKey();
        int broadcasterPort = idToAddressPort.get(processId).getValue();

        try {
            socket = new DatagramSocket(null); // this works for both sender and receiver, because we put senderId == receiverId for receiver in Main
            socket.setReuseAddress(true); // set before bind
            socket.bind(new InetSocketAddress(broadcasterAddress, broadcasterPort));
            System.out.println("INFOOOO " + socket.getLocalAddress() + " port " + socket.getLocalPort() + " initial timeout " + socket.getSoTimeout() + " SO_REUSEADDR = " + socket.getReuseAddress());
        } catch (SocketException e) {
            System.err.println("Creating receiver socket failed. Socket is USED!!!");
            System.err.println(e.getMessage());
            throw new RuntimeException(e);
        }

        try {
            broadcastSocket = new DatagramSocket(null);
            broadcastSocket.setReuseAddress(true); // set before bind
            broadcastSocket.bind(new InetSocketAddress(broadcasterAddress, broadcasterPort));
            broadcastSocket.setSoTimeout(MAX_ACK_WAIT_TIME); // TODO change to smaller value
        } catch (SocketException e) {
            System.err.println("Creating broadcast socket failed.");
            System.err.println(e.getMessage());
            throw new RuntimeException(e);
        }


        // add socket shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Inside Socket Shutdown Hook");
            if (socket != null && !socket.isClosed())
                socket.close();
            if (broadcastSocket != null && !broadcastSocket.isClosed())
                broadcastSocket.close();
        }));

        // log Mutex
        logMutex = new ReentrantLock();
    }

    public void broadcast() { // call this method from main on a separate thread
//        System.out.println("Is socket null?" + (socket==null));
        System.out.println("Broadcast called");
        toAdd = new int[idToAddressPort.size()];
        Arrays.fill(toAdd, 1);
        int[] ackedCount = new int[idToAddressPort.size()]; // TODO the logic with ackedCount is flawed - it can be 0 also during execution
        Deque<Integer>[] batches = new ArrayDeque[idToAddressPort.size()];
        for (int i = 0; i < idToAddressPort.size(); i++)
            batches[i] = new ArrayDeque<>();
        Phase[] phases = new Phase[idToAddressPort.size()];
        Arrays.fill(phases, Phase.SLOW_START);
        int[] windowSize = new int[idToAddressPort.size()];
        Arrays.fill(windowSize, MIN_WINDOW_SIZE);

        int numberOfBatches = numberOfMessages / BATCH_SIZE;
        if (numberOfMessages % BATCH_SIZE != 0)
            numberOfBatches++;

        this.numberOfBatches = numberOfBatches;

        while (true) {
            // batch broadcast
//            System.out.print("window sizes = ");
//            System.out.println("batches[receiverId-1] should be equal");
            for (int receiverId = 1; receiverId <= idToAddressPort.size(); receiverId++) {
                int batchesBefore = batches[receiverId - 1].size();
//                System.out.println(System.identityHashCode(batches[receiverId - 1]));
                windowSize[receiverId - 1] = loadBatches(batches[receiverId - 1], windowSize[receiverId - 1], receiverId); // windowSize can be shrunken in this method if it is the last batch
//                System.out.print(windowSize[receiverId - 1] + "-b" + batches[receiverId - 1].size() + "-bb" + batchesBefore);
                int toLog = windowSize[receiverId - 1] - batchesBefore;

                generateAndSendBatches(batches, toLog, receiverId); // toLog used for logging
            }
//            System.out.println();

            // await acks from all receivers
            // rtt is the max rtt of them all
//            System.out.println("RTT ");
//            System.out.println("batches before " + batches[0].size());
            long rtt = awaitAcks(batches);
//            System.out.println("batches after " + batches[0].size());
//            System.out.println("RTT after " + rtt + " phases[0] = " + phases[0]);

            // update rtt
            try {
                broadcastSocket.setSoTimeout(Math.min(MAX_ACK_WAIT_TIME, (int) rtt)); // TODO the min() was added after M1 submission
            } catch (SocketException e) {
                throw new RuntimeException(e);
            }

            for (int i = 0; i < idToAddressPort.size(); i++) {
                int acksReceived = 0;
                acksReceived += windowSize[i] - batches[i].size();
                ackedCount[i] += acksReceived;

                // LOGIC FOR WINDOW SIZE MANAGEMENT IN PERFECT NETWORK
                if (ackedCount[i] == 0) // initially wait
                    continue;
                else if (batches[i].size() > AWAIT_CUT_OFF_THRESHOLD) {
                    windowSize[i] /= 2;
                    windowSize[i] = Math.max(MIN_WINDOW_SIZE, windowSize[i]);
                    phases[i] = Phase.CONGESTION_AVOIDANCE;
                } else {
                    if (phases[i].equals(Phase.SLOW_START))
                        windowSize[i] *= 2;
                    else
                        windowSize[i] += INCREMENT;
                }
            }
        }
    }

    private int loadBatches(Deque<Integer> batches, int windowSize, int receiverId) {
//        System.out.println(System.identityHashCode(batches));
        for (int i = batches.size(); i < windowSize; i++) {
            if (toAdd[receiverId - 1] > numberOfBatches)
                return batches.size(); // end reached
            batches.add(toAdd[receiverId - 1]++);
        }
        return batches.size();
    }

    public void generateAndSendBatches(Deque<Integer>[] batches, int toLog, int receiverId) {
        // get receiver info
        InetAddress receiverAddress = idToAddressPort.get(receiverId).getKey();
        int receiverPort = idToAddressPort.get(receiverId).getValue();

        int remaining = batches[receiverId - 1].size();
//        System.out.println(System.identityHashCode(batches[receiverId - 1]));

        // batch sending logic
        for (int batchNumber : batches[receiverId - 1]) {
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

//        System.out.println("batch number " + batchNumber + " batchLength = " + batch.length);
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
        ByteBuffer buffer = ByteBuffer.allocate(1 + 8 + payloadBytes.length + 8); // boolean, integer, integer, string payload, long time

        // Place the payload bytes into the buffer
//        System.out.println("PLACING SENDER_ID " + processId + " INTO THE DATAGRAM PACKET");
        buffer.put((byte) 0); // 1 meaning it is NOT an ACK
        buffer.putInt(processId);
        buffer.putInt(batchNumber);
        buffer.put(payloadBytes);
        buffer.putLong(millis);

        // Create a packet to send data to the receiver's address
        byte[] sendData = buffer.array();
        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, receiverAddress, receiverPort);

        // send the packet
        try {
            assert broadcastSocket != null : "Broadcast Socket is null in sendBatch";
            broadcastSocket.send(sendPacket);
            if (logBatch) {
                for (int messageToSend : batch) {
                    // log the broadcast
                    try {
                        logMutex.lock();
                        logBuffer.log("b " + messageToSend);
                    } finally {
                        logMutex.unlock();
                    }
                }
            }
        } catch (AssertionError e) {
            System.out.println(e.getMessage());
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Failed to send batch number " + batchNumber + ": " + e.getMessage());
            if (broadcastSocket != null && !broadcastSocket.isClosed()) {
                broadcastSocket.close();
            }
            System.exit(1);
        }
    }

    public long awaitAcks(Deque<Integer>[] batches) {
        // Prepare a packet to receive ACK data
        byte[] ackData = new byte[17]; // senderId and batchNumber
        DatagramPacket ackPacket = new DatagramPacket(ackData, ackData.length);
        long rtt = 1;

        while (true) {
            try {
                // early exit condition - all acks received from everyone
                boolean allAcked = true;
                for (Deque<Integer> batch : batches)
                    if (batch.size() != 0) {
                        allAcked = false;
                        break;
                    }
                if (allAcked)
                    return rtt;

                assert broadcastSocket != null : "Broadcast Socket is null in awaitAcks()";
                broadcastSocket.receive(ackPacket); // this is blocking until received

                // the below executes only if ACK is received before timeout
                byte[] data = ackPacket.getData();
                int length = ackPacket.getLength();

                // Ensure the length is at least 8 to read two integers
                if (length < 17) {
                    throw new IllegalArgumentException("Packet is too short to contain two integers and a long.");
                }
                byte isAck = data[0];
                int ackSenderId = ((data[1] & 0xFF) << 24) | ((data[2] & 0xFF) << 16) | ((data[3] & 0xFF) << 8) | (data[4] & 0xFF);
                int ackBatchNumber = ((data[5] & 0xFF) << 24) | ((data[6] & 0xFF) << 16) | ((data[7] & 0xFF) << 8) | (data[8] & 0xFF);
                long sendTime =
                        ((long) (data[length - 8] & 0xFF) << 56) |
                                ((long) (data[length - 7] & 0xFF) << 48) |
                                ((long) (data[length - 6] & 0xFF) << 40) |
                                ((long) (data[length - 5] & 0xFF) << 32) |
                                ((long) (data[length - 4] & 0xFF) << 24) |
                                ((long) (data[length - 3] & 0xFF) << 16) |
                                ((long) (data[length - 2] & 0xFF) << 8) |
                                ((long) (data[length - 1] & 0xFF));
                if (isAck == (byte) 0 )
                    System.out.println("Should be an ack from " + ackSenderId + " batch " + ackBatchNumber);
                rtt = Math.max(rtt, System.currentTimeMillis() - sendTime);
//                System.out.println("rtt " + rtt);

                // remove ackBatchNumber from the batches queue
                if (ackSenderId == processId) { // should be always true TODO remove this if
//                    System.out.println(" delivered from ackSenderId " + batches[ackSenderId - 1].toString());
                    batches[ackSenderId - 1].remove(ackBatchNumber);
                }
                else
                    System.out.println("ERROR ACK meant for sender " + ackSenderId + " arrived to sender " + processId);

            } catch (java.net.SocketTimeoutException e) {
                // Timeout occurred, stop processing received
                System.out.println("awaitAcks Broadcast Socket timeout");
                break;
            } catch (AssertionError e) {
                System.err.println(e.getMessage());
                System.exit(1);
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println("Error while waiting for ACKs: " + e.getMessage());
                if (broadcastSocket != null && !broadcastSocket.isClosed()) {
                    broadcastSocket.close();
                }
                System.exit(1);
            }
        }
        return rtt;
    }

    // RECEIVER
    public void handleData(byte[] data, int length) {
        // Ensure the length is at least 17 to read 1 byte, two integers & 1 long
        if (length < 17) {
            throw new IllegalArgumentException("Packet is too short to contain two integers and a long.");
        }

        // Extract the first two integers
        byte isAck = data[0];
        if (isAck == (byte) 1)
            System.out.println("Should NOT be an ack");
        int senderId = ((data[1] & 0xFF) << 24) |
                ((data[2] & 0xFF) << 16) |
                ((data[3] & 0xFF) << 8) |
                (data[4] & 0xFF);
        int batchNumber = ((data[5] & 0xFF) << 24) |
                ((data[6] & 0xFF) << 16) |
                ((data[7] & 0xFF) << 8) |
                (data[8] & 0xFF);

        // Extract the last 8 bytes as a long (nanoTime)
        long sendTime = ((long) (data[length - 8] & 0xFF) << 56) |
                ((long) (data[length - 7] & 0xFF) << 48) |
                ((long) (data[length - 6] & 0xFF) << 40) |
                ((long) (data[length - 5] & 0xFF) << 32) |
                ((long) (data[length - 4] & 0xFF) << 24) |
                ((long) (data[length - 3] & 0xFF) << 16) |
                ((long) (data[length - 2] & 0xFF) << 8) |
                ((long) (data[length - 1] & 0xFF));

        if (isAck == (byte) 0) {
            // Read the remaining data as a UTF-8 string
            String message = new String(data, 9, length - 17, StandardCharsets.UTF_8);

            // Split the payload by spaces
            String[] parts = message.trim().split("\\s+");
            if (parts[0].length() == 0) {
                System.out.println("Empty Message (likely an ACK) " + message.trim() + " in batch " + batchNumber);
                return;
            }

            // Parse the remaining integers as messageNumbers
            for (String part : parts) {
//            System.out.println("part " + part + " from batchNumber " + batchNumber);
                int messageNumber = Integer.parseInt(part);  // Direct parsing without trim
                markDelivered(senderId, messageNumber);      // Process each number directly
            }

            System.out.println("sending ack for batch Number " + batchNumber);
            sendACK(senderId, batchNumber, sendTime);
        } else {
            rtt = Math.max(rtt, System.currentTimeMillis() - sendTime);
//                System.out.println("rtt " + rtt);

            // remove ackBatchNumber from the batches queue
            if (ackSenderId == processId) { // should be always true TODO remove this if
//                    System.out.println(" delivered from ackSenderId " + batches[ackSenderId - 1].toString());
                batches[ackSenderId - 1].remove(ackBatchNumber);
            }
            else
                System.out.println("ERROR ACK meant for sender " + ackSenderId + " arrived to sender " + processId);

        }
    }

    // TODO have one receive loop
    // this loop will receive data & acks simultaneously
    // each message must be modified and contain a bit isAck (e.g. the first bit)
    // if it is ack, process it on a separate thread
    // if it is a message, process it on a separate thread
    // broadcast loop thread and ack processing thread will share resources - timeout must be set & windowsize adjusted
    public void receive() {
        try {
            // Prepare a packet to receive data
            byte[] receiveData = new byte[UDP_PACKET_SIZE]; // TODO ask if we can assume 1024 Bytes as maximum size of a packet
            DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);

            while (true) {
                // Receive the packet
                try {
                    socket.receive(receivePacket);
                    handleData(receivePacket.getData(), receivePacket.getLength());
                } catch (SocketTimeoutException e) { // TODO this should not exist
                    System.out.println("Receiver time out exception! Ignoring it and restarting the loop");
                }
            }
        } catch (Exception e) {
            System.err.println("Error in receive loop: " + e.getMessage());
            e.printStackTrace();
            System.exit(2);
        }
//        } finally {
//            if (socket != null && !socket.isClosed()) {
//                System.out.println("Closing socket...");
////                socket.close();
//            }
//        }
    }

    private void sendACK(int senderId, int batchNumber, long sendTime) { // TODO sendACK(int senderId, int batchNumber) - should be enough to identify originality

        // get sender's IP and port
        InetAddress senderAddress = idToAddressPort.get(senderId).getKey();
        int senderPort = idToAddressPort.get(senderId).getValue();

        // prepare ackData
        ByteBuffer buffer = ByteBuffer.allocate(17);

        // Put the senderId and batchNumber into the ByteBuffer
        buffer.put((byte) 1); // 1 meaning it is an ACK
        buffer.putInt(senderId);
        buffer.putInt(batchNumber);
        buffer.putLong(sendTime);

        // Get the byte array from the ByteBuffer
        byte[] ackData = buffer.array();

        // Create ACK packet to send data back to the sender's address
        DatagramPacket ackPacket = new DatagramPacket(ackData, ackData.length, senderAddress, senderPort);

        try {
//            System.out.println("Sending ack from process " + processId + " port " + socket.getLocalPort() + " address " + socket.getLocalAddress() + " to " + senderId);
            socket.send(ackPacket);
        } catch (IOException e) {
            System.err.println("Failed to send ACK for batchNumber=" + batchNumber + ": " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private void markDelivered(int senderId, int messageNumber) {
        if (!delivered.isDelivered(senderId, messageNumber)) {
            try {
                logMutex.lock();
                logBuffer.log("d " + senderId + " " + messageNumber);
            } finally {
                logMutex.unlock();
            }
            delivered.setDelivered(senderId, messageNumber);
        }
    }
}
