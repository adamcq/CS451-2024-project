package cs451;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PerfectLinkMultiThread {
    private final HashMap<Integer, AbstractMap.SimpleEntry<InetAddress, Integer>> idToAddressPort;
    private final int receiverId;
    private final int senderId;
    InetAddress receiverAddress;
    int receiverPort;
    DeliveredCompressed delivered;
    BitSet acked;
    int MAX_WINDOW_SIZE = 65536; // 2^16
    private final int LOG_BUFFER_SIZE = 10000;
    private DatagramSocket socket;
    private final boolean isReceiver;
    private final int MAX_ACK_WAIT_TIME = 25;
    private final int UDP_PACKET_SIZE = 1024;
    private final int BATCH_SIZE = 8;
    private final int INCREMENT = 1;
    LogBuffer logBuffer;
    int numberOfMessages;
    int numbeOfBatches;
    private final ExecutorService threadPool;
    enum Phase {SLOW_START, CONGESTION_AVOIDANCE}

    public PerfectLinkMultiThread(HashMap<Integer, AbstractMap.SimpleEntry<InetAddress, Integer>> idToAddressPort, int receiverId, int senderId, String outputPath, int numberOfMessages) throws Exception {
        this.idToAddressPort = idToAddressPort;
        this.receiverId = receiverId;
        this.senderId = senderId;
        receiverAddress = idToAddressPort.get(receiverId).getKey();
        receiverPort = idToAddressPort.get(receiverId).getValue();
        isReceiver = (senderId == receiverId);
        logBuffer = new LogBuffer(LOG_BUFFER_SIZE, outputPath);
        this.numberOfMessages = numberOfMessages;
        if (isReceiver)
            delivered = new DeliveredCompressed(idToAddressPort.size(), MAX_WINDOW_SIZE, numberOfMessages);
        this.threadPool = Executors.newFixedThreadPool(8);
        initSocket();
    }

    // TODO try using another socket for sending ACKs on receiver
    // TODO try the tc.py script with different delay values and different loss probabilities
    // TODO change the logic for adjusting window size - based on receiver business rather than just packets dropped (e.g. 1024 / number of senders)
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
        int toAdd = (batches.size() == 0) ? -1 : batches.peekLast();
        for (int i = batches.size(); i < windowSize; i++) {
            toAdd = acked.nextClearBit(toAdd+1);
            if (toAdd > numbeOfBatches)
                return batches.size(); // end reached
            batches.add(toAdd);
        }
        return batches.size();
    }


    public void sendMessages(int numberOfMessages) {
        this.numberOfMessages = numberOfMessages;
        int ackedCount = 0;
        int numberOfBatches = numberOfMessages / BATCH_SIZE;
        if (numberOfMessages % BATCH_SIZE != 0)
            numberOfBatches++;

        this.numbeOfBatches = numberOfBatches;
        Deque<Integer> batches = new ArrayDeque<>();
        this.acked = new BitSet();
        this.acked.set(0);
        Phase phase = Phase.SLOW_START;
        int windowSize = 1;

        while (ackedCount < numberOfBatches) {
            // concurrent logic
            // have a queue batches
            // awaitAcks directly removes from the queue messages Acked
            // loadBatches loads the queue with up to windowSize elements from acked, where acked is clear
            // send the batches
            // await acks for X miliseconds
            // remove received acks from the queue one by one
            // ...

            windowSize = loadBatches(batches, windowSize); // windowSize can be shrunken in this method if it is the last batch

//            System.out.println("Phase is " + phase + " and window size " + windowSize);
//            System.out.println("Phase is " + phase + " and batch size  " + batches.size());
//            System.out.println("Phase is " + phase + " and unacked     " + (numberOfBatches - ackedCount));

            generateAndSendBatches(batches);
            awaitAcks(batches);

            int acksReceived = windowSize - batches.size();
//            System.out.println("Phase is " + phase + " and received    " + acksReceived +  " acks");
//            System.out.println();
            ackedCount += acksReceived;
//            System.out.println("Batches_size " + batches.size());

            windowSize = 300;

            // OLD LOGIC FOR WINDOW SIZE MANAGEMENT
//            if (ackedCount == 0) // initially wait
//                continue;
//            else if (batches.size() != 0) {
//                windowSize /= 2;
//                windowSize = Math.max(1, windowSize);
//                phase = Phase.CONGESTION_AVOIDANCE;
//            } else {
//                if (phase.equals(Phase.SLOW_START))
//                    windowSize *= 2;
//                else
//                    windowSize += INCREMENT;
//            }
        }

        // after all was sent
        System.out.println("All " + numberOfMessages + " messages were sent by delivering " + ackedCount + " batches.");
        if (socket != null && !socket.isClosed()) {
            socket.close();
        }
    }

    public void generateAndSendBatches(Deque<Integer> batches) {
//        System.out.println("Sending " + batches.size() + "  batches: " + batches.toString());
        for (int batchNumber : batches) {
            int currentBatchSize = Math.min(BATCH_SIZE, numberOfMessages - (batchNumber - 1) * BATCH_SIZE);

            // Create and fill the batch array
            int[] batch = new int[currentBatchSize];
            for (int j = 0; j < currentBatchSize; j++) {
                batch[j] = (batchNumber - 1) * BATCH_SIZE + j + 1;
            }
            sendBatch(batchNumber, batch);
        }
    }

    public void sendBatch(int batchNumber, int[] batch) {

        // Convert senderId and messageNumber to a space-separated string format
        StringBuilder payload = new StringBuilder(); // TODO ask if it's okay to send integer ID, integer batchNumber and then message as a string - this would lead to faster checking of acks. No need to read the string!!!

        // append message numbers to the batch
        for (int messageNumber : batch) {
            payload.append(messageNumber);
            payload.append(" ");
        }

        // Remove the last space
        if (payload.length() > 0) {
            payload.setLength(payload.length() - 1);
        }

        // payload e.g., "42 43 44 45 46 47 48 49 50"
        byte[] payloadBytes = payload.toString().getBytes(StandardCharsets.UTF_8);

        // Create a ByteBuffer
        ByteBuffer buffer = ByteBuffer.allocate(8 + payloadBytes.length); // integer, integer, string payload

        // Place the payload bytes into the buffer
        buffer.putInt(senderId);
        buffer.putInt(batchNumber);
        buffer.put(payloadBytes);

        // Create a packet to send data to the receiver's address
        byte[] sendData = buffer.array();
        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, receiverAddress, receiverPort);

        // send the packet
        try {
            assert socket != null;
            socket.send(sendPacket);
            threadPool.submit(() -> {
                for (int messageToSend : batch) {
                    // log the broadcast
                    logBuffer.log("b " + messageToSend);
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        }
    }

    public void awaitAcks(Deque<Integer> batches) {
        // Prepare a packet to receive ACK data
        byte[] ackData = new byte[8]; // senderId and batchNumber
        DatagramPacket ackPacket = new DatagramPacket(ackData, ackData.length);

        while (true) {
            try {
                if (batches.size() == 0)
                    return;

                assert socket != null;
                socket.receive(ackPacket); // this is blocking until received

                // the below executes only if ACK is received before timeout
                byte[] data = ackPacket.getData();
                int length = ackPacket.getLength();

                // Ensure the length is at least 8 to read two integers
                if (length < 8) {
                    throw new IllegalArgumentException("Packet is too short to contain two integers.");
                }
                int ackSenderId = ((data[0] & 0xFF) << 24) | ((data[1] & 0xFF) << 16) | ((data[2] & 0xFF) << 8) | (data[3] & 0xFF);
                int ackBatchNumber = ((data[4] & 0xFF) << 24) | ((data[5] & 0xFF) << 16) | ((data[6] & 0xFF) << 8) | (data[7] & 0xFF);

                // remove ackBatchNumber from the batches queue
                if (ackSenderId == senderId) { // should be always true TODO remove this if
//                    System.out.println("setting " + ackBatchNumber);
                    batches.remove(ackBatchNumber);
                    acked.set(ackBatchNumber);
                }
                else
                    System.out.println("ERROR ACK meant for sender " + ackSenderId + " arrived to sender " + senderId);

            } catch (java.net.SocketTimeoutException e) {
                // Timeout occurred, stop processing received
                break;
            } catch (Exception e) {
                e.printStackTrace();
                if (socket != null && !socket.isClosed()) {
                    socket.close();
                }
            }
        }
    }

    public void sendLoop(DatagramPacket sendPacket, DatagramPacket ackPacket, int[] batch, int batchNumber) {
        while (true) {
            try {
                assert socket != null;
                socket.send(sendPacket);
                threadPool.submit(() -> {
                    for (int messageToSend : batch) {
                        // log the broadcast
                        logBuffer.log("b " + messageToSend);
                    }
                });

                socket.receive(ackPacket); // this is blocking until received

                // the below executes only if ACK is received before timeout
                byte[] data = ackPacket.getData();
                int length = ackPacket.getLength();

                // Ensure the length is at least 8 to read two integers
                if (length < 8) {
                    throw new IllegalArgumentException("Packet is too short to contain two integers.");
                }
                int ackSenderId = ((data[0] & 0xFF) << 24) | ((data[1] & 0xFF) << 16) | ((data[2] & 0xFF) << 8) | (data[3] & 0xFF);
                int ackBatchNumber = ((data[4] & 0xFF) << 24) | ((data[5] & 0xFF) << 16) | ((data[6] & 0xFF) << 8) | (data[7] & 0xFF);


                if (ackBatchNumber == batchNumber) // TODO checking the senderId is pointless
                    break; // move on to the next batch
                else {
                    System.out.println("The below batch numbers should be the same:");
                    System.out.println("Batch " + batchNumber + " sent from " + senderId);
                    System.out.println("Batch " + ackBatchNumber + " ack received for " + ackSenderId);
                }
            } catch (java.net.SocketTimeoutException e) {
                // Timeout occurred, retransmit the message
                System.out.println("No ACK received. Retransmitting... " + senderId + " batch " + batchNumber);
            } catch (Exception e) {
                e.printStackTrace();
                if (socket != null && !socket.isClosed()) {
                    socket.close();
                }
            }
        }
    }

    public void handleData(byte[] data, int length) {
        // Ensure the length is at least 8 to read two integers
        if (length < 8) {
            throw new IllegalArgumentException("Packet is too short to contain two integers.");
        }

        // Extract the first two integers
        int senderId = ((data[0] & 0xFF) << 24) | ((data[1] & 0xFF) << 16) | ((data[2] & 0xFF) << 8) | (data[3] & 0xFF);
        int batchNumber = ((data[4] & 0xFF) << 24) | ((data[5] & 0xFF) << 16) | ((data[6] & 0xFF) << 8) | (data[7] & 0xFF);

        // Read the remaining data as a UTF-8 string
        String message = new String(data, 8, length - 8, StandardCharsets.UTF_8);

        // Split the payload by spaces
        String[] parts = message.split("\\s+");

        // Parse the remaining integers as messageNumbers
//        System.out.println(Arrays.toString(parts));
        for (String part : parts) {
            int messageNumber = Integer.parseInt(part);  // Direct parsing without trim
            markDelivered(senderId, messageNumber);      // Process each number directly
        }

        sendACK(senderId, batchNumber);
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
            e.printStackTrace();
        } finally {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        }
    }

    private void sendACK(int senderId, int batchNumber) { // TODO sendACK(int senderId, int batchNumber) - should be enough to identify originality

        // get sender's IP and port
        InetAddress senderAddress = idToAddressPort.get(senderId).getKey();
        int senderPort = idToAddressPort.get(senderId).getValue();

        // prepare ackData
        ByteBuffer buffer = ByteBuffer.allocate(8);

        // Put the senderId and batchNumber into the ByteBuffer
        buffer.putInt(senderId);
        buffer.putInt(batchNumber);

        // Get the byte array from the ByteBuffer
        byte[] ackData = buffer.array();

        // Create ACK packet to send data back to the sender's address
        DatagramPacket ackPacket = new DatagramPacket(ackData, ackData.length, senderAddress, senderPort);

        try {
            socket.send(ackPacket);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void markDelivered(int senderId, int messageNumber) {
        if (!delivered.isDelivered(senderId, messageNumber)) {
            logBuffer.log("d " + senderId + " " + messageNumber);
            delivered.setDelivered(senderId, messageNumber);
        }
    }

    public void shutdown() {
        threadPool.shutdown();
        if (socket != null && !socket.isClosed()) {
            socket.close();
        }
    }
}