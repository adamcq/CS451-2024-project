package cs451;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class PerfectLinkSingleThread {
    private final HashMap<Integer, AbstractMap.SimpleEntry<InetAddress, Integer>> idToAddressPort;
    private final int receiverId;
    private final int senderId;
    InetAddress receiverAddress;
    int receiverPort;
    BitSet[] delivered; // TODO change to sliding window
    private final int LOG_BUFFER_SIZE = 10000;
    private DatagramSocket socket;
    private final boolean isReceiver;
    private final int MAX_ACK_WAIT_TIME = 100;
    private final int UDP_PACKET_SIZE = 1024;
    private final int BATCH_SIZE = 8;
    LogBuffer logBuffer;
    int numberOfMessages;

    public PerfectLinkSingleThread(HashMap<Integer, AbstractMap.SimpleEntry<InetAddress, Integer>> idToAddressPort, int receiverId, int senderId, String outputPath) throws Exception {
        this.idToAddressPort = idToAddressPort;
        this.receiverId = receiverId;
        this.senderId = senderId;
        receiverAddress = idToAddressPort.get(receiverId).getKey();
        receiverPort = idToAddressPort.get(receiverId).getValue();
        isReceiver = (senderId == receiverId);
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

    // Method to send multiple messages
    public void sendMessages(int numberOfMessages) {
        this.numberOfMessages = numberOfMessages;
        // Loop from 1 to numberOfMessages
        int batchNumber = 1;
        for (int i = 1; i <= numberOfMessages; i += BATCH_SIZE) {
            // Determine the size of the current batch
            int currentBatchSize = Math.min(BATCH_SIZE, numberOfMessages - i + 1);

            // Create and fill the batch array
            int[] batch = new int[currentBatchSize];
            for (int j = 0; j < currentBatchSize; j++) {
                batch[j] = i + j;
            }

            // Send the current batch
            send(batchNumber, batch);
            batchNumber++;
        }
    }

    public void send(int batchNumber, int[] batch) { // e.g. [1, 2, 3, 4, 5, 6, 7]

        // Convert senderId and messageNumber to a space-separated string format
        StringBuilder payload = new StringBuilder(); // TODO modify the packet to send integer ID, integer batchNumber and then message as a string - this would lead to faster checking of acks. No need to read the string!!!

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

        // Prepare a packet to receive ACK data
        byte[] ackData = new byte[8]; // senderId and batchNumber
        DatagramPacket ackPacket = new DatagramPacket(ackData, ackData.length);

        // Send the packet
        while (true) {
            try {
                assert socket != null;
                socket.send(sendPacket);
                for (int messageToSend : batch) {
                    // log the broadcast
                    logBuffer.log("b " + messageToSend);
                }
                socket.receive(ackPacket); // this is blocking until received

                // the below only executes if ACK is received TODO do something with the ack info (unnecessary in sequential sending because waiting for ack is blocking right after send..)
                ByteBuffer ackBuffer = ByteBuffer.wrap(ackPacket.getData(), 0, ackPacket.getLength());
                int ackSenderId = ackBuffer.getInt();
                int ackBatchNumber = ackBuffer.getInt();

//                // Optimized way to extract the first two integers
//                byte[] data = ackPacket.getData();
//                int length = ackPacket.getLength();
//
//                // Ensure the length is at least 8 to read two integers
//                if (length < 8) {
//                    throw new IllegalArgumentException("Packet is too short to contain two integers.");
//                }
//                int ackSenderId = ((data[0] & 0xFF) << 24) | ((data[1] & 0xFF) << 16) | ((data[2] & 0xFF) << 8) | (data[3] & 0xFF);
//                int ackBatchNumber = ((data[4] & 0xFF) << 24) | ((data[5] & 0xFF) << 16) | ((data[6] & 0xFF) << 8) | (data[7] & 0xFF);


                if (ackBatchNumber == batchNumber) // TODO checking the senderId is pointless
                    break; // move on to the next batch
                else {
                    System.out.println("The below batch numbers should be the same:");
                    System.out.println("Batch " + batchNumber + " sent from " + senderId + ": " + payload);
                    System.out.println("Batch " + ackBatchNumber + " ack received for " + ackSenderId);
                }
            } catch(java.net.SocketTimeoutException e) {
                // Timeout occurred, retransmit the message
                System.out.println("No ACK received. Retransmitting... " + senderId + " batch " + batchNumber);
            } catch(Exception e) {
                e.printStackTrace();
                if (socket != null && !socket.isClosed()) {
                    socket.close();
                }
            }
        }
//        if (socket != null && !socket.isClosed()) {
//            socket.close();
//        }
    }

    public void handleData(byte[] data, int length) {

        // Extract data from packet
        ByteBuffer receiveBuffer = ByteBuffer.wrap(data, 0, length);
        int senderId = receiveBuffer.getInt();
        int batchNumber = receiveBuffer.getInt();
        byte[] receivePayloadBytes = new byte[receiveBuffer.remaining()];
        receiveBuffer.get(receivePayloadBytes);
        String message = new String(receivePayloadBytes, StandardCharsets.UTF_8);

        // Split the payload by spaces
        String[] parts = message.split("\\s+");

        // Parse the remaining integers as messageNumbers
//        System.out.println(Arrays.toString(parts));
        List<Integer> messageNumbers = new ArrayList<>();
        for (int i = 0; i < parts.length; i++) {
            messageNumbers.add(Integer.parseInt(parts[i].trim()));
        }

        for (int messageNumber : messageNumbers) {
            markDelivered(senderId, messageNumber);
        }
        sendACK(senderId, batchNumber);
    }

    public void receive() {
        delivered = new BitSet[idToAddressPort.size()];
        for (int i = 0; i < delivered.length; i++) {
            delivered[i] = new BitSet();
        }

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
        if (!delivered[senderId - 1].get(messageNumber - 1)) {
            logBuffer.log("d " + senderId + " " + messageNumber);
            delivered[senderId - 1].set(messageNumber - 1);
        }
    }
}