package cs451;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.HashMap;

public class PerfectReceiver {
    private final HashMap<Integer, AbstractMap.SimpleEntry<InetAddress, Integer>> idToAddressPort;
    private DatagramSocket socket;
    private final MemoryFriendlyBitSet delivered;
    private LogBuffer logBuffer;
    private int UDP_PACKET_SIZE = 1024;


    public PerfectReceiver(int processId,
                           int numberOfMessages,
                           HashMap<Integer, AbstractMap.SimpleEntry<InetAddress, Integer>> idToAddressPort,
                           String outputPath,
                           DatagramSocket socket) {
        this.idToAddressPort = idToAddressPort;
        this.delivered = new MemoryFriendlyBitSet(idToAddressPort.size(), numberOfMessages);

        // init log buffer
        try {
            this.logBuffer = new LogBuffer(outputPath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.socket = socket;
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
                try {
                    socket.receive(receivePacket);
                    handleData(receivePacket.getData(), receivePacket.getLength());
                } catch (SocketTimeoutException e) {
                    System.out.println("Receiver time out exception! Ignoring it and restarting the loop");
                }
            }
        } catch (Exception e) {
            System.err.println("Error in receive loop: " + e.getMessage());
            e.printStackTrace();
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
        if (!delivered.isSet(senderId, messageNumber)) {
            logBuffer.log("d " + senderId + " " + messageNumber);
            delivered.set(senderId, messageNumber);
        }
    }
}
