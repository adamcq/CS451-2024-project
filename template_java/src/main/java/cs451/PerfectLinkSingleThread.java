package cs451;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;

public class PerfectLinkSingleThread {
    private final HashMap<Integer, AbstractMap.SimpleEntry<InetAddress, Integer>> idToAddressPort;
    private final int receiverId;
    private final int senderId;
    InetAddress receiverAddress;
    int receiverPort;
    boolean[][] delivered; // TODO change the data structure for delivered
    int bufferSize = 1000;
    int MAX_MESSAGES = 10000000; // TODO change
    private DatagramSocket socket;
    private final boolean isReceiver;
    private final int MAX_ACK_WAIT_TIME = 100;
    LogBuffer logBuffer;
    int numberOfMessages;

    public PerfectLinkSingleThread(HashMap<Integer, AbstractMap.SimpleEntry<InetAddress, Integer>> idToAddressPort, int receiverId, int senderId, String outputPath) throws Exception {
        this.idToAddressPort = idToAddressPort;
        this.receiverId = receiverId;
        this.senderId = senderId;
        receiverAddress = idToAddressPort.get(receiverId).getKey();
        receiverPort = idToAddressPort.get(receiverId).getValue();
        isReceiver = (senderId == receiverId);
        logBuffer = new LogBuffer(bufferSize, outputPath);
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

    public void send(int messageNumber) {
        // create a byte buffer to hold ID & message number - 4 bytes each
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putInt(senderId);
        buffer.putInt(messageNumber);

        // Create a packet to send data to the receiver's address
        byte[] sendData = buffer.array();
        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, receiverAddress, receiverPort);

        // log the broadcast
        logBuffer.log("b " + messageNumber);

        // Prepare a packet to receive ACK data
        byte[] ackData = new byte[8];
        DatagramPacket ackPacket = new DatagramPacket(ackData, ackData.length);

        // Send the packet
        while (true) {
            try {
                socket.send(sendPacket);
//                System.out.println("ack sent at time:\t\t" + System.nanoTime());
                socket.receive(ackPacket); // this is blocking until received
//                System.out.println("ack received at time:\t" + System.nanoTime());

                // Extract the data (2 integers)
                ByteBuffer byteBuffer = ByteBuffer.wrap(ackPacket.getData());
                int senderId = byteBuffer.getInt();
                int ackNumber = byteBuffer.getInt();
//                if (ackNumber == numberOfMessages) { // TODO change this logic to check if it's the last batch
//                    logBuffer.close();
//                }
//                System.out.println("Acked " + ackNumber + " from id " + senderId);
                break;
            } catch(java.net.SocketTimeoutException e) {
                // Timeout occurred, retransmit the message
                System.out.println("No ACK received. Retransmitting... " + senderId + " " + messageNumber);
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

    // Method to send multiple messages
    public void sendMessages(int numberOfMessages) {
        // TODO send 8 at a time
        this.numberOfMessages = numberOfMessages;
        for (int message = 1; message <= numberOfMessages; message++) {
            final int msg = message; // must be final or effectively final to use in lambda
            send(msg);
        }
    }

    public void receive() {
        System.out.println(idToAddressPort.size());

        delivered = new boolean[idToAddressPort.size()][MAX_MESSAGES];

        try {
            // Prepare a packet to receive data
            byte[] receiveData = new byte[8];
            DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);

//            System.out.println("UDP Receiver is running and waiting for data on " + receiverAddress + ":" + receiverPort);

            while (true) {
                // Receive the packet
                socket.receive(receivePacket);

                // Extract the data (2 integers)
                ByteBuffer byteBuffer = ByteBuffer.wrap(receivePacket.getData());
                int senderId = byteBuffer.getInt();
                int messageNumber = byteBuffer.getInt();

                markDelivered(senderId, messageNumber);

                sendACK(senderId, messageNumber);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        }
    }

    private void sendACK(int senderId, int messageNumber) {
        // create a byte buffer to hold ID & message number - 4 bytes each
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putInt(senderId);
        buffer.putInt(messageNumber);

        // get sender's IP and port
        InetAddress senderAddress = idToAddressPort.get(senderId).getKey();
        int senderPort = idToAddressPort.get(senderId).getValue();

        // Create ACK packet to send data back to the sender's address
        byte[] ackData = buffer.array();
        DatagramPacket ackPacket = new DatagramPacket(ackData, ackData.length, senderAddress, senderPort);

        try {
            socket.send(ackPacket);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void markDelivered(int senderId, int messageNumber) {
        if (!delivered[senderId - 1][messageNumber - 1]) {
            logBuffer.log("d " + senderId + " " + messageNumber); // TODO some messages might remain undelivered at the end
            delivered[senderId - 1][messageNumber - 1] = true;
        }
    }
}