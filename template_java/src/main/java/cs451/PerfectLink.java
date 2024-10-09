package cs451;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class PerfectLink {
    private final HashMap<Integer, AbstractMap.SimpleEntry<InetAddress, Integer>> idToAddressPort;
    private final int receiverId;
    private final int senderId;
    InetAddress receiverAddress;
    int receiverPort;
    private final ExecutorService executor;
    boolean[][] delivered; // TODO change the data structure for delivered
    int MAX_MESSAGES = 10000;
    private DatagramSocket socket;
    private final boolean isReceiver;
    private final int MAX_ACK_WAIT_TIME = 100;

    public PerfectLink(HashMap<Integer, AbstractMap.SimpleEntry<InetAddress, Integer>> idToAddressPort, int receiverId, int senderId) throws Exception {
        this.idToAddressPort = idToAddressPort;
        this.receiverId = receiverId;
        this.senderId = senderId;
        receiverAddress = idToAddressPort.get(receiverId).getKey();
        receiverPort = idToAddressPort.get(receiverId).getValue();
        isReceiver = (senderId == receiverId);
        this.executor = Executors.newFixedThreadPool(8); // Limit to 8 threads

        initSocket();
    }

    private void initSocket() {
        InetAddress senderAddress = idToAddressPort.get(senderId).getKey();
        int senderPort = idToAddressPort.get(senderId).getValue();

        try {
            socket = new DatagramSocket(senderPort, senderAddress); // this works for both sender and receiver, because we put senderId == receiverId for receiver in Main

            if (!isReceiver) {
                socket.setSoTimeout(MAX_ACK_WAIT_TIME);
//                Thread.sleep(100); // Give receiver time to start
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
        System.out.println("b " + messageNumber);

        // Prepare a packet to receive ACK data
        byte[] ackData = new byte[8];
        DatagramPacket ackPacket = new DatagramPacket(ackData, ackData.length);

        // Send the packet
        while (true) {
            try {
                socket.send(sendPacket);
                System.out.println("ack sent at time:\t\t" + System.nanoTime());
                socket.receive(ackPacket); // this is blocking until received
                System.out.println("ack received at time:\t" + System.nanoTime());

                // Extract the data (2 integers)
                ByteBuffer byteBuffer = ByteBuffer.wrap(ackPacket.getData());
                int senderId = byteBuffer.getInt();
                int ackNumber = byteBuffer.getInt();
                System.out.println("Acked " + ackNumber + " from id " + senderId);
                break;
            } catch(java.net.SocketTimeoutException e) {
                // Timeout occurred, retransmit the message
                System.out.println("No ACK received. Retransmitting...");
            } catch(Exception e) {
                e.printStackTrace();
                if (socket != null && !socket.isClosed()) {
                    socket.close();
                }
            }
        }
        if (socket != null && !socket.isClosed()) {
            socket.close();
        }
    }

    // Method to send multiple messages
    public void sendMessages(int numberOfMessages) {
        for (int message = 1; message <= numberOfMessages; message++) {
            final int msg = message; // must be final or effectively final to use in lambda
            executor.submit(() -> send(msg));  // Submit each send task to the thread pool
        }
    }

    public void receive() {
        System.out.println(idToAddressPort.size());
        delivered = new boolean[idToAddressPort.size()][MAX_MESSAGES];

        try {
            // Prepare a packet to receive data
            byte[] receiveData = new byte[8];
            DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);

            System.out.println("UDP Receiver is running and waiting for data on " + receiverAddress + ":" + receiverPort);

            while (true) {
                // Receive the packet
                socket.receive(receivePacket);

                // Submit processing of the received packet to a thread
                executor.submit(() -> {
                    // Extract the data (2 integers)
                    ByteBuffer byteBuffer = ByteBuffer.wrap(receivePacket.getData());
                    int senderId = byteBuffer.getInt();
                    int messageNumber = byteBuffer.getInt();

                    if (!isDelievred(senderId, messageNumber)) {
                        System.out.println("d " + senderId + " " + messageNumber);
                        markDelivered(senderId, messageNumber);
                    }

                    sendACK(senderId, messageNumber);
                });
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

    public void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private void markDelivered(int senderId, int messageNumber) {
        delivered[senderId-1][messageNumber-1] = true;
    }
    private boolean isDelievred(int senderId, int messageNumber) {
        return delivered[senderId-1][messageNumber-1];
    }
}