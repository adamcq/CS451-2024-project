package cs451;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class StubbornLink {
    private final HashMap<Integer, AbstractMap.SimpleEntry<InetAddress, Integer>> idToAddressPort;
    private final InetAddress receiverAddress;
    private final int receiverPort;
    private final InetAddress senderAddress;
    private final int senderPort;
    private final int senderId;
    private final int receiverId;
    private final ExecutorService executor;
    private FairLossLink fairLossLink;

    public StubbornLink(HashMap<Integer, AbstractMap.SimpleEntry<InetAddress, Integer>> idToAddressPort, int receiverId, int senderId) {
        this.idToAddressPort = idToAddressPort;
        this.senderId = senderId;
        this.receiverId = receiverId;
        receiverAddress = idToAddressPort.get(receiverId).getKey();
        receiverPort = idToAddressPort.get(receiverId).getValue();
        senderAddress = idToAddressPort.get(senderId).getKey();
        senderPort = idToAddressPort.get(senderId).getValue();
        fairLossLink = new FairLossLink(idToAddressPort, receiverId, senderId);

        // Initialize an ExecutorService with a fixed thread pool of 8 threads
        executor = Executors.newFixedThreadPool(8);
    }

    // The single send() function that handles all message sending logic
    public void sendMessages(int numberOfMessages) {
        for (int message = 1; message <= numberOfMessages; message++)
            send(message);
    }

    public void send(int messageNumber) {
        while (true) {
            fairLossLink.send(messageNumber);
        }
    }

    // Each thread sends messages in an infinite loop
    private void sendMessagesInLoop(int threadId, int numberOfMessages) {
        DatagramSocket socket = null;
        try {
            socket = new DatagramSocket(senderPort + threadId, senderAddress); // Unique port for each thread
            while (true) {
                // Cycle through all messages, and each thread sends all messages in a loop
                for (int messageNumber = 1; messageNumber <= numberOfMessages; messageNumber++) {
                    sendSingleMessage(socket, messageNumber);  // Send each message
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        }
    }

    // The actual logic to send a single message
    private void sendSingleMessage(DatagramSocket socket, int messageNumber) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(8);
            buffer.putInt(senderId);
            buffer.putInt(messageNumber);

            // Create a packet to send data to the receiver's address
            byte[] sendData = buffer.array();
            DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, receiverAddress, receiverPort);

            // Send the packet
            socket.send(sendPacket);
            // Log the broadcast
            System.out.println("Thread " + Thread.currentThread().getId() + " sent message " + messageNumber);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Optionally, shutdown the executor service when done
    public void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }
    }

    public void receive() {
        DatagramSocket socket = null;
        try {
            // Create a DatagramSocket bound to the IP address and port
            socket = new DatagramSocket(receiverPort, receiverAddress);
            byte[] receiveData = new byte[8];

            // Prepare a packet to receive data
            DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);

            System.out.println("UDP Receiver is running and waiting for data on " + receiverAddress + ":" + receiverPort);

            while (true) {
                // Receive the packet
                socket.receive(receivePacket);

                // Extract the data (2 integers)
                ByteBuffer byteBuffer = ByteBuffer.wrap(receivePacket.getData());
                int senderId = byteBuffer.getInt();
                int messageNumber = byteBuffer.getInt();

                System.out.println("d " + senderId + " " + messageNumber);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        }
    }
}
