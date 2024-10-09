package cs451;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.HashMap;

public class FairLossLink {
    HashMap<Integer, AbstractMap.SimpleEntry<InetAddress, Integer>> idToAddressPort;
    private final InetAddress receiverAddress;
    private final int receiverPort;
    private final InetAddress senderAddress;
    private final int senderPort;
    private final int senderId;
    private final int receiverId;

    public FairLossLink(HashMap<Integer, AbstractMap.SimpleEntry<InetAddress, Integer>> idToAddressPort, int receiverId, int senderId) {
        this.idToAddressPort = idToAddressPort;
        this.senderId = senderId;
        this.receiverId = receiverId;
        receiverAddress = idToAddressPort.get(receiverId).getKey();
        receiverPort = idToAddressPort.get(receiverId).getValue();
        senderAddress = idToAddressPort.get(senderId).getKey();
        senderPort = idToAddressPort.get(senderId).getValue();
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

    public void sendMessages(int numberOfMessages) {
        for (int i = 0; i < numberOfMessages; i++) {
            send(i+1);
        }
    }

    public void send(int messageNumber) {
        DatagramSocket socket = null;
        try {
            // Create a DatagramSocket bound to the sender's IP address and port
            socket = new DatagramSocket(senderPort, senderAddress);

            // create a byte buffer to hold ID & message number - 4 bytes each
            ByteBuffer buffer = ByteBuffer.allocate(8);
            buffer.putInt(senderId);
            buffer.putInt(messageNumber);

            // Create a packet to send data to the receiver's address
            byte[] sendData = buffer.array();
            DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, receiverAddress, receiverPort);

            // Send the packet
            socket.send(sendPacket);

            // log the broadcast
            System.out.println("b " + messageNumber);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        }
    }
}
