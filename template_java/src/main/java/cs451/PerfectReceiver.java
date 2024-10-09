package cs451;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.HashMap;

public class PerfectReceiver {
    private final String ipAddress;
    private final int port;
    private final int[] delivered;
    HashMap<Integer, AbstractMap.SimpleEntry<String, Integer>> idToAddressPort;

    public PerfectReceiver(String ipAddress, int port, HashMap<Integer, AbstractMap.SimpleEntry<String, Integer>> idToAddressPort) {
        this.ipAddress = ipAddress;
        this.port = port;
        this.idToAddressPort = idToAddressPort;
        delivered = new int[128];
    }

    public void receiveData() {
        DatagramSocket socket = null;

        try {
            // Convert the IP address string to InetAddress
            InetAddress address = InetAddress.getByName(ipAddress);

            // Create a DatagramSocket bound to the IP address and port
            socket = new DatagramSocket(port, address);
            byte[] receiveData = new byte[8];

            // Prepare a packet to receive data
            DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);

            System.out.println("UDP Receiver is running and waiting for data on " + ipAddress + ":" + port);

            while (true) {
                // Receive the packet
                socket.receive(receivePacket);

                // Extract the data (2 integers)
                ByteBuffer byteBuffer = ByteBuffer.wrap(receivePacket.getData());
                int senderId = byteBuffer.getInt();
                int messageNumber = byteBuffer.getInt();

                System.out.println("d " + senderId + " " + messageNumber);
                delivered[senderId]++;
                sendAck(socket, senderId, messageNumber);
//                System.out.println("sss " + senderId + " " + delivered[senderId]);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        }
    }

    public void sendAck(DatagramSocket socket, int senderId, int messageToAck) throws IOException {
        AbstractMap.SimpleEntry<String, Integer> receiverAddressPort= idToAddressPort.get(senderId);
        String senderIp = receiverAddressPort.getKey();
        int senderPort = receiverAddressPort.getValue();

        // Create a ByteBuffer to hold the integer
        ByteBuffer buffer = ByteBuffer.allocate(4); // 4 bytes for an integer
        buffer.putInt(messageToAck);

        // Convert the ByteBuffer to a byte array
        byte[] data = buffer.array();

        // send Ack - new byte[1] - empty array (doesn't matter)
        DatagramPacket sendPacket = new DatagramPacket(data, data.length, InetAddress.getByName(senderIp), senderPort);

        socket.send(sendPacket);
    }
}

