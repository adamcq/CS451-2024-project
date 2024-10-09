package cs451;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.ByteBuffer;

public class UDPReceiver {
    private final String ipAddress;
    private final int port;

    public UDPReceiver(String ipAddress, int port) {
        this.ipAddress = ipAddress;
        this.port = port;
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

