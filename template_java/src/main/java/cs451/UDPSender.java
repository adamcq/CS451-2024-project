package cs451;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.ByteBuffer;

public class UDPSender {
    private final String receiverIpAddress;
    private final int receiverPort;
    private final String senderIpAddress;  // Optional
    private final int senderPort;

    public UDPSender(String receiverIpAddress, int receiverPort, String senderIpAddress, int senderPort) {
        this.receiverIpAddress = receiverIpAddress;
        this.receiverPort = receiverPort;
        this.senderIpAddress = senderIpAddress;
        this.senderPort = senderPort;
    }

    public void sendData(int id, int messageNumber) {
        DatagramSocket socket = null;
        try {
            // Convert the receiver IP address string to InetAddress
            InetAddress receiverAddress = InetAddress.getByName(receiverIpAddress);
            InetAddress senderAddress = InetAddress.getByName(senderIpAddress); // Sender's IP

            // Create a DatagramSocket bound to the sender's IP address and port
            socket = new DatagramSocket(senderPort, senderAddress);

            // create a byte buffer to hold ID & message number - 4 bytes each
            ByteBuffer buffer = ByteBuffer.allocate(8);
            buffer.putInt(id);
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
