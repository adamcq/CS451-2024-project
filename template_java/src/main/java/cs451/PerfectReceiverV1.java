package cs451;

import java.net.*;
import java.nio.ByteBuffer;

public class PerfectReceiverV1 {
    private final InetAddress receiverIp;
    private final PerfectLinkV1 perfectLink;
    private DatagramSocket socket;

    public PerfectReceiverV1(InetAddress receiverIp, PerfectLinkV1 perfectLink, int port) throws Exception {
        this.receiverIp = receiverIp;
        this.perfectLink = perfectLink;
        this.socket = new DatagramSocket(port, receiverIp);
    }

    public void receiveMessage() throws Exception {
        byte[] buffer = new byte[8];
        DatagramPacket receivePacket = new DatagramPacket(buffer, buffer.length);

        while (true) {
            socket.receive(receivePacket);  // Blocking until message received
//            System.out.println("dd " + System.currentTimeMillis());

            ByteBuffer byteBuffer = ByteBuffer.wrap(receivePacket.getData());
            int senderId = byteBuffer.getInt();
            int messageNumber = byteBuffer.getInt();

            // Check if the message has already been delivered
            if (!perfectLink.isDelivered(senderId, messageNumber)) {

                // Mark message as delivered
                perfectLink.markAsDelivered(senderId, messageNumber);

                // log delivered
                System.out.println("d " + senderId + " " + messageNumber);
//                break;

//                // Send ACK immediately
//                sendAck(senderId, messageNumber, receivePacket.getAddress(), receivePacket.getPort());
            }
        }
    }

    private void sendAck(int senderId, int messageNumber, InetAddress address, int port) throws Exception {
        ByteBuffer ackBuffer = ByteBuffer.allocate(8);
        ackBuffer.putInt(senderId);
        ackBuffer.putInt(messageNumber);
        byte[] ackData = ackBuffer.array();

        DatagramPacket ackPacket = new DatagramPacket(ackData, ackData.length, address, port);
        socket.send(ackPacket);
//        System.out.println("Sent ACK for Message " + messageNumber + " to Sender " + senderId);
        System.out.println("d " + senderId + " " + messageNumber);
    }
}
