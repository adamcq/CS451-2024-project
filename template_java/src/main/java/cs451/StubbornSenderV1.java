package cs451;

import java.net.*;
import java.nio.ByteBuffer;

public class StubbornSenderV1 {
    private final int senderId;
    private final PerfectLinkV1 perfectLink;
    private final InetAddress receiverAddress;
    private final int receiverPort;
    private DatagramSocket socket;

    public StubbornSenderV1(int senderId, PerfectLinkV1 perfectLink, InetAddress receiverAddress, int receiverPort) throws Exception {
        this.senderId = senderId;
        this.perfectLink = perfectLink;
        this.receiverAddress = receiverAddress;
        this.receiverPort = receiverPort;
        this.socket = new DatagramSocket();
    }

    public boolean sendMessage(int messageNumber) throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putInt(senderId);
        buffer.putInt(messageNumber);
        byte[] data = buffer.array();
        DatagramPacket packet = new DatagramPacket(data, data.length, receiverAddress, receiverPort);

        // log sending
        System.out.println("b " + messageNumber);

        // Loop until message is acknowledged (ACK received)
        while (!perfectLink.isDelivered(senderId, messageNumber)) {
            socket.send(packet);
//            System.out.println("bb " + messageNumber + " " + perfectLink.isDelivered(senderId, messageNumber) + " " + System.currentTimeMillis());
//            System.out.println("Sending Message " + messageNumber + " from Sender " + senderId);

            // Immediately listen for ACK after sending the message
//            if (receiveAck(senderId, messageNumber)) {
////                System.out.println("Message " + messageNumber + " acknowledged.");
//                perfectLink.markAsDelivered(senderId, messageNumber);
//            }
        }
        return true;
    }

    // Non-blocking method to check for ACK
    public boolean receiveAck(int expectedSenderId, int expectedMessageNumber) throws Exception {
        // Set a small timeout for socket.receive to make it non-blocking
//        socket.setSoTimeout(100);  // 100 milliseconds timeout

        try {
            byte[] ackBuffer = new byte[8];
            DatagramPacket ackPacket = new DatagramPacket(ackBuffer, ackBuffer.length);
            socket.receive(ackPacket);  // Receive ACK if available

            ByteBuffer ackData = ByteBuffer.wrap(ackPacket.getData());
            int ackSenderId = ackData.getInt();
            int ackMessageNumber = ackData.getInt();

            // Check if the received ACK corresponds to the expected message
            return ackSenderId == expectedSenderId && ackMessageNumber == expectedMessageNumber;
        } catch (SocketTimeoutException e) {
            // No ACK received within timeout, continue sending
            return false;
        }
    }
}
