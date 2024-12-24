//package cs451;
//
//import java.net.DatagramPacket;
//import java.net.UnknownHostException;
//import java.nio.ByteBuffer;
//
//public class TestReceiver {
//    public TestReceiver() {
//
//    }
//
//    public void receive() {
//        System.out.println("Receiver active");
//
//        int UDP_PACKET_SIZE = 512;
//        byte[] receiveData = new byte[UDP_PACKET_SIZE]; // TODO ask if we can assume 1024 Bytes as maximum size of a packet
//        DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
//
//        while (true) {
//            // Receive the packet
//            try {
//                socket.receive(receivePacket);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//
//            byte[] data = receivePacket.getData();
//            int length = receivePacket.getLength();
//
//            final int receivedCounterCopy = receivedCounter++;
//            if (data[0] == (byte) 0) {
//                receivedMessages++;
//                handleMessage(data, length, receivePacket.getPort(), receivedCounterCopy);
//            } else if (data[0] == (byte) 1) {
//                receivedAcks++;
//                handleAck(data, length, receivePacket.getPort(), receivedCounterCopy);
//            } else {
//                System.out.println("WHAAAATT DID YOU SEEEEEEEEND OMGGGG");
//            }
//        }
//    }
//
//    private void handleMessage(byte[] data, int length, int port, int receivedCounter) {
//        System.out.println("Received " + ByteBuffer.wrap(data).getInt(1));
//        sendAckExecutor.submit(
//                () -> {
//                    try {
//                        sendAck(data, length, port, receivedCounter);
//                    } catch (UnknownHostException e) {
//                        throw new RuntimeException(e);
//                    }
//                }
//        );
//    }
//
//    private void handleAck(byte[] data, int length, int port, int receivedCounter) {
//        int numberReceived = ByteBuffer.wrap(data).getInt(1);
//
//        acked.set(numberReceived);
//        startSendFrom = acked.nextClearBit(startSendFrom);
//    }
//
//    private void sendAck(byte[] data, int length, int port, int receivedCounter) throws UnknownHostException {
//        data[0] = (byte) 1;
//        sentAcks.getAndIncrement();
//        send(data, ports[hostId == 0 ? 1 : 0]);
//    }
//}
