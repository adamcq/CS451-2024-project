//package cs451;
//
//import java.io.IOException;
//import java.net.DatagramPacket;
//import java.net.UnknownHostException;
//import java.nio.ByteBuffer;
//
//public class TestSender {
//
//    public TestSender() {
//
//    }
//    public void sendLoop(int numberOfMessages) {
//        while (true) {
//            int counter = 0;
//            int number = acked.nextClearBit(startSendFrom);
//            while (counter < SEND_WINDOW_SIZE) {
//                number = acked.nextClearBit(number);
//                if (number > numberOfMessages) {
//                    break;
//                }
//                sentMessages++;
////                System.out.println("send " + number);
//                send(number++, ports[hostId == 0 ? 1 : 0]);
//                counter++;
//            }
//        }
//    }
//
//    public void send(int number, int port) {
//        ByteBuffer buffer = ByteBuffer.allocate(5);
//
//        buffer.put((byte) 0);
//        buffer.putInt(number);
//
//        byte[] ackData = buffer.array();
//
//        send(ackData, port);
//    }
//
//    private void send(byte[] data, int port)  {
//        DatagramPacket ackPacket = new DatagramPacket(data, data.length, dstAddress, port);
//
//        try {
//            sentCounter++;
//            socket.send(ackPacket);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }
//}
