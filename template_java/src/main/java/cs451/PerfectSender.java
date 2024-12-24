package cs451;

import cs451.Message.Message;

import java.net.DatagramPacket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.BitSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class PerfectSender {
    BitSet acked;
    int smallestAcked = 0;
    AtomicBoolean allAcked;
    RunConfig runConfig;
    public PerfectSender(RunConfig runConfig) {
        this.runConfig = runConfig;
        this.allAcked = new AtomicBoolean();
    }

    public void send(Message[] messages) {
        Thread awaitAcksThread = new Thread(() -> {
                awaitAcks(messages);
            }, "AwaitAcksThread");
        awaitAcksThread.start();

        while (!allAcked.get()) {
//            for (Message m : messages) {
//                send(m);
//            }
        }
        System.out.println("Send complete for messages " + Arrays.toString(messages));
    }

//    public void send(Message m) {
//        byte[] numbers = payload.toString().getBytes(StandardCharsets.UTF_8);
//
//        ByteBuffer buffer = ByteBuffer.allocate(8 + numbers.length); // integer, integer, string payload
//
//        buffer.putInt(runConfig.getProcessId());
//        buffer.putInt(batchNumber);
//        buffer.put(numbers);
//
//        byte[] sendData = buffer.array();
//        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, receiverAddress, receiverPort);
//
//        byte[] ackData = new byte[8]; // senderId and batchNumber
//        DatagramPacket ackPacket = new DatagramPacket(ackData, ackData.length);
//
//        // Send the packet
//        while (true) {
//            try {
//                assert socket != null;
//                socket.send(sendPacket);
//                for (int messageToSend : batch) {
//                    // log the broadcast
//                    logBuffer.log("b " + messageToSend);
//                }
//                socket.receive(ackPacket); // this is blocking until received
//
//                // the below only executes if ACK is received TODO do something with the ack info (unnecessary in sequential sending because waiting for ack is blocking right after send..)
//                ByteBuffer ackBuffer = ByteBuffer.wrap(ackPacket.getData(), 0, ackPacket.getLength());
//                int ackSenderId = ackBuffer.getInt();
//                int ackBatchNumber = ackBuffer.getInt();
//
//                if (ackBatchNumber == batchNumber) // TODO checking the senderId is pointless
//                    break; // move on to the next batch
//                else {
//                    System.out.println("The below batch numbers should be the same:");
//                    System.out.println("Batch " + batchNumber + " sent from " + runConfig.getProcessId() + ": " + payload);
//                    System.out.println("Batch " + ackBatchNumber + " ack received for " + ackSenderId);
//                }
//            } catch(java.net.SocketTimeoutException e) {
//                // Timeout occurred, retransmit the message
//                System.out.println("No ACK received. Retransmitting... " + runConfig.getProcessId() + " batch " + batchNumber);
//            } catch(Exception e) {
//                e.printStackTrace();
//                if (socket != null && !socket.isClosed()) {
//                    socket.close();
//                }
//            }
//        }
////        if (socket != null && !socket.isClosed()) {
////            socket.close();
////        }
//    }

    private void awaitAcks(Message[] messages) {
        this.acked = new BitSet(messages.length);
        while (smallestAcked < messages.length) {
//            runConfig.getSocket().receive();
        }
        System.out.println("All messages acked");
        allAcked.set(true);
    }

    public static void main(String[] args) throws Exception {
//        perfectLinkMain(args);
//        BEBMain(args);
//        latticeAgreementMain(args);

//        ExecutorService executor;
        try {
            ExecutorService executor = Executors.newFixedThreadPool(3);

        } catch (Exception e) {
            System.err.println(e.getMessage());
        }

    }
}
