package cs451;

import cs451.Message.Message;
import cs451.Message.MessageAcker;
import cs451.Message.MessageHashUtil;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class BEB {
    // BEB ARGS
    private final RunConfig runConfig;

    int numberOfBatches;
    long messagesSent;
    ReentrantLock logMutex;
    ConcurrentHashMap<Long, MessageAcker> toBroadcast; // (senderId, messageId) message hash to (Message, ackedSet)
    AtomicInteger maxSeenMessage;

    public BEB(RunConfig runConfig) {
        this.runConfig = runConfig;

        this.logMutex = new ReentrantLock();
        this.toBroadcast = new ConcurrentHashMap<>();
        this.maxSeenMessage = new AtomicInteger(0);

        // add DEBUG shutdown hook TODO remove this
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Inside Socket Shutdown Hook");
            System.out.println("Messages Sent " + messagesSent);
        }));
    }

    public void receive() {
        new FifoReceiver(runConfig, toBroadcast, logMutex, maxSeenMessage).receive();
    }

    private void sendMessage(Message message, InetAddress receiverAddress, int receiverPort) {
        // Prepare the packet
        ByteBuffer buffer = ByteBuffer.allocate(message.getMessageSize()); // boolean, integer, integer, string payload, long time

        buffer.put(message.getMessageType()); // 0 meaning it is NOT an ACK
        buffer.putInt(message.getSenderId());
        buffer.putInt(message.getBatchNumber());
        int[] data = message.getData();
        for (int i = 0; i < data.length; i++)
            buffer.putInt(data[i]);
        buffer.putInt(runConfig.getProcessId());

        byte[] sendData = buffer.array();
//        System.out.println("senData " + Arrays.toString(sendData));
        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, receiverAddress, receiverPort);

        // log & send the packet
        try {
            assert runConfig.getSocket() != null : "Broadcast Socket is null in sendBatch";
            runConfig.getSocket().send(sendPacket);
        } catch (AssertionError e) {
            System.out.println(e.getMessage());
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Failed to send batch number " + message.getBatchNumber() + ": " + e.getMessage());
            if (runConfig.getSocket() != null && !runConfig.getSocket().isClosed()) {
                runConfig.getSocket().close();
            }
            System.exit(1);
        }
    }

    private void logMessage(Message message) {
        for (int number : message.getData()) {
            try {
                logMutex.lock();
                runConfig.getLogBuffer().log("b " + number);
            } finally {
                logMutex.unlock();
            }
        }
    }

    public void broadcast() { // call this method from main on a separate thread
        System.out.println("Broadcast called");

        // Initialize broadcast variables
        int newToAdd = 1; // number of new messages to add
        int lastNewAdded = 0;

        int numberOfBatches = runConfig.getNumberOfMessages() / Constants.BATCH_SIZE;
        if (runConfig.getNumberOfMessages() % Constants.BATCH_SIZE != 0)
            numberOfBatches++;

        this.numberOfBatches = numberOfBatches;
        int broadcast_timeout = 1;

        // Broadcast server
        while (true) {
//            System.out.println("Broadcast time=" + System.currentTimeMillis() + " toBroadcast.size=" + toBroadcast.size() + " newToAdd=" + newToAdd + " lastNewAdded=" + lastNewAdded + " messagesSent=" + messagesSent);

//            System.out.println("broadcast adding " + newToAdd + " messages. Last acked " + ownMessagesDelivered.get() + " own messages. toBroadcast=" + toBroadcast.toString());
//            System.out.println();

            // Generate & add newToAdd
            // messages
            for (int i = 0; i < newToAdd; i++) {
                if (lastNewAdded == numberOfBatches)
                    break;

                Message message = Message.createMessage(lastNewAdded++, runConfig.getNumberOfMessages(), runConfig.getProcessId());
                logMessage(message);

                // TODO idea - have a separate queue for my own messages (treat them differently)
                toBroadcast.put(
                        MessageHashUtil.createMessageHash(message),
                        new MessageAcker(message, runConfig)
                );
            }

            // Broadcast
//            int counter = 0;
            for (Map.Entry<Long, MessageAcker> entry : toBroadcast.entrySet()) {
                for (Map.Entry<Integer, AbstractMap.SimpleEntry<InetAddress, Integer>> addressPort : runConfig.getIdToAddressPort().entrySet()){
                    if (!entry.getValue().isAcked(addressPort.getKey())) {
                        sendMessage(entry.getValue().getMessage(), addressPort.getValue().getKey(), addressPort.getValue().getValue());
                        messagesSent++;
                    }
                }
                // sleep
//                if ((++counter) %5==0) {
//                    counter = 0;
//                }
            }
            try {
//                        Thread.sleep(broadcast_timeout);
                Thread.sleep(0,10000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            //simple logic to add new messages
            newToAdd = maxSeenMessage.get() - lastNewAdded + 4; // only send the next message if old one was delivered

        }
    }
}
