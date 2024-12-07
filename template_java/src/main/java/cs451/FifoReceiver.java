package cs451;

import cs451.Message.Message;
import cs451.Message.MessageAcker;
import cs451.Message.MessageHashUtil;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class FifoReceiver {
    private final RunConfig runConfig;
    private final ConcurrentHashMap<Long, MessageAcker> toBroadcast;
    private final ReentrantLock logMutex;
    private final AtomicInteger maxSeenMessage;
    private final MemoryFriendlyBitSet urbDelivered;
    private final HashMap<Long, int[]> waitingToBeDelivered;
    private final int[] nextBatchToDeliver;
    private long acksSent;
    private long messagesReceived;

    public FifoReceiver(RunConfig runConfig, ConcurrentHashMap<Long, MessageAcker> toBroadcast, ReentrantLock logMutex, AtomicInteger maxSeenMessage) {
        this.runConfig = runConfig;
        this.toBroadcast = toBroadcast;
        this.logMutex = logMutex;
        this.maxSeenMessage = maxSeenMessage;
        waitingToBeDelivered = new HashMap<>();
        nextBatchToDeliver = new int[runConfig.getNumberOfHosts()];
        Arrays.fill(nextBatchToDeliver, 1);

        this.urbDelivered = new MemoryFriendlyBitSet(runConfig.getNumberOfHosts(), runConfig.getNumberOfMessages());
        // add DEBUG shutdown hook TODO remove this
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Inside Receiver Shutdown Hook");
            System.out.println("Acks Sent " + acksSent);
            System.out.println("Messages Received " + messagesReceived);
        }));
    }

    public void receive() {
        try {
            // Prepare a packet to receive data
            byte[] receiveData = new byte[Constants.UDP_PACKET_SIZE]; // TODO ask if we can assume 1024 Bytes as maximum size of a packet
            DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);

            while (true) {
                // Receive the packet
                try {
                    runConfig.getSocket().receive(receivePacket);
                    messagesReceived++;

                    byte[] data = receivePacket.getData();
                    int length = receivePacket.getLength();
                    InetAddress relayAddress = receivePacket.getAddress();
                    int relayPort = receivePacket.getPort();

                    byte payloadType = data[0];

                    if (payloadType == (byte) 0)
                        handleMessage(data, length, relayAddress, relayPort); // TODO perhaps each on separate thread
                    else
                        handleAck(data, length, relayAddress, relayPort);

                } catch (SocketTimeoutException e) { // TODO this should not exist
                    System.out.println("Receiver time out exception! Ignoring it and restarting the loop");
                }
            }
        } catch (Exception e) {
            System.err.println("Error in receive loop: " + e.getMessage());
            e.printStackTrace();
            System.exit(2);
        }
        finally {
            if (runConfig.getSocket() != null && !runConfig.getSocket().isClosed()) {
                System.out.println("Closing socket...");
                runConfig.getSocket().close();
            }
        }
    }

    private void sendACK(byte[] ackData, int ackLength, InetAddress relayAddress, int relayPort) { // TODO sendACK(int senderId, int batchNumber) - should be enough to identify originality
        // mark message as ACK
        ackData[0] = 1;

        // change relayId to mine
        ackData[ackLength - 4] = (byte) ((runConfig.getProcessId() >> 24) & 0xFF);
        ackData[ackLength - 3] = (byte) ((runConfig.getProcessId() >> 16) & 0xFF);
        ackData[ackLength - 2] = (byte) ((runConfig.getProcessId() >> 8) & 0xFF);
        ackData[ackLength - 1] = (byte) (runConfig.getProcessId() & 0xFF);

        // Create ACK packet to send data back to the relay's address
        DatagramPacket ackPacket = new DatagramPacket(ackData, ackLength, relayAddress, relayPort);

        try {
            runConfig.getSocket().send(ackPacket);
            acksSent++;
        } catch (IOException e) {
            System.err.println("Failed to send ACK from relay port " + relayPort + ": " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private void handleAck(byte[] data, int length, InetAddress relayAddress, int relayPort) {
//        System.out.println("Received ACK");
        // Extract data
        int senderId = ((data[1] & 0xFF) << 24) |
                ((data[2] & 0xFF) << 16) |
                ((data[3] & 0xFF) << 8) |
                (data[4] & 0xFF);

        int batchNumber = ((data[5] & 0xFF) << 24) |
                ((data[6] & 0xFF) << 16) |
                ((data[7] & 0xFF) << 8) |
                (data[8] & 0xFF);

//        if (batchNumber > maxSeenMessage.get())
//            maxSeenMessage.set(batchNumber);

        int relayId = ((data[length - 4] & 0xFF) << 24) |
                ((data[length - 3] & 0xFF) << 16) |
                ((data[length - 2] & 0xFF) << 8) |
                (data[length - 1] & 0xFF);

        long messageHash = MessageHashUtil.createMessageHash(senderId, batchNumber);
        if (!isUrbDelivered(senderId, batchNumber)) {
//            System.out.println("processing ack from " + senderId + " batch " + batchNumber);

            int numberAcked = toBroadcast.get(messageHash).addAckFrom(relayId);
//            System.out.println("messageHash " + messageHash + " decoded " + MessageHashUtil.extractSenderId(messageHash) + " " + MessageHashUtil.extractMessageNumber(messageHash) + " numberAcked " + numberAcked + " ackedSet " + toBroadcast.get(messageHash).getAcked().toString());

            // urbDeliver
            if (numberAcked > runConfig.getNumberOfHosts() / 2) {
                int[] payload = new int[(length - 13) / 4];
                for (int i = 0; i < payload.length; i++) {
                    payload[i] = ((data[i*4 + 9] & 0xFF) << 24) |
                            ((data[i*4 + 10] & 0xFF) << 16) |
                            ((data[i*4 + 11] & 0xFF) << 8) |
                            (data[i*4 + 12] & 0xFF);
                }
//                System.out.println("Received type " + 1 + " from " + senderId + " batch " + batchNumber + " payload=" + Arrays.toString(payload) + " relayer " + relayId);

//                System.out.println("MarkUrbDel ACK " + 1 + " from " + senderId + " batch " + batchNumber + " payload=" + Arrays.toString(payload) + " relayer " + relayId);
                markUrbDelivered(senderId, batchNumber, messageHash, payload);  // Process each number directly
            }
        }

        if (isUrbDelivered(senderId, batchNumber) && toBroadcast.containsKey(messageHash)) {
            int numberAcked = toBroadcast.get(messageHash).addAckFrom(relayId);
//            int numberAcked = setBitAndGetCount(ackedFrom, relayId);

            // remove
            if (numberAcked == runConfig.getNumberOfHosts()) {
                toBroadcast.remove(messageHash);
            }
        }
    }

    private void handleMessage(byte[] data, int length, InetAddress relayAddress, int relayPort) {
//        System.out.println("Received Message");
        // Extract data
        int senderId = ((data[1] & 0xFF) << 24) |
                ((data[2] & 0xFF) << 16) |
                ((data[3] & 0xFF) << 8) |
                (data[4] & 0xFF);

        int batchNumber = ((data[5] & 0xFF) << 24) |
                ((data[6] & 0xFF) << 16) |
                ((data[7] & 0xFF) << 8) |
                (data[8] & 0xFF);

        if (batchNumber > maxSeenMessage.get())
            maxSeenMessage.set(batchNumber);

        int[] payload = new int[(length - 13) / 4];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = ((data[i*4 + 9] & 0xFF) << 24) |
                    ((data[i*4 + 10] & 0xFF) << 16) |
                    ((data[i*4 + 11] & 0xFF) << 8) |
                    (data[i*4 + 12] & 0xFF);
        }

        int relayId = ((data[length - 4] & 0xFF) << 24) |
                ((data[length - 3] & 0xFF) << 16) |
                ((data[length - 2] & 0xFF) << 8) |
                (data[length - 1] & 0xFF);

//        System.out.println("Received type " + data[0] + " from " + senderId + " batch " + batchNumber + " payload=" + Arrays.toString(payload) + " relayer " + relayId);

        long messageHash = MessageHashUtil.createMessageHash(senderId, batchNumber);
        if (!isUrbDelivered(senderId, batchNumber)) {
            toBroadcast.putIfAbsent(
                    messageHash,
                    new MessageAcker(new Message(data[0], senderId, batchNumber, payload), runConfig)
            );
            int numberAcked = toBroadcast.get(messageHash).addAckFrom(relayId);
//            int numberAcked = toBroadcast.get(messageHash).addAckFrom(relayId);
//            System.out.println("messageHash " + messageHash + " decoded " + MessageHashUtil.extractSenderId(messageHash) + " " + MessageHashUtil.extractMessageNumber(messageHash) + " numberAcked " + numberAcked + " ackedSet " + toBroadcast.get(messageHash).getAcked().toString());

            // urbDeliver
            if (numberAcked > runConfig.getNumberOfHosts() / 2) {
//                System.out.println("MarkUrbDel MES " + data[0] + " from " + senderId + " batch " + batchNumber + " payload=" + Arrays.toString(payload) + " relayer " + relayId);
                markUrbDelivered(senderId, batchNumber, messageHash, payload);  // Process each number directly
            }
        }

        if (isUrbDelivered(senderId, batchNumber) && toBroadcast.containsKey(messageHash)) {
            int numberAcked = toBroadcast.get(messageHash).addAckFrom(relayId);

            // remove
            if (numberAcked == runConfig.getNumberOfHosts()) {
                toBroadcast.remove(messageHash);
            }
        }

        // Send ACK
        sendACK(data, length, relayAddress, relayPort);
    }


    private void markUrbDelivered(int senderId, int batchNumber, long messageHash, int[] payload) {
//        System.out.println("URB Delivering " + senderId + " " + batchNumber + " " + Arrays.toString(payload));
        if (!urbDelivered.isSet(senderId, batchNumber)) {
            urbDelivered.set(senderId, batchNumber);

            // FIFO deliver logic
            if (batchNumber != nextBatchToDeliver[senderId - 1]) {
                waitingToBeDelivered.put(messageHash, payload);
            }
            else {
                try {
                    // log this
                    logMutex.lock();
                    for (int number : payload) {
                        runConfig.getLogBuffer().log("d " + senderId + " " + number);
                    }
                    waitingToBeDelivered.remove(messageHash);
                    nextBatchToDeliver[senderId - 1]++;

                    // log all waiting
                    while (true) {
                        messageHash = MessageHashUtil.createMessageHash(senderId, nextBatchToDeliver[senderId - 1]);
                        if (!waitingToBeDelivered.containsKey(messageHash))
                            break;
                        payload = waitingToBeDelivered.get(messageHash);
                        for (int number : payload) {
//                            System.out.println("Delivering from " + senderId + " batch " + batchNumber + " payload=" + Arrays.toString(payload) );
                            runConfig.getLogBuffer().log("d " + senderId + " " + number);
                        }
                        waitingToBeDelivered.remove(messageHash);
                        nextBatchToDeliver[senderId - 1]++;
                    }
                } finally {
                    logMutex.unlock();
                }
            }
        }
    }
    private boolean isUrbDelivered(int senderId, int batchNumber) {
        return urbDelivered.isSet(senderId, batchNumber);
    }
}
