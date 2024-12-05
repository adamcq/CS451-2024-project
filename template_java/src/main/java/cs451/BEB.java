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
    private int numberOfMessages;
    private HashMap<Integer, AbstractMap.SimpleEntry<InetAddress, Integer>> idToAddressPort;
    private final int processId;
//    ExecutorService executor;

    // PERFECT SENDER ARGS

    int numberOfBatches;
    int MIN_WINDOW_SIZE = 1;
    private final int BATCH_SIZE = 8;
    private final int INCREMENT = 1;
    int MAX_ACK_WAIT_TIME = 200;
    private LogBuffer logBuffer;
    String outputPath;
    long messagesSent;
    long acksSent;
    DatagramSocket socket;
    enum Phase {SLOW_START, CONGESTION_AVOIDANCE}
    ReentrantLock logMutex;

    // PERFECT RECEIVER ARGS
    ConcurrentHashMap<Long, MessageAcker> toBroadcast; // (senderId, messageId) message hash to (Message, ackedSet)
    private final MemoryFriendlyBitSet urbDelivered;
    private int UDP_PACKET_SIZE = 512;
    private final int numberOfHosts;
    private AtomicLong rtt = new AtomicLong(MAX_ACK_WAIT_TIME);
    AtomicInteger ownMessagesDelivered;

    public BEB(HashMap<Integer, AbstractMap.SimpleEntry<InetAddress, Integer>> idToAddressPort, int processId, String outputPath, int numberOfMessages) {
        this.numberOfMessages = numberOfMessages;
        this.outputPath = outputPath;
        this.processId = processId;
        this.idToAddressPort = idToAddressPort;
        this.numberOfHosts = idToAddressPort.size();

        this.urbDelivered = new MemoryFriendlyBitSet(this.numberOfHosts, numberOfMessages);;

        // init log buffer
        try {
            this.logBuffer = new LogBuffer(outputPath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // init socket
        InetAddress broadcasterAddress = idToAddressPort.get(processId).getKey();
        int broadcasterPort = idToAddressPort.get(processId).getValue();

        try {
            socket = new DatagramSocket(broadcasterPort, broadcasterAddress); // this works for both sender and receiver, because we put senderId == receiverId for receiver in Main
        } catch (SocketException e) {
            System.err.println("Creating receiver socket failed. Socket is USED!!!\n" + e.getMessage());
            throw new RuntimeException(e);
        }

        // log Mutex
        logMutex = new ReentrantLock();

        // Messages to broadcast
        this.toBroadcast = new ConcurrentHashMap<>();
        // start receiver
        Thread receiverThread = new Thread(this::receive, "ReceiverThread");
        receiverThread.start();

        // start broadcast
        Thread broadcastThread = new Thread(this::broadcast, "BroadcastThread");
        broadcastThread.start();

        // add socket shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Inside Socket Shutdown Hook");
            System.out.println("Messages Sent " + messagesSent);
            System.out.println("Acks Sent " + acksSent);
            broadcastThread.interrupt();
            receiverThread.interrupt();
            if (socket != null && !socket.isClosed())
                socket.close();
        }));
    }


    /**
     * @param lastCreated the Batch Number of the last created Batch
     * @return new Message
     */
    private Message createMessage(int lastCreated) {
        int[] data = new int[Math.min(8, numberOfMessages - lastCreated * 8)];
        for (int i = 0; i < data.length; i++) {
            data[i] = lastCreated * 8 + i + 1;
        }

        return new Message(
                (byte) 0,
                processId,
                lastCreated+1,
                data,
                System.currentTimeMillis()
        );
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
        buffer.putInt(processId);
        buffer.putLong(System.currentTimeMillis()); // TODO change the broadcastTime

        byte[] sendData = buffer.array();
//        System.out.println("senData " + Arrays.toString(sendData));
        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, receiverAddress, receiverPort);

        // log & send the packet
        try {
            assert socket != null : "Broadcast Socket is null in sendBatch";
            // TODO log when creating the message (PREVIOUSLY LOGGED HERE)
            socket.send(sendPacket);
            messagesSent++;
        } catch (AssertionError e) {
            System.out.println(e.getMessage());
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Failed to send batch number " + message.getBatchNumber() + ": " + e.getMessage());
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
            System.exit(1);
        }
    }

    private void logMessage(Message message) {
        for (int number : message.getData()) {
            // log the broadcast
            try {
                logMutex.lock();
                logBuffer.log("b " + number);
            } finally {
                logMutex.unlock();
            }
        }
    }

    public void broadcast() { // call this method from main on a separate thread
        System.out.println("Broadcast called");

        // Initialize broadcast variables
        int newToAdd = 1; // number of new messages to add
        Integer lastNewAdded = 0;

        int[] ackedCount = new int[this.numberOfHosts]; // TODO the logic with ackedCount is flawed - it can be 0 also during execution
        Deque<Integer>[] batches = new ArrayDeque[this.numberOfHosts];

        for (int i = 0; i < this.numberOfHosts; i++)
            batches[i] = new ArrayDeque<>();

        Phase[] phases = new Phase[this.numberOfHosts];
        Arrays.fill(phases, Phase.SLOW_START);

        int[] windowSize = new int[this.numberOfHosts];
        Arrays.fill(windowSize, MIN_WINDOW_SIZE);

        int numberOfBatches = numberOfMessages / BATCH_SIZE;
        if (numberOfMessages % BATCH_SIZE != 0)
            numberOfBatches++;

        this.numberOfBatches = numberOfBatches;

        ownMessagesDelivered = new AtomicInteger(0); // TODO this will hold logic for how many to send
        int totalDelivered = 0;
        int broadcast_timeout = 1;

        // Broadcast server
        while (true) {
//            System.out.println("broadcast adding " + newToAdd + " messages. Last acked " + ownMessagesDelivered.get() + " own messages. toBroadcast=" + toBroadcast.toString());
            System.out.println("Broadcast log toBroadcast.size=" + toBroadcast.size() + " newToAdd=" + newToAdd + " rtt=" + rtt.get() + " lastNewAdded=" + lastNewAdded + " noBatches=" + numberOfBatches + " ownMessagesDelivered=" + ownMessagesDelivered.get());
//            System.out.println();

            // reset
//            ownMessagesDelivered.set(0); // TODO this will hold logic for how many to send

            // Generate & add newToAdd messages
            for (int i = 0; i < newToAdd; i++) {
                if (lastNewAdded == numberOfBatches)
                    break;

                Message message = createMessage(lastNewAdded++);
                logMessage(message);

                // TODO idea - have a separate queue for my own messages (treat them differently)
                toBroadcast.put(
                        MessageHashUtil.createMessageHash(message),
                        new MessageAcker(message)
                );
            }

            // Broadcast
//            int counter = 0;
            for (Map.Entry<Long, MessageAcker> entry : toBroadcast.entrySet()) {
                for (Map.Entry<Integer, AbstractMap.SimpleEntry<InetAddress, Integer>> addressPort : idToAddressPort.entrySet()){
                    if (!entry.getValue().isAcked(addressPort.getKey())) {
                        sendMessage(entry.getValue().getMessage(), addressPort.getValue().getKey(), addressPort.getValue().getValue());
//                int recommendedWindowSize = getRecommendedWindowSize(); // optional
                    }
                }
                // sleep
//                if ((++counter) %5==0) {
                    try {
                        Thread.sleep(broadcast_timeout);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
//                    counter = 0;
//                }
            }

            // TODO Logic about how many new messages to add next time based on the delivery rate
            //  also based on toBroadcast size
            //  it should not be in absolute values, because they can kill processes
            //  rather compare before vs after (periodically) - if the difference is too big - exponential decrease
            //  if it is not - additive increase
            // TODO this could be faulty

                        //simple logic to add new messages
            newToAdd = ownMessagesDelivered.get() - lastNewAdded + 1; // only send the next message if old one was delivered


//            // semi-advanced logic
//            if (ownMessagesDelivered.get() > 50) { // case sending too fast
//                newToAdd /= 2;
//            } else if (lastNewAdded == 1 && ownMessagesDelivered.get() == 0) { // case waiting for other processes to start
//                newToAdd = 0;
//            } else {
//                newToAdd += ownMessagesDelivered.get(); // regular case
//            }
        }
    }

    // RECEIVER
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

        int relayId = ((data[length - 12] & 0xFF) << 24) |
                ((data[length - 11] & 0xFF) << 16) |
                ((data[length - 10] & 0xFF) << 8) |
                (data[length - 9] & 0xFF);

        long sendTime = ((long) (data[length - 8] & 0xFF) << 56) |
                ((long) (data[length - 7] & 0xFF) << 48) |
                ((long) (data[length - 6] & 0xFF) << 40) |
                ((long) (data[length - 5] & 0xFF) << 32) |
                ((long) (data[length - 4] & 0xFF) << 24) |
                ((long) (data[length - 3] & 0xFF) << 16) |
                ((long) (data[length - 2] & 0xFF) << 8) |
                ((long) (data[length - 1] & 0xFF));

        // TODO mark batch as acked
        //  mark ACK
        //  reliably broadcast ACK
        //  if > N / 2 + 1 have ACKed, remove from batches
        if (senderId == processId && relayId != processId) { // TODO verify if this is the best strategy for rtt
            rtt.set(Math.min(MAX_ACK_WAIT_TIME, Math.max(rtt.get(), System.currentTimeMillis() - sendTime)));

        }

        long messageHash = MessageHashUtil.createMessageHash(senderId, batchNumber);
        if (!isUrbDelivered(senderId, batchNumber)) {
//            System.out.println("processing ack from " + senderId + " batch " + batchNumber);

            int numberAcked = toBroadcast.get(messageHash).addAckFrom(relayId);
//            System.out.println("messageHash " + messageHash + " decoded " + MessageHashUtil.extractSenderId(messageHash) + " " + MessageHashUtil.extractMessageNumber(messageHash) + " numberAcked " + numberAcked + " ackedSet " + toBroadcast.get(messageHash).getAcked().toString());

            // urbDeliver
            if (numberAcked > numberOfHosts / 2) {
                int[] payload = new int[(length - 21) / 4];
                for (int i = 0; i < payload.length; i++) {
                    payload[i] = ((data[i*4 + 9] & 0xFF) << 24) |
                            ((data[i*4 + 10] & 0xFF) << 16) |
                            ((data[i*4 + 11] & 0xFF) << 8) |
                            (data[i*4 + 12] & 0xFF);
                }

                try {
                    logMutex.lock();
                    markUrbDelivered(senderId, batchNumber, payload);  // Process each number directly
                } finally {
                    logMutex.unlock();
                }
            }
        }

        if (isUrbDelivered(senderId, batchNumber) && toBroadcast.containsKey(messageHash)) {
            int numberAcked = toBroadcast.get(messageHash).addAckFrom(relayId);

            // remove
            if (numberAcked == numberOfHosts) {
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

        int[] payload = new int[(length - 21) / 4];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = ((data[i*4 + 9] & 0xFF) << 24) |
                    ((data[i*4 + 10] & 0xFF) << 16) |
                    ((data[i*4 + 11] & 0xFF) << 8) |
                    (data[i*4 + 12] & 0xFF);
        }

        int relayId = ((data[length - 12] & 0xFF) << 24) |
                ((data[length - 11] & 0xFF) << 16) |
                ((data[length - 10] & 0xFF) << 8) |
                (data[length - 9] & 0xFF);

        long sendTime = ((long) (data[length - 8] & 0xFF) << 56) |
                ((long) (data[length - 7] & 0xFF) << 48) |
                ((long) (data[length - 6] & 0xFF) << 40) |
                ((long) (data[length - 5] & 0xFF) << 32) |
                ((long) (data[length - 4] & 0xFF) << 24) |
                ((long) (data[length - 3] & 0xFF) << 16) |
                ((long) (data[length - 2] & 0xFF) << 8) |
                ((long) (data[length - 1] & 0xFF));

//        System.out.println("Received type " + data[0] + " from " + senderId + " batch " + batchNumber + " payload=" + Arrays.toString(payload) + " at " + sendTime);

        long messageHash = MessageHashUtil.createMessageHash(senderId, batchNumber);
        if (!isUrbDelivered(senderId, batchNumber)) {
            toBroadcast.putIfAbsent(
                    messageHash,
                    new MessageAcker(new Message(data[0], senderId, batchNumber, payload, sendTime))
            );

            int numberAcked = toBroadcast.get(messageHash).addAckFrom(relayId);
//            System.out.println("messageHash " + messageHash + " decoded " + MessageHashUtil.extractSenderId(messageHash) + " " + MessageHashUtil.extractMessageNumber(messageHash) + " numberAcked " + numberAcked + " ackedSet " + toBroadcast.get(messageHash).getAcked().toString());

            // urbDeliver
            if (numberAcked > numberOfHosts / 2) {
                try {
                    logMutex.lock();
                    markUrbDelivered(senderId, batchNumber, payload);  // Process each number directly
                } finally {
                    logMutex.unlock();
                }
            }
        }

        if (isUrbDelivered(senderId, batchNumber) && toBroadcast.containsKey(messageHash)) {
            int numberAcked = toBroadcast.get(messageHash).addAckFrom(relayId);

            // remove
            if (numberAcked == numberOfHosts) {
                toBroadcast.remove(messageHash);
            }
        }

//
//            int numberAcked = toBroadcast.get(messageHash).addAckFrom(senderId);
//
//            // urbDeliver
//            if (numberAcked > numberOfHosts / 2) {
//                try {
//                    logMutex.lock();
//                    markUrbDelivered(senderId, batchNumber, payload);  // Process each number directly
//                } finally {
//                    logMutex.unlock();
//                }
//                // TODO remove from toBroadcast
//                toBroadcast.remove(messageHash);
//            }
//        }

        // Send ACK
        sendACK(data, length, relayAddress, relayPort);
    }

    // TODO have one receive loop
    //  this loop will receive data & acks simultaneously
    //  each message must be modified and contain a bit isAck (e.g. the first bit)
    //  if it is ack, process it on a separate thread
    //  if it is a message, process it on a separate thread
    //  broadcast loop thread and ack processing thread will share resources - timeout must be set & windowsize adjusted
    public void receive() {
        try {
            // Prepare a packet to receive data
            byte[] receiveData = new byte[UDP_PACKET_SIZE]; // TODO ask if we can assume 1024 Bytes as maximum size of a packet
            DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);

            while (true) {
                // Receive the packet
                try {
                    socket.receive(receivePacket);

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
            if (socket != null && !socket.isClosed()) {
                System.out.println("Closing socket...");
                socket.close();
            }
        }
    }

    private void sendACK(byte[] ackData, int ackLength, InetAddress relayAddress, int relayPort) { // TODO sendACK(int senderId, int batchNumber) - should be enough to identify originality
        // mark message as ACK
        ackData[0] = 1;

        // change relayId to mine
        ackData[ackLength - 12] = (byte) ((processId >> 24) & 0xFF);
        ackData[ackLength - 11] = (byte) ((processId >> 16) & 0xFF);
        ackData[ackLength - 10] = (byte) ((processId >> 8) & 0xFF);
        ackData[ackLength - 9] = (byte) (processId & 0xFF);

        // Create ACK packet to send data back to the relay's address
        DatagramPacket ackPacket = new DatagramPacket(ackData, ackLength, relayAddress, relayPort);

        try {
            socket.send(ackPacket);
            acksSent++;
        } catch (IOException e) {
            System.err.println("Failed to send ACK from relay port " + relayPort + ": " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private void markUrbDelivered(int senderId, int batchNumber, int[] payload) {
//        System.out.println("URB Delivering " + senderId + " " + batchNumber + " " + Arrays.toString(payload));
        if (!urbDelivered.isSet(senderId, batchNumber)) {
            if (processId == senderId)
                ownMessagesDelivered.getAndIncrement();
            for (int number : payload) {
                logBuffer.log("d " + senderId + " " + number);
            }
        }
        urbDelivered.set(senderId, batchNumber);
    }
    private boolean isUrbDelivered(int senderId, int batchNumber) {
        boolean isSet;
        try {
            logMutex.lock();
            isSet = urbDelivered.isSet(senderId, batchNumber);
        } finally {
            logMutex.unlock();
        }
        return isSet;
    }
//    private void markDelivered(int senderId, int messageNumber) {
//        if (!delivered.isSet(senderId, messageNumber)) {
//            try {
//                logMutex.lock();
//                logBuffer.log("d " + senderId + " " + messageNumber);
//            } finally {
//                logMutex.unlock();
//            }
//            delivered.set(senderId, messageNumber);
//        }
//    }
}
