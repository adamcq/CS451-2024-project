package cs451;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TestLink {
    private final DatagramSocket receiveSocket;
    private final DatagramSocket sendSocket;
    private final DatagramSocket sendAckSocket;
    ExecutorService sendAckExecutor = Executors.newSingleThreadExecutor();
    ExecutorService sendMessageExecutor;
    ExecutorService handleReceivedExecutor;
    int numberOfHosts;
    int hostId;
    int receivedCounter = 0;
    int receivedAcks = 0;
    int receivedMessages = 0;
    int sentMessages = 0;
    int sentAcks = 0;
    ByteBuffer buffer; // TODO verify if multithreading doesn't break this
    int sentCounter = 0;
    //    int SEND_WINDOW_SIZE = 100;
//    BitSet acked = new BitSet();
//    int startSendFrom = 0;
    BebState bebState;
    RunConfig runConfig;
    int UDP_PACKET_SIZE = 512;
    BlockingQueue<DatagramPacket> taskQueue;

    public TestLink(RunConfig runConfig, BebState bebState) {
        this.receiveSocket = runConfig.getSocket();
//        sendAckExecutor = Executors.newFixedThreadPool(1);
//        sendMessageExecutor = Executors.newFixedThreadPool(1);
        handleReceivedExecutor = Executors.newFixedThreadPool(3);
        this.bebState = bebState;
        this.runConfig = runConfig;

        try {
            this.sendSocket = new DatagramSocket();
            this.sendAckSocket = new DatagramSocket();

            System.out.println("Socket at port " + runConfig.getSocket().getLocalPort());
            System.out.println("SendSocket at port " + sendSocket.getLocalPort());
            System.out.println("SendAckSocket at port " + sendSocket.getLocalPort());
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }

        this.numberOfHosts = runConfig.getNumberOfHosts();
        this.hostId = runConfig.getProcessId();
        this.buffer = ByteBuffer.allocate(UDP_PACKET_SIZE);

        int queueCapacity = 5;
        taskQueue = new LinkedBlockingQueue<>(queueCapacity);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//            int dstId = hostId == 0 ? 1 : 0;
            System.out.println("Received TOTAL=" + receivedCounter + " ACKS=" + receivedAcks + " MESSAGES=" + receivedMessages);
            System.out.println("Sent TOTAL=" + sentCounter + " ACKS=" + sentAcks + " MESSAGES=" + sentMessages);
            for (int dstId = 1; dstId <= numberOfHosts; dstId++) {
                System.out.println("From " + dstId + " received " + bebState.getAcked()[dstId - 1].cardinality() + " all till " + bebState.getStartSendFrom()[dstId - 1] + (dstId == hostId ? " <- SELF" : ""));
//                System.out.println("Double check: " + bebState.getAcked()[dstId].nextClearBit(0));
            }
        }));
    }

    public boolean send(int number, int dstId) {

//        sentMessages++;
        buffer.put((byte) 0);
        buffer.putInt(hostId);
        buffer.putInt(number);


        byte[] data = buffer.array();
        buffer.clear();

        return send(data, dstId);
//        sendMessageExecutor.submit(
//                () -> send(data, port)
//        );
    }

    private boolean send(byte[] data, int dstId) {
        /* direct send logic */
//        DatagramPacket packet = new DatagramPacket(data, data.length, runConfig.getIdToAddressPort().get(dstId).getKey(), runConfig.getIdToAddressPort().get(dstId).getValue());
        try {
            sentMessages++;
//                runConfig.getSocket().send(taskQueue.take());
//            System.out.println();
            sendSocket.send(new DatagramPacket(data, data.length, runConfig.getIdToAddressPort().get(dstId).getKey(), runConfig.getIdToAddressPort().get(dstId).getValue()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return true;

        /* add to taskQueue logic */
//        boolean success = false;
//        try {
//            success = taskQueue.offer(new DatagramPacket(data, data.length, runConfig.getIdToAddressPort().get(dstId).getKey(), runConfig.getIdToAddressPort().get(dstId).getValue()));
//            if (success)
//                sentCounter++;
//        } catch (Exception e) {
//            System.err.println(e.getMessage());
//        }
//        return success;
    }

    public void processTasks() {
        while (true) {
            try {
                sentMessages++;
//                runConfig.getSocket().send(taskQueue.take());
                sendSocket.send(taskQueue.take());
//                System.out.println("Sent: " + ByteBuffer.wrap(packet.getData()).getInt(1) + " " + ByteBuffer.wrap(packet.getData()).getInt(5));
//                System.out.println("TaskQueue sizeL " + taskQueue.size());
            } catch (InterruptedException | IOException e) {
                throw new RuntimeException(e);
            }
        }
    }


    public void receive() {
        System.out.println("Receiver active");

        byte[] receiveData = new byte[UDP_PACKET_SIZE]; // TODO ask if we can assume 1024 Bytes as maximum size of a packet
        DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);

        while (true) {
            // Receive the packet
            try {
                receiveSocket.receive(receivePacket);

                byte[] data = receivePacket.getData();
                int length = receivePacket.getLength();

                final int receivedCounterCopy = receivedCounter++;
                /* debug logs */
//                System.out.println("Received at " + System.currentTimeMillis());
//                ThreadPoolExecutor executor = (ThreadPoolExecutor) handleReceivedExecutor;
//                System.out.println("Received at " + System.currentTimeMillis() + " handleReceivedExecutor task queue size = " + executor.getQueue().size());

                if (data[0] == (byte) 0) {
//                    handleReceivedExecutor.submit(
//                            () -> handleMessage(data.clone(), length, receivePacket.getPort(), receivedCounterCopy)
//                    );
                    handleMessage(data.clone(), length, receivePacket.getPort(), receivedCounterCopy);
                    receivedMessages++;
                } else if (data[0] == (byte) 1) {
                    receivedAcks++;
//                    handleReceivedExecutor.submit(
//                            () -> handleAck(data.clone(), length, receivePacket.getPort(), receivedCounterCopy)
//                    );
                    handleAck(data.clone(), length, receivePacket.getPort(), receivedCounterCopy);
                } else {
                    System.out.println("WHAAAATT DID YOU SEEEEEEEEND OMGGGG");
                }
            } catch (SocketTimeoutException e) {
                System.out.println("Broadcast again");
//                singleBebBroadcast();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void handleMessage(byte[] data, int length, int port, int receivedCounter) {
//        System.out.println("Received from " + ByteBuffer.wrap(data).getInt(1) + " number " + ByteBuffer.wrap(data).getInt(5));
        sendAck(data, length, port, receivedCounter);
//        sendAckExecutor.submit(
//            () -> {
//                try {
//                    sendAck(data.clone(), length, port, receivedCounter);
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                }
//            }
//        );
    }

    private void handleAck(byte[] data, int length, int port, int receivedCounter) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
//        byte messageType = buffer.get();
        int senderId = buffer.getInt(1);
        int numberReceived = buffer.getInt(5);

//        System.out.println("Received ACK from " + senderId + " " + numberReceived);

        bebState.getAcked()[senderId - 1].set(numberReceived);
        bebState.getStartSendFrom()[senderId - 1] = bebState.getAcked()[senderId - 1].nextClearBit(bebState.getStartSendFrom()[senderId - 1]);
    }

    private void sendAck(byte[] data, int length, int port, int receivedCounter) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int dstId = buffer.getInt(1);
        buffer.put((byte) 1);
        buffer.putInt(hostId);

        DatagramPacket ackPacket = new DatagramPacket(data, data.length, runConfig.getIdToAddressPort().get(dstId).getKey(), runConfig.getIdToAddressPort().get(dstId).getValue());

        try {
            sendAckSocket.send(ackPacket);
            sentAcks++;
            sentCounter++;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void singleBebBroadcast() {
        int updateSelfUntil = -1;
        for (int dstId = 1; dstId <= this.numberOfHosts; dstId++) {
            if (dstId == this.hostId)
                continue;
            int counter = 0;
            int number = bebState.getAcked()[dstId - 1].nextClearBit(bebState.getStartSendFrom()[dstId - 1]);
            while (counter < bebState.getSendWindowSize()[dstId - 1]) {
                number = bebState.getAcked()[dstId - 1].nextClearBit(number);
                if (number > this.runConfig.getNumberOfMessages()) {
                    break;
                }
                sentMessages++;
                this.send(number++, dstId);
                counter++;
            }
            updateSelfUntil = Math.max(updateSelfUntil, number);
        }
        if (updateSelfUntil >= bebState.getAcked()[this.hostId - 1].nextClearBit(bebState.getStartSendFrom()[this.hostId - 1])) {
            bebState.getAcked()[this.hostId - 1].set(bebState.getStartSendFrom()[this.hostId - 1], updateSelfUntil);
            bebState.getStartSendFrom()[this.hostId - 1] = updateSelfUntil;
        }
    }
}


