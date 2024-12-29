package cs451;

import cs451.Message.LatticeMessage;
import cs451.Message.Message;

import java.awt.*;
import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.*;

public class LatticeLink {
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
    int receivedNacks = 0;
    int receivedMessages = 0;
    int sentMessages = 0;
    int sentAcks = 0;
    int sentNacks = 0;
    ByteBuffer buffer; // TODO verify if multithreading doesn't break this
    int sentCounter = 0;
    //    int SEND_WINDOW_SIZE = 100;
//    BitSet acked = new BitSet();
//    int startSendFrom = 0;
    LatticeRunConfig runConfig;
    int UDP_PACKET_SIZE = 4096;
    BlockingQueue<DatagramPacket> taskQueue;
    LatticeState latticeState;

    public LatticeLink(LatticeRunConfig runConfig, LatticeState latticeState) {
        this.receiveSocket = runConfig.getSocket();
        this.runConfig = runConfig;
        this.latticeState = latticeState;

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
        }));
    }

    public boolean send(int number, int dstId) {
        buffer.put((byte) 0);
        buffer.putInt(hostId);
        buffer.putInt(number);


        byte[] data = buffer.array();
        buffer.clear();

        return send(data, dstId);
    }

    public boolean send(byte[] data, int dstId) {
        try {
            sentMessages++;
            sendSocket.send(new DatagramPacket(data, data.length, runConfig.getIdToAddressPort().get(dstId).getKey(), runConfig.getIdToAddressPort().get(dstId).getValue()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return true;
    }



    public void receive() {
        System.out.println("Receiver active");

        byte[] receiveData = new byte[UDP_PACKET_SIZE]; // TODO ask if we can assume 1024 Bytes as maximum size of a packet
        DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);

        while (true) {
            try {
                receiveSocket.receive(receivePacket);

                byte[] data = receivePacket.getData();
                int length = receivePacket.getLength();

//                System.out.println("Received MSG " + LatticeMessage.deserialize(data));

                receivedCounter++;

                if (data[0] == (byte) 0) {
                    handleMessage(data.clone());
                    receivedMessages++;
                } else if (data[0] == (byte) 1) {
                    receivedAcks++;
                    handleAck(data.clone());
                } else if (data[0] == (byte) 2) {
                    receivedNacks++;
                    handleNack(data.clone());
                } else {
                    System.out.println("WHAAAATT DID YOU SEEEEEEEEND OMGGGG");
                }
            } catch (SocketTimeoutException e) {
                System.out.println("Broadcast again");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void handleMessage(byte[] data) {
        LatticeMessage msg = LatticeMessage.deserialize(data);
        msg.setRelayId(runConfig.getProcessId());
        System.out.println("handleMESSAGE !latticeState.iterationReached(msg.getIteration()) || latticeState.iterationComplete(msg.getIteration()) = " + (!latticeState.iterationReached(msg.getIteration())) + " " + latticeState.iterationComplete(msg.getIteration()) + " maxIterSeen=" + latticeState.maxIteration + " currentIter="+msg.getIteration());

        if (!latticeState.iterationReached(msg.getIteration()) || latticeState.iterationComplete(msg.getIteration()))
            return;

        // TODO if the first time you see the (iteration, senderId, proposalNumber) - store it in acceptor toNack
        //  if the 2nd time you see (iteration, senderId, proposalNumber) & it is in

        AcceptorState acceptorState = latticeState.acceptorStateMap.get(msg.getIteration());
        System.out.println("handlemsg " + " data[0]=" + data[0] + " msg=" + msg + "acceptor=" + latticeState.acceptorStateMap.get(msg.getIteration()).getAcceptedValue() + "toNack=" + acceptorState.getToNack() + "maxPropNumFrom=" + Arrays.toString(acceptorState.getMaxSeenProposalNumberFromAll()));

        // reset toNack set if the new msg.proposalNumber is bigger than anything seen before
        if (acceptorState.getMaxSeenProposalNumberFrom(msg.getSenderId()) < msg.getProposalNumber()) {
            System.out.println("RESETTING toNack iteration " + msg.getIteration() + " senderId " + msg.getSenderId() + " from " + acceptorState.getMaxSeenProposalNumberFrom(msg.getSenderId()) + " to " + msg.getProposalNumber() + " resetting toNack from " + acceptorState.getToNack() + " to {}");
            acceptorState.resetToNack(msg.getSenderId());
            acceptorState.setMaxSeenProposalNumberFrom(msg.getProposalNumber(), msg.getSenderId());
        }

        if (latticeState.containsProposedValue(msg) && !acceptorState.getToNack().contains((msg.getSenderId()))) {
            sendAck(msg);
            System.out.println("VAL (ACK) received " + msg.getProposalValue() + " msg " + msg);
            System.out.println("VAL (ACK) had " + acceptorState.getAcceptedValue() + " msg " + msg);
        } else {
            acceptorState.addToNack((msg.getSenderId()));
            System.out.println("VAL (NACK) received " + msg.getProposalValue() + " msg " + msg);
            System.out.println("VAL (NACK) had " + acceptorState.getAcceptedValue() + " msg " + msg);
            latticeState.addToAccepted(msg);
            msg.setProposalValue(latticeState.getAcceptedValue(msg));
            msg.setSetSize(msg.getProposalValue().size());
            sendNack(msg);
            System.out.println("handlemsg " + msg + "acceptor=" + acceptorState.getAcceptedValue() + "NACK");
        }
    }

    private void handleAck(byte[] data) {
        LatticeMessage msg = LatticeMessage.deserialize(data);
        System.out.println("handleACK !latticeState.iterationReached(msg.getIteration()) || latticeState.iterationComplete(msg.getIteration()) = " + (!latticeState.iterationReached(msg.getIteration())) + " " + latticeState.iterationComplete(msg.getIteration()));
        if (!latticeState.iterationReached(msg.getIteration()) || latticeState.iterationComplete(msg.getIteration()))
            return;
        System.out.println("ACK received " + msg + " acked " + latticeState.proposerStateMap.get(msg.getIteration()).getAcked() + " nacked " + latticeState.proposerStateMap.get(msg.getIteration()).getNacked());
        latticeState.ackReceived(msg);
    }

    private void handleNack(byte[] data) {
        LatticeMessage msg = LatticeMessage.deserialize(data);
        System.out.println("handleNACK !latticeState.iterationReached(msg.getIteration()) || latticeState.iterationComplete(msg.getIteration()) = " + (!latticeState.iterationReached(msg.getIteration())) + " " + latticeState.iterationComplete(msg.getIteration()));
        if (!latticeState.iterationReached(msg.getIteration()) || latticeState.iterationComplete(msg.getIteration()))
            return;
        System.out.println("NACK received " + msg + " acked " + latticeState.proposerStateMap.get(msg.getIteration()).getAcked() + " nacked " + latticeState.proposerStateMap.get(msg.getIteration()).getNacked());
        latticeState.nackReceived(msg);
    }

    private void sendAck(LatticeMessage msg) {
        msg.setMessageType((byte) 1);
        msg.setRelayId(runConfig.getProcessId());
        byte[] data = msg.serialize();

        DatagramPacket ackPacket = new DatagramPacket(data, data.length, runConfig.getIdToAddressPort().get(msg.getSenderId()).getKey(), runConfig.getIdToAddressPort().get(msg.getSenderId()).getValue());

        try {
            sendAckSocket.send(ackPacket);
            System.out.println("ACK sent " + msg + " acked " + latticeState.proposerStateMap.get(msg.getIteration()).getAcked() + " nacked " + latticeState.proposerStateMap.get(msg.getIteration()).getNacked());
            sentAcks++;
            sentCounter++;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void sendNack(LatticeMessage msg) {
        msg.setMessageType((byte) 2);
        msg.setRelayId(runConfig.getProcessId());
        byte[] data = msg.serialize();

        DatagramPacket nackPacket = new DatagramPacket(data, data.length, runConfig.getIdToAddressPort().get(msg.getSenderId()).getKey(), runConfig.getIdToAddressPort().get(msg.getSenderId()).getValue());

        try {
            sendAckSocket.send(nackPacket);
            System.out.println("NACK sent" + msg + " acked " + latticeState.proposerStateMap.get(msg.getIteration()).getAcked() + " nacked " + latticeState.proposerStateMap.get(msg.getIteration()).getNacked());

            sentNacks++;
            sentCounter++;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}


