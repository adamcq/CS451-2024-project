package cs451;

import cs451.Message.LatticeMessage;
import cs451.Message.Message;

import java.awt.*;
import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
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
    ProposerState proposerState;
    LatticeRunConfig runConfig;
    int UDP_PACKET_SIZE = 4096;
    BlockingQueue<DatagramPacket> taskQueue;
    AcceptorState acceptorState;

    public LatticeLink(LatticeRunConfig runConfig, ProposerState proposerState, AcceptorState acceptorState) {
        this.receiveSocket = runConfig.getSocket();
        this.proposerState = proposerState;
        this.acceptorState = acceptorState;
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
//        System.out.println("handlemsg " + msg + "acceptor=" + acceptorState.getAcceptedValue());
        if (acceptorState.containsProposedValue(msg.getProposalValue())) {
            sendAck(msg);
            System.out.println("VAL (ACK) received " + msg.getProposalValue() + " msg " + msg);
            System.out.println("VAL (ACK) had " + acceptorState.getAcceptedValue() + " msg " + msg);
//            try {
//                Thread.sleep(100);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
        } else {
            System.out.println("VAL (NACK) received " + msg.getProposalValue() + " msg " + msg);
            System.out.println("VAL (NACK) had " + acceptorState.getAcceptedValue() + " msg " + msg);
//            try {
//                Thread.sleep(100);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
            acceptorState.addToAccepted(msg.getProposalValue());
            msg.setProposalValue(acceptorState.getAcceptedValue());
            msg.setSetSize(msg.getProposalValue().size());
            sendNack(msg);
//            System.out.println("handlemsg " + msg + "acceptor=" + acceptorState.getAcceptedValue() + "NACK");
        }
    }

    private void handleAck(byte[] data) {
        LatticeMessage msg = LatticeMessage.deserialize(data);
        System.out.println("ACK received " + msg + " acked " + proposerState.getAcked() + " nacked " + proposerState.getNacked());
        proposerState.ackReceived(msg);
    }

    private void handleNack(byte[] data) {
        LatticeMessage msg = LatticeMessage.deserialize(data);
        System.out.println("NACK received " + msg + " acked " + proposerState.getAcked() + " nacked " + proposerState.getNacked());
        proposerState.nackReceived(msg);
    }

    private void sendAck(LatticeMessage msg) {
        msg.setMessageType((byte) 1);
        msg.setRelayId(runConfig.getProcessId());
        byte[] data = msg.serialize();

        DatagramPacket ackPacket = new DatagramPacket(data, data.length, runConfig.getIdToAddressPort().get(msg.getSenderId()).getKey(), runConfig.getIdToAddressPort().get(msg.getSenderId()).getValue());

        try {
            sendAckSocket.send(ackPacket);
            System.out.println("ACK sent " + msg + " acked " + proposerState.getAcked() + " nacked " + proposerState.getNacked());
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
            System.out.println("NACK sent" + msg + " acked " + proposerState.getAcked() + " nacked " + proposerState.getNacked());

            sentNacks++;
            sentCounter++;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}


