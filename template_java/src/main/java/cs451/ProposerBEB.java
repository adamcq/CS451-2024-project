package cs451;

import cs451.Message.LatticeMessage;

import java.net.SocketException;
import java.util.HashSet;
import java.util.Set;

public class ProposerBEB {

    cs451.LatticeLink latticeLink;
    int sentMessages;
    int sentAcks;
    int sentTotal;
    int receivedAcks;
    int receivedMessages;
    int receivedTotal;
    ProposerState proposerState;
    int broadcastTotal;
    LatticeRunConfig runConfig;
    private volatile boolean stopBroadcast = false; // Flag to stop the broadcast loop



    public ProposerBEB(LatticeLink latticeLink, ProposerState proposerState, LatticeRunConfig runConfig) throws SocketException {
        this.runConfig = runConfig;
        this.latticeLink = latticeLink;
        this.proposerState = proposerState;

//        testLink.runConfig.getSocket().setSoTimeout(100);
    }

    public void bebBroadcast(MessageType messageType, Set<Integer> proposerValue, int activeProposalNumber) {
        System.out.println("BEB broadcast called with: " + MessageType.PROPOSAL + " " + proposerState.getProposedValue() + " " + proposerState.getActiveProposalNumber());
        LatticeMessage msg = new LatticeMessage((byte) 0, runConfig.getProcessId(), runConfig.getProcessId(), activeProposalNumber, proposerValue.size(), proposerValue);
        byte[] data = msg.serialize();

        while (!Thread.currentThread().isInterrupted()) {
            Set<Integer> debugReceivers = new HashSet<>();
            for (int dstId = 1; dstId <= runConfig.getNumberOfHosts(); dstId++) {
//                if (dstId == latticeLink.hostId) // TODO this can only be done if I always ack from self right after broadcast - implement this!
//                    continue;

                if (!(proposerState.getAcked().get(dstId - 1) || proposerState.getNacked().get(dstId - 1))) {
                    debugReceivers.add(dstId);
                    latticeLink.send(data, dstId);
                }
            }
            System.out.println("Sending MSG " + LatticeMessage.deserialize(data) + " proposerState proposal value " + proposerState.getProposedValue() + " to " + debugReceivers);
            debugReceivers.clear();

//            try {
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
        }
    }
}
