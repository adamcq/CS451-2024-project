package cs451;

import cs451.Message.LatticeMessage;

import java.net.SocketException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ProposerBEB {

    LatticeLink latticeLink;
    LatticeState latticeState;
    LatticeRunConfig runConfig;
    private volatile boolean stopBroadcast = false; // Flag to stop the broadcast loop
    private final Map<Integer, AbstractMap.SimpleEntry<LatticeMessage, byte[]>> messages;



    public ProposerBEB(LatticeLink latticeLink, LatticeState latticeState, LatticeRunConfig runConfig) throws SocketException {
        this.runConfig = runConfig;
        this.latticeLink = latticeLink;
        this.latticeState = latticeState;
        this.messages = new ConcurrentHashMap<>();
    }

    public void updateBroadcast(Set<Integer> proposerValue, int activeProposalNumber, int iteration) {
        // build msg
        LatticeMessage msg = new LatticeMessage((byte) 0, runConfig.getProcessId(), runConfig.getProcessId(), activeProposalNumber, iteration, proposerValue.size(), proposerValue);
        byte[] data = msg.serialize();

        // update messages
        synchronized (messages) {
            messages.put(iteration, new AbstractMap.SimpleEntry<>(msg, data));
        }

        System.out.println("Updating beb (inserting to iteration) " + iteration +": ");
        for (AbstractMap.SimpleEntry entry : messages.values()) {
            System.out.println("\t" + entry.getKey());
        }
        System.out.println();
    }

    public void startBroadcastLoop() {
        broadcastLoop();
        System.out.println("Broadcast loop interrupted 2");
    }

    private void broadcastLoop() {
        try {
            System.out.println("Broadcast loop started");
            while (true) { // TODO instead check when sbd wants to update activepropnumber, update the set and restart loop - loop should internally iterate thru set of messsages for each ITERATION simultaneously
                System.out.println("beb looop inside messagesCount=" + messages.size() + " timestampMillis=" + System.currentTimeMillis());

                List<Integer> toRemove = new ArrayList<>();
                for (Map.Entry<Integer, AbstractMap.SimpleEntry<LatticeMessage, byte[]>> entry : messages.entrySet()) {
                    Set<Integer> debugReceivers = new HashSet<>();
                    LatticeMessage msg = entry.getValue().getKey();

                    // TODO i need to remove old messages state from memory at some point
                    if (latticeState.isMessageDelivered(msg)) {
                        System.out.println("Removing message from BROADCAST. acked by: " );//+ latticeState.proposerStateMap.get(msg.getIteration()).getAcked() + " msg: " + msg);
                        toRemove.add(msg.getIteration());

                        continue; // TODO verify if the break should be here
                    }

                    for (int dstId = 1; dstId <= runConfig.getNumberOfHosts(); dstId++) {
                        if (dstId == latticeLink.hostId)
                            continue;

                        System.out.println("(!latticeState.hasDstReceivedMessage(dstId, msg))=" + (!latticeState.hasDstReceivedMessage(dstId, msg)));
                        if (!latticeState.hasDstReceivedMessage(dstId, msg)) {
                            debugReceivers.add(dstId);
                            latticeLink.send(entry.getValue().getValue(), dstId);
                        }
                    }
                    System.out.println("Sending MSG " + msg + " to " + debugReceivers);
                    debugReceivers.clear();
                }

                for (int iterToRemove : toRemove) {
                    latticeState.removeIteration(iterToRemove); // TODO debug - uncomment this - but logic has to change
                    synchronized (messages) {
                        messages.remove(iterToRemove);
                    }
                }

//            try {
//                Thread.sleep(50);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
            }
        } catch (Exception e ) {
            System.out.println("EXCEPTION ERROR BEBLOOP " + e.getMessage() + e);
            e.printStackTrace();
        }
//        System.out.println("Broadcast loop interrupted");
    }
}
