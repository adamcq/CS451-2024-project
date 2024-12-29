package cs451;

import cs451.Message.LatticeMessage;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class LatticeState {
    Map<Integer, ProposerState> proposerStateMap;
    Map<Integer, AcceptorState> acceptorStateMap;
    int maxIteration = 0;
    int activeIterations = 0;
    int nextToDeliver = 1;
    Map<Integer, Set<Integer>> toDeliver; // TODO change to Map<Integer, BitSet>
    LatticeRunConfig runConfig;
    AcceptorValuesHashMap acceptorValuesHashMap;
    Set<Integer> activeIterationsSet;
    int maxActiveIterationsThreshold = 10; // TODO change to around 10-100

    public LatticeState(LatticeRunConfig runConfig, AcceptorValuesHashMap acceptorValuesHashMap) {
        proposerStateMap = new ConcurrentHashMap<>();
        acceptorStateMap = new ConcurrentHashMap<>();
        this.acceptorValuesHashMap = acceptorValuesHashMap;

        toDeliver = new HashMap<>();
        this.runConfig = runConfig;
        this.activeIterationsSet = new HashSet<>();

        if (runConfig.getNumberOfHosts() <= 5)
            maxActiveIterationsThreshold = 100;
        else
            maxActiveIterationsThreshold = 10;
    }

    @Override
    public String toString() {
        return "LatticeState{" +
                "proposerStateMap=" + proposerStateMap +
                ", acceptorStateMap=" + acceptorStateMap +
                ", maxIteration=" + maxIteration +
                ", activeIterations=" + activeIterations +
                '}';
    }

    public void addIteration(int iteration, ProposerState proposerState, AcceptorState acceptorState) {
//        System.out.println("STATE containsIteration (iter, proposerState, acceptorState): (" + iteration + " " + proposerStateMap.containsKey(iteration) + ", " + acceptorStateMap.containsKey(iteration) + ")");
        if (!proposerStateMap.containsKey(iteration)) {
            proposerStateMap.put(iteration, proposerState);
            acceptorStateMap.put(iteration, acceptorState);
            maxIteration++;
            activeIterations++;
            activeIterationsSet.add(iteration);

            // remove old stuff which should not be active TODO verify if it actually works
            // TODO also maybe change to -10 instead of -1 to leave some buffer
//            acceptorStateMap.remove(iteration - 2*maxActiveIterationsThreshold);
        }
    }

    public void removeIteration(int iteration) {
//        System.out.println("LatticeState removing iteration " + iteration);
//        System.out.println("state: " + this);
        proposerStateMap.remove(iteration);
//        acceptorStateMap.remove(iteration); // TODO this needs to be removed at some point for memory reasons - perhaps a bitset to keep track or use the vs/ds values to see if all are in accepted
        activeIterations--;
        activeIterationsSet.remove(iteration);
    }

    public boolean iterationReached(int iteration) {
        return iteration <= maxIteration;
    }

    public boolean iterationComplete(int iteration) {
//        return !acceptorStateMap.containsKey(iteration);
        return !proposerStateMap.containsKey(iteration); // TODO this is just debug, uncomment this and remove line above
    }

    /* ACCEPTOR STATE */
    public boolean containsProposedValue(LatticeMessage msg) { // TODO actually add the iteration if it's not active yet
        return acceptorStateMap.get(msg.getIteration()).containsProposedValue(msg.getProposalValue());
    }
    public void addToAccepted(LatticeMessage msg) {
        acceptorStateMap.get(msg.getIteration()).addToAccepted(msg.getProposalValue());
    }
    public Set<Integer> getAcceptedValue(LatticeMessage msg) {
        return acceptorStateMap.get(msg.getIteration()).getAcceptedValue();
    }

    /* PROPOSER STATE */
    public void ackReceived(LatticeMessage msg) {
        proposerStateMap.get(msg.getIteration()).ackReceived(msg);
    }

    public void nackReceived(LatticeMessage msg) {
        proposerStateMap.get(msg.getIteration()).nackReceived(msg);
    }

    /* BEB BROADCAST */
    // this method checks if for the current activeProposalNumber, the dstId has acked or nacked the message
    public boolean hasDstReceivedMessage(int dstId, LatticeMessage msg) {
        return (proposerStateMap.get(msg.getIteration()).getAcked().get(dstId - 1)
                || proposerStateMap.get(msg.getIteration()).getNacked().get(dstId - 1));
    }

    public boolean isMessageDelivered(LatticeMessage msg) { // TODO check if this is correct
//        System.out.println("isMessageDelivered DEBUG msg="+msg+" msg.getIteration()="+msg.getIteration()+" proposerStateMap.get(msg.getIteration())="+proposerStateMap.get(msg.getIteration()) +" (!proposerStateMap.get(msg.getIteration()).isActive())"+(!proposerStateMap.get(msg.getIteration()).isActive()));
        return (!proposerStateMap.get(msg.getIteration()).isActive());
    }

    /* DELIVER MESSAGES */
    public synchronized void waitForDeliver(int iteration, Set<Integer> messageSet) {
        toDeliver.put(iteration, messageSet);
        Set<Integer> toRemove = new HashSet<>();

        while (toDeliver.containsKey(nextToDeliver)) {
            StringBuilder valueToLog = new StringBuilder();
//            valueToLog.append(nextToDeliver).append(" d ");
            valueToLog.append("d ");
//            System.out.println("DECIDING toDeliver.size()="+toDeliver.size()+" iteration=" + nextToDeliver + " d " + nextToDeliver + " " + toDeliver.get(nextToDeliver));
            for (int i : toDeliver.get(nextToDeliver)) {
                valueToLog.append(i);
                valueToLog.append(" ");
            }
            runConfig.getLogBuffer().log(valueToLog.toString());
            toRemove.add(nextToDeliver);
            nextToDeliver++;
        }

        for (int key : toRemove)
            toDeliver.remove(key);
    }

    /* ACCEPTOR HASH MAP */
    public AcceptorValuesHashMap getAcceptorValuesHashMap() {
        return acceptorValuesHashMap;
    }
}
