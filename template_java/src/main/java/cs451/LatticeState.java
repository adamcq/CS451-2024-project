package cs451;

import cs451.Message.LatticeMessage;
import cs451.Message.Message;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class LatticeState {
    Map<Integer, ProposerState> proposerStateMap;
    Map<Integer, AcceptorState> acceptorStateMap;
    int maxIteration = 0;
    int activeIterations = 0;

    public LatticeState() {
        proposerStateMap = new HashMap<>();
        acceptorStateMap = new HashMap<>();
    }

    public void addIteration(int iteration, ProposerState proposerState, AcceptorState acceptorState) {
        System.out.println("STATE containsIteration (iter, proposerState, acceptorState): (" + iteration + " " + proposerStateMap.containsKey(iteration) + ", " + acceptorStateMap.containsKey(iteration) + ")");
        if (!proposerStateMap.containsKey(iteration)) {
            proposerStateMap.put(iteration, proposerState);
            acceptorStateMap.put(iteration, acceptorState);
            maxIteration++;
            activeIterations++;
        }
    }

    public void removeIteration(int iteration) {
        proposerStateMap.remove(iteration);
        acceptorStateMap.remove(iteration);
        activeIterations--;
    }

    public boolean iterationReached(int iteration) {
        return iteration <= maxIteration;
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
}
