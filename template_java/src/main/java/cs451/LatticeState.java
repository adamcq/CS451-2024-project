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
}
