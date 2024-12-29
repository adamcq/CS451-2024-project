package cs451;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class AcceptorState {
    private Set<Integer> acceptedValue;
    private int[] maxSeenProposalNumberFrom;
    private Set<Integer> toNack;
    LatticeState latticeState;
    public AcceptorState(Set<Integer> initialProposal, LatticeState latticeState) {
        this.acceptedValue = new HashSet<>();
        this.acceptedValue.addAll(initialProposal);
        this.latticeState = latticeState;

        toNack = new HashSet<>();
        maxSeenProposalNumberFrom = new int[128];
    }

    public boolean isSubsetOfProposedValue(Set<Integer> proposedValue) {
        return proposedValue.containsAll(acceptedValue);
//        return acceptedValue.containsAll(proposedValue);
    }

    public void addToAccepted(Set<Integer> proposedValue) {
        acceptedValue.addAll(proposedValue);
    }

    public Set<Integer> getAcceptedValue() {
        return acceptedValue;
    }

    public void setMaxSeenProposalNumberFrom(int proposalNumber, int senderId) {
        this.maxSeenProposalNumberFrom[senderId - 1] = proposalNumber;
    }

    public int getMaxSeenProposalNumberFrom(int senderId) {
        return maxSeenProposalNumberFrom[senderId - 1];
    }

    public int[] getMaxSeenProposalNumberFromAll() {
        return maxSeenProposalNumberFrom;
    }
    public void addToNack(int senderId) {
        toNack.add(senderId);
    }

    public Set<Integer> getToNack() {
        return toNack;
    }
    public void resetToNack(int senderId) {
        toNack.remove(senderId);
    }

    @Override
    public String toString() {
        return "AcceptorState{" +
                "acceptedValue=" + acceptedValue +
                '}';
    }
}
