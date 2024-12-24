package cs451;

import java.util.HashSet;
import java.util.Set;

public class AcceptorState {
    private Set<Integer> acceptedValue;
    public AcceptorState() {
        this.acceptedValue = new HashSet<>();
    }

    public boolean containsProposedValue(Set<Integer> proposedValue) {
        return acceptedValue.containsAll(proposedValue);
    }

    public void addToAccepted(Set<Integer> proposedValue) {
        acceptedValue.addAll(proposedValue);
    }

    public Set<Integer> getAcceptedValue() {
        return acceptedValue;
    }
}
