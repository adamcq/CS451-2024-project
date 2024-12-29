package cs451;

import cs451.Message.LatticeMessage;

import java.util.BitSet;
import java.util.Set;

public class ProposerState {
    boolean active = true;
    int ackCount = 0;
    int nackCount = 0;
    BitSet acked;
    BitSet nacked;
    int activeProposalNumber = 0;
    Set<Integer> proposedValue;
    Proposer proposer;
    LatticeRunConfig runConfig;
    int iteration;
    // TODO implement hashmap {iteration: ProposerState} - remove after decided - control the logic in main based on the size (max 5 at the same time, etc.)
    public ProposerState(Set<Integer> proposal,  LatticeRunConfig runConfig, Proposer proposer, int iteration) {
        proposedValue = proposal;
        active = true;
        activeProposalNumber++;
        ackCount = 0;
        nackCount = 0;
        this.runConfig = runConfig;
        this.iteration = iteration;
        this.proposer = proposer;

        this.nacked = new BitSet();
        this.acked = new BitSet();
        ackReceived(new LatticeMessage((byte) 1, runConfig.getProcessId(), runConfig.getProcessId(), 1, iteration, proposal.size(), proposal));
    }

//    public void setProposer(Proposer proposer) {
//        this.proposer = proposer;
//        proposer.uponProposerStateInitialized(this);
//    }


    public void ackReceived(LatticeMessage msg) {
        System.out.println("ACK received step 2 " + msg + " " + activeProposalNumber);
        if (msg.getProposalNumber() != activeProposalNumber)
            return;

        // CUSTOM - ignore duplicate ACK for this round
        if (acked.get(msg.getRelayId() - 1) || nacked.get(msg.getRelayId() - 1) || iteration != msg.getIteration())
            return;

//        System.out.println("ACK received for " + msg);

//        proposerState.setAckCount(proposerState.getAckCount() + 1); // INSUFFICIENT
        addAcked(msg.getRelayId());

        // UPON logic
        if (ackCount > runConfig.getNumberOfHosts() / 2 && active) {
//            System.exit(1);
            System.out.println("DECIDING " + msg + " ACKED=" + acked + " NACKED=" + nacked + "ackedCount="+ackCount+" nackCount="+nackCount);
            decide(proposedValue); // TODO before i had msg.getProposalValue() and that was the ERROR!
            setActive(false);
        }

        if (nackCount > 0 && ackCount + ackCount > runConfig.getNumberOfHosts() / 2 && active) { // TODO active should also check integer (MAYBE)
            System.out.println("REBROADCASTING1 ackCount="+ackCount+" nackCount="+nackCount+" acked="+acked+" nacked"+nacked + "proposedValue="+proposedValue);
            incrementActiveProposalNumber();
            resetNacked();
            resetAcked();
//            System.out.println("br trird1");
            proposer.uponNewBroadcastTriggered(this);
//            System.out.println("br trird2");
//            System.out.println("EXIT 1");
//            System.exit(1);
        }
    }

    public void nackReceived(LatticeMessage msg) {
        if (msg.getProposalNumber() != activeProposalNumber)
            return;

        // CUSTOM - ignore duplicate NACK for this round
        if (acked.get(msg.getRelayId() - 1) || nacked.get(msg.getRelayId() - 1) || iteration != msg.getIteration())
            return;

        System.out.println("TODO REMOVE THE LINE BELOW " + proposedValue + " msg=" + msg);
        Set<Integer> newProposalValue = updateProposalValue(msg.getProposalValue());
        System.out.println("TODO REMOVE THE LINE ABOVE " + proposedValue);
//        Set<Integer> newProposalValue = updateProposalValue(msg.getProposalValue());
        addNacked(msg.getRelayId());

        // UPON logic
        if (nackCount > 0 && ackCount + nackCount > runConfig.getNumberOfHosts() / 2 && active) {
            System.out.println("REBROADCASTING2 iteration="+iteration+"ackCount="+ackCount+" nackCount="+nackCount+" acked="+acked+" nacked"+nacked + "proposedValue="+proposedValue);
            incrementActiveProposalNumber();
            resetNacked();
            resetAcked();
//            System.out.println("br trigrd1");
            proposer.uponNewBroadcastTriggered(this);
//            System.out.println("br trigrd2");
//            System.out.println("EXIT 3");
//            System.exit(3);
        }
    }

    public void proposalReceived(LatticeMessage msg) {

    }

    public Set<Integer> updateProposalValue(Set<Integer> value) {
        proposedValue.addAll(value);
        return proposedValue;
    }

    public int incrementActiveProposalNumber() {
        return ++activeProposalNumber;
    }

    public void addAcked(int relayId) {
        if (!acked.get(relayId - 1))
            ackCount++;
        acked.set(relayId - 1);
    }

    public void addNacked(int relayId) {
        if (!nacked.get(relayId - 1))
            nackCount++;
        nacked.set(relayId - 1);
    }

    public void resetAcked() {
        acked = new BitSet();
        ackCount = 0;
    }

    public void resetNacked() {
        nacked = new BitSet();
        nackCount = 0;
    }

    private void decide(Set<Integer> value) {
        System.out.println("decide called");
        StringBuilder valueToLog = new StringBuilder();
        valueToLog.append(iteration).append(" d ");
        for (int i : value) {
            valueToLog.append(i);
            valueToLog.append(" ");
        }
        runConfig.getLogBuffer().log(valueToLog.toString()); // TODO verify if last " " needs to be removed
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public int getAckCount() {
        return ackCount;
    }

    public void setAckCount(int ackCount) {
        this.ackCount = ackCount;
    }

    public int getNackCount() {
        return nackCount;
    }

    public void setNackCount(int nackCount) {
        this.nackCount = nackCount;
    }

    public int getActiveProposalNumber() {
        return activeProposalNumber;
    }

    public void setActiveProposalNumber(int activeProposalNumber) {
        this.activeProposalNumber = activeProposalNumber;
    }

    public Set<Integer> getProposedValue() {
        return proposedValue;
    }

    public void setProposedValue(Set<Integer> proposedValue) {
        this.proposedValue = proposedValue;
    }

    public BitSet getAcked() {
        return acked;
    }

    public BitSet getNacked() {
        return nacked;
    }

    public int getIteration() {
        return iteration;
    }

    @Override
    public String toString() {
        return "ProposerState{" +
                "active=" + active +
                ", ackCount=" + ackCount +
                ", nackCount=" + nackCount +
                ", acked=" + acked +
                ", nacked=" + nacked +
                ", activeProposalNumber=" + activeProposalNumber +
                ", proposedValue=" + proposedValue +
                ", proposer=" + proposer +
                ", runConfig=" + runConfig +
                ", iteration=" + iteration +
                '}';
    }
}
