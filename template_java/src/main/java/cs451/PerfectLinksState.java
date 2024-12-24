package cs451;

public class PerfectLinksState {
    private MemoryFriendlyBitSet delivered;
    private MemoryFriendlyBitSet acked;

    public PerfectLinksState(RunConfig runConfig) {
        this.delivered = new MemoryFriendlyBitSet(runConfig.getNumberOfHosts(), runConfig.getNumberOfMessages());
        this.acked = new MemoryFriendlyBitSet(runConfig.getNumberOfHosts(), runConfig.getNumberOfMessages());
    }
}
