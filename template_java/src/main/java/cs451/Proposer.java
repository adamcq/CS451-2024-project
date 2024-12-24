package cs451;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Proposer {
    private final ProposerBEB beb;
    private final LatticeRunConfig runConfig;
    private final ExecutorService executor;
    private Future<?> currentTask;

    public Proposer(ProposerBEB beb, LatticeRunConfig runConfig) {
        this.beb = beb;
        this.runConfig = runConfig;
        this.executor = Executors.newSingleThreadExecutor();
    }

    public void uponNewBroadcastTriggered(ProposerState proposerState) {
        System.out.println("UPON_NEW_BROADCAST_TRIGGERED !!!!!!");

        // Cancel the current task if it is running
        stopCurrentBroadcast(proposerState);

        // Submit a new broadcast task
        currentTask = executor.submit(() -> {
            beb.bebBroadcast(MessageType.PROPOSAL, proposerState.getProposedValue(), proposerState.getActiveProposalNumber());
        });
    }

    public void stopCurrentBroadcast(ProposerState proposerState) {
        if (currentTask != null && !currentTask.isDone()) {
            currentTask.cancel(true);
        }
    }

    public void uponProposerStateInitialized(ProposerState proposerState) {
        currentTask = executor.submit(() -> {
            beb.bebBroadcast(MessageType.PROPOSAL, proposerState.getProposedValue(), proposerState.getActiveProposalNumber());
        });
    }

    public void shutdown() {
        // Gracefully shut down the executor
        executor.shutdownNow();
    }
}
