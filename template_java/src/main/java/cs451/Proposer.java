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
//        System.out.println("UPON_NEW_BROADCAST_TRIGGERED !!!!!! iteration=" + proposerState.iteration + " actPropNum=" + proposerState.getActiveProposalNumber());

        // Cancel the current task if it is running
//        stopCurrentBroadcast(proposerState);

        // Submit a new broadcast task
//        currentTask = executor.submit(() -> {
//            beb.bebBroadcast(MessageType.PROPOSAL, proposerState.getProposedValue(), proposerState.getActiveProposalNumber(), proposerState.iteration);
//        });

        // TODO verify if the updated broadcast is visible by the broadcast thread

        // Add the new task to the queue
//        System.out.println("UPDATE BROADCAST CALLING with PARAMS: (proposedValue, proposalNumber, iteration)=(" + proposerState.getProposedValue()+" "+proposerState.getActiveProposalNumber()+" "+proposerState.getIteration()+")");
        beb.updateBroadcast(proposerState.getProposedValue(), proposerState.getActiveProposalNumber(), proposerState.getIteration());

        // stop the loop
//        Thread.currentThread().interrupt();
//        System.out.println("Thread stopped");

        // Interrupt the thread to refresh the loop
//        stopCurrentBroadcast(proposerState);

        // submit the loop again
//        currentTask = executor.submit(beb::startBroadcastLoop);
    }

    public void stopCurrentBroadcast(ProposerState proposerState) {
        if (currentTask != null && !currentTask.isDone()) {
            currentTask.cancel(true);
        }
    }

    public void startBroadcastLoop() {
        currentTask = executor.submit(beb::startBroadcastLoop);
        System.out.println("Submitted startBroadcastLoop to the executor");
    }

    public void shutdown() {
        // Gracefully shut down the executor
        System.out.println("Shutting down the broadcast executor");
        executor.shutdownNow();
    }
}
