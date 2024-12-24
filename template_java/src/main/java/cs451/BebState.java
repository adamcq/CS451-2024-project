package cs451;

import java.util.BitSet;

public class BebState {
    private BitSet[] acked;
    private int[] startSendFrom;
    private int[] sendWindowSize;

    public BebState(RunConfig runConfig) {
        int numberOfHosts = runConfig.getNumberOfHosts();
        this.startSendFrom = new int[numberOfHosts];
        this.acked = new BitSet[numberOfHosts];
        for (int i = 0; i < numberOfHosts; i++) {
            acked[i] = new BitSet();
        }
        sendWindowSize = new int[numberOfHosts];
        for (int i = 0; i < numberOfHosts; i++)
            sendWindowSize[i] = 1;
    }

    public synchronized BitSet[] getAcked() {
//        System.out.println("getAcked called by thread " + Thread.currentThread());
        return acked;
    }

    public void setAcked(BitSet[] acked) {
        this.acked = acked;
    }

    public int[] getStartSendFrom() {
        return startSendFrom;
    }

    public void setStartSendFrom(int[] startSendFrom) {
        this.startSendFrom = startSendFrom;
    }

    public int[] getSendWindowSize() {
        return sendWindowSize;
    }

    public void setSendWindowSize(int[] sendWindowSize) {
        this.sendWindowSize = sendWindowSize;
    }
}
