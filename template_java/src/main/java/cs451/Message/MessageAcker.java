package cs451.Message;

import cs451.RunConfig;

import java.util.BitSet;
import java.util.HashSet;

public class MessageAcker {
    private Message message;
//    private HashSet<Integer> acked;
    private BitSet acked;
    private int ackedCount;

    public MessageAcker(Message message, RunConfig runConfig) {
        this.message = message;
//        this.acked = new HashSet<>();
        this.acked = new BitSet(runConfig.getNumberOfHosts());
        ackedCount = 0;
    }

    public boolean isAcked(int senderId) {
//        return acked.contains(senderId);
        return acked.get(senderId - 1);
    }

    /**
     * @param senderId senderId to add to acked set
     * @return ackedCount - caller can then check if ackedCount > N/2
     */
    public int addAckFrom(int senderId) {
        if (!acked.get(senderId - 1))
            ackedCount++;
        acked.set(senderId - 1);
        return ackedCount;
    }

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
    }

    public BitSet getAcked() {
        return acked;
    }

    public void setAcked(BitSet acked) {
        this.acked = acked;
    }

    public int getAckedCount() {
        return ackedCount;
    }

    public void setAckedCount(int ackedCount) {
        this.ackedCount = ackedCount;
    }

    @Override
    public String toString() {
        return "MessageAcker{" +
                "message=" + message +
                ", acked=" + acked +
                ", ackedCount=" + ackedCount +
                '}';
    }
}
