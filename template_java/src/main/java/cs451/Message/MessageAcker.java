package cs451.Message;

import java.util.HashSet;

public class MessageAcker {
    private Message message;
    private HashSet<Integer> acked;
    private int ackedCount;

    public MessageAcker(Message message) {
        this.message = message;
        this.acked = new HashSet<>();
        ackedCount = 0;
    }

    public boolean isAcked(int senderId) {
        return acked.contains(senderId);
    }

    /**
     * @param senderId senderId to add to acked set
     * @return ackedCount - caller can then check if ackedCount > N/2
     */
    public int addAckFrom(int senderId) {
        if (!acked.contains(senderId))
            ackedCount++;
        acked.add(senderId);
        return ackedCount;
    }

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
    }

    public HashSet<Integer> getAcked() {
        return acked;
    }

    public void setAcked(HashSet<Integer> acked) {
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
