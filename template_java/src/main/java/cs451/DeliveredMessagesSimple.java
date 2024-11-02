package cs451;

import java.util.Arrays;
import java.util.BitSet;

public class DeliveredMessagesSimple {
    BitSet[] delivered;
    private int MAX_WINDOW_SIZE; // TODO only use if necessary. Keep it dynamic otherwise

    public DeliveredMessagesSimple(int numberOfHosts, int maxWindowSize) {
        this.MAX_WINDOW_SIZE = maxWindowSize;
        delivered = new BitSet[numberOfHosts];
        for (int i = 0; i < numberOfHosts; i++) {
            delivered[i] = new BitSet();
        }
    }

    public void setDelivered(int senderId, int messageNumber) {
        int senderIndex = senderId - 1;
        int messageIndex = messageNumber - 1;

        // logic to update the delivered[senderIndex] array
        delivered[senderIndex].set(messageIndex);
    }

    public boolean isDelivered(int senderId, int messageNumber) {
        int senderIndex = senderId - 1;
        int messageIndex = messageNumber - 1;

        return delivered[senderIndex].get(messageIndex);
    }
}
