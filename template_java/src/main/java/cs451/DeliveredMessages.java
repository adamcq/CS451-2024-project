package cs451;

import java.util.Arrays;
import java.util.BitSet;

public class DeliveredMessages {
    private int[] offset;
    BitSet[] delivered;
    private int MAX_WINDOW_SIZE; // TODO only use if necessary. Keep it dynamic otherwise

    public DeliveredMessages(int numberOfHosts, int maxWindowSize) {
        this.MAX_WINDOW_SIZE = maxWindowSize;
        offset = new int[numberOfHosts]; // initially all 0s
        delivered = new BitSet[numberOfHosts];
        for (int i = 0; i < numberOfHosts; i++) {
            delivered[i] = new BitSet();
        }
    }

    public void setDelivered(int senderId, int messageNumber) {
        int senderIndex = senderId - 1;
        int messageIndex = messageNumber - 1;

        // logic to update the delivered[senderIndex] array

        // remove the first values until the one which is not set and upload offset
        int shift = delivered[senderIndex].nextClearBit(0);
        delivered[senderIndex].clear(0, shift);
        offset[senderIndex] += shift;

        // then update the messageIndex
        messageIndex -= offset[senderIndex];

        // then set the bit at messageIndex
        delivered[senderIndex].set(messageIndex);
    }

    public boolean isDelivered(int senderId, int messageNumber) {
        int senderIndex = senderId - 1;
        int messageIndex = messageNumber - 1;

        messageIndex -= offset[senderIndex];
        return messageIndex >= 0 && delivered[senderIndex].get(messageIndex);
    }
}

/*
* 1 1 1 1 0 1 0 1 1
* 0 0 0 0 0 1 0 1 1
*
*
*
*
* */