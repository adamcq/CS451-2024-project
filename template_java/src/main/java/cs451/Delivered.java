package cs451;

import java.util.BitSet;

public class Delivered {
    BitSet[] delivered;

    public Delivered(int numberOfHosts, int maxWindowSize, int numberOfMessages) {
        delivered = new BitSet[numberOfHosts];
        for (int i = 0; i < numberOfHosts; i++) {
            delivered[i] = new BitSet(numberOfMessages);
        }
    }

    public boolean isDelivered(int senderId, int messageNumber) {
        return delivered[senderId - 1].get(messageNumber - 1);
    }

    public void setDelivered(int senderId, int messageNumber) {
        delivered[senderId - 1].set(messageNumber - 1);
    }
}
