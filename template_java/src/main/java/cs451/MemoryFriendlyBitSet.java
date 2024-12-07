package cs451;

import java.util.BitSet;

public class MemoryFriendlyBitSet {
    // there will be 2^16 windows (bitsets) of size maximally 2^16 for each sender
    // when all bits in window are set, the window is set to null and minWindowIdx[senderIndex] is incremented
    // this way we use at most 128 * 2^16 = 2^(7+16) = 2^23 bits ~= 1MB
    BitSet[][] delivered;
    int[] minWindowIdx;
    int numberOfWindows;
    private int MAX_WINDOW_SIZE = 4096; // 65536; // 2^16

    public MemoryFriendlyBitSet(int numberOfHosts, int numberOfMessages) {
        minWindowIdx = new int[numberOfHosts];
        int numberOfWindows = numberOfMessages / MAX_WINDOW_SIZE + 1;
        if (numberOfMessages % MAX_WINDOW_SIZE != 0) {
            numberOfWindows++;
        }
        this.numberOfWindows = numberOfWindows;
        delivered = new BitSet[numberOfHosts][numberOfWindows];
        for (int i = 0; i < numberOfHosts; i++) {
            delivered[i] = new BitSet[numberOfWindows];
            delivered[i][0] = new BitSet(MAX_WINDOW_SIZE);
        }
        System.out.println("INIT " + numberOfHosts + " x " + this.numberOfWindows + " x " + MAX_WINDOW_SIZE + " for " + numberOfMessages + " messages");
    }

    public void set(int senderId, int messageNumber) {
        int senderIndex = senderId - 1;
        int messageIndex = messageNumber - 1;

        int window = messageIndex / MAX_WINDOW_SIZE;
        int idx = messageIndex % MAX_WINDOW_SIZE;

        // if window doesn't exist, create it
        if (delivered[senderIndex][window] == null)
            delivered[senderIndex][window] = new BitSet(MAX_WINDOW_SIZE); // TODO MAX_WINDOW_SIZE doesn't need to be set

        // set bit
        delivered[senderIndex][window].set(idx);

        // if the min window is full, remove it and update the min window index
        if (delivered[senderIndex][minWindowIdx[senderIndex]].get(MAX_WINDOW_SIZE - 1) && delivered[senderIndex][minWindowIdx[senderIndex]].nextClearBit(0) == MAX_WINDOW_SIZE) {
//            System.out.println("freeing memory senderId " + senderId + " batch index " + messageIndex);
            delivered[senderIndex][minWindowIdx[senderIndex]] = null;
            System.gc();
            minWindowIdx[senderIndex]++;
            while (delivered[senderIndex][minWindowIdx[senderIndex]] != null && delivered[senderIndex][minWindowIdx[senderIndex]].nextClearBit(0) == MAX_WINDOW_SIZE)
                minWindowIdx[senderIndex]++;
        }
    }

    public boolean isSet(int senderId, int messageNumber) {
        int senderIndex = senderId - 1;
        int messageIndex = messageNumber - 1;

        int window = messageIndex / MAX_WINDOW_SIZE;
        int idx = messageIndex % MAX_WINDOW_SIZE;

//        System.out.println("debug  numberOfWindows,  MAX_WINDOW_SIZE " + numberOfWindows + " " + MAX_WINDOW_SIZE);
//        System.out.println("debug0 senderIndex, messageIndex " + senderIndex + " " + messageIndex);
//        System.out.println("debug1 window, idx " + window + " " + idx);

        if (window < minWindowIdx[senderIndex]) // TODO verify
            return true;

        return delivered[senderIndex][window] != null && delivered[senderIndex][window].get(idx);
    }
}



// 65536 // 2^16