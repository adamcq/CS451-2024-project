package cs451;

import java.util.BitSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerArray;

public class DeliveredCompressedConcurrent {
    private final ConcurrentHashMap<Integer, BitSet[]> delivered;
    private final AtomicIntegerArray minWindowIdx;
    private final int numberOfWindows;
    private final int MAX_WINDOW_SIZE;

    public DeliveredCompressedConcurrent(int numberOfHosts, int maxWindowSize, int numberOfMessages) {
        this.MAX_WINDOW_SIZE = maxWindowSize;
        this.minWindowIdx = new AtomicIntegerArray(numberOfHosts);
        int calculatedWindows = (numberOfMessages + maxWindowSize - 1) / maxWindowSize;
        this.numberOfWindows = calculatedWindows;
        this.delivered = new ConcurrentHashMap<>(numberOfHosts);

        // Initialize empty BitSet arrays for each sender
        for (int i = 0; i < numberOfHosts; i++) {
            BitSet[] bitSets = new BitSet[numberOfWindows];
            bitSets[0] = new BitSet(MAX_WINDOW_SIZE); // Initialize the first window
            delivered.put(i, bitSets);
        }

        System.out.println("Initialized " + numberOfHosts + " x " + this.numberOfWindows + " x " + MAX_WINDOW_SIZE + " for " + numberOfMessages + " messages");
    }

    public void setDelivered(int senderId, int messageNumber) {
        int senderIndex = senderId - 1;
        int messageIndex = messageNumber - 1;

        int window = messageIndex / MAX_WINDOW_SIZE;
        int idx = messageIndex % MAX_WINDOW_SIZE;

        // Retrieve the BitSet array for this sender
        BitSet[] bitSets = delivered.get(senderIndex);

        // Synchronized block on the specific BitSet to avoid cross-thread conflicts
        synchronized (bitSets) {
            // If the window BitSet doesn't exist, initialize it
            if (bitSets[window] == null) {
                bitSets[window] = new BitSet(MAX_WINDOW_SIZE);
            }

            // Set the bit for the specific message index
            bitSets[window].set(idx);

            // Check if the minimum window is full
            int currentMinWindow = minWindowIdx.get(senderIndex);
            BitSet minBitSet = bitSets[currentMinWindow];

            if (minBitSet != null && minBitSet.nextClearBit(0) == MAX_WINDOW_SIZE) {
                bitSets[currentMinWindow] = null;
                minWindowIdx.incrementAndGet(senderIndex);

                // Move minWindowIdx forward if the next windows are also full
                while (minWindowIdx.get(senderIndex) < numberOfWindows &&
                        bitSets[minWindowIdx.get(senderIndex)] != null &&
                        bitSets[minWindowIdx.get(senderIndex)].nextClearBit(0) == MAX_WINDOW_SIZE) {
                    bitSets[minWindowIdx.get(senderIndex)] = null;
                    minWindowIdx.incrementAndGet(senderIndex);
                }
            }
        }
    }

    public boolean isDelivered(int senderId, int messageNumber) {
        int senderIndex = senderId - 1;
        int messageIndex = messageNumber - 1;

        int window = messageIndex / MAX_WINDOW_SIZE;
        int idx = messageIndex % MAX_WINDOW_SIZE;

        BitSet[] bitSets = delivered.get(senderIndex);

        // Synchronized access on the specific BitSet for safety
        synchronized (bitSets) {
            return bitSets[window] != null && bitSets[window].get(idx);
        }
    }
}
