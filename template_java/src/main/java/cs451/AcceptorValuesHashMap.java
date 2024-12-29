package cs451;

import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

public class AcceptorValuesHashMap {

    // Global mapping of integers to hash indices
    private final Map<Integer, Integer> globalHashMap;
    private final int[] hashToNumber;
    private int nextHashIndex; // Tracks the next available hash index
    private final int maxHashSize; // Maximum number of hashes (e.g., 1000)

    public AcceptorValuesHashMap(int maxHashSize) { // TODO maxHashSize is vs or ds
        this.globalHashMap = new HashMap<>();
        this.hashToNumber = new int[maxHashSize];
        this.nextHashIndex = 0;
        this.maxHashSize = maxHashSize;
    }

    // Hash an integer to a global index
    public synchronized int getOrAddHashIndex(int value) {
        // If the value is already hashed, return its index
        if (globalHashMap.containsKey(value)) {
            return globalHashMap.get(value);
        }

        // Ensure we don't exceed the max hash size
        if (nextHashIndex >= maxHashSize) {
            throw new IllegalStateException("Hash index limit exceeded. Increase maxHashSize.");
        }

        // Assign a new hash index to the value
        globalHashMap.put(value, nextHashIndex);
        hashToNumber[nextHashIndex] = value;
        return nextHashIndex++;
    }

    // Reverse lookup: Get the original value from a hash index
    public synchronized int getOriginalValue(int hashIndex) {
        return hashToNumber[hashIndex];
    }

    @Override
    public String toString() {
        return "AcceptorValuesHashMap{" +
                "globalHashMap=" + globalHashMap +
                ", hashToNumber=" + Arrays.toString(hashToNumber) +
                ", nextHashIndex=" + nextHashIndex +
                ", maxHashSize=" + maxHashSize +
                '}';
    }
}
