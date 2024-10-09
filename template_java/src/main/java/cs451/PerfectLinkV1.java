package cs451;

public class PerfectLinkV1 {
    // A 2D array to track delivered messages
    // For simplicity, we use boolean to mark delivered (true if delivered, false otherwise)
    private final boolean[][] delivered;

    public PerfectLinkV1() {
        // Initialize the delivered array
        // Assume sender IDs range from 0 to 127 (128 senders max)
        delivered = new boolean[128][1000];
    }

    // Mark the message as delivered
    public void markAsDelivered(int senderId, int messageNumber) {
        delivered[senderId-1][messageNumber-1] = true;
    }

    // Check if the message is already delivered
    public boolean isDelivered(int senderId, int messageNumber) {
        return delivered[senderId-1][messageNumber-1];
    }
}
