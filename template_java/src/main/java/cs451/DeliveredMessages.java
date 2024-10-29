package cs451;

public class DeliveredMessages {
    private int[] seqNo; // stores the sequence Number of the window for each sender
    private boolean[][] deliveredWindow; // current delivered window for each sender
    private int windowSize;

    public DeliveredMessages(int numberOfSenders, int windowSize) {
        this.windowSize = windowSize;
        seqNo = new int[numberOfSenders]; // initially 0s
        deliveredWindow = new boolean[numberOfSenders][windowSize];
    }

    public void markDelivered(int senderId, int messageNumber) {
        int senderIdx = senderId - 1;
        int messageIndex = messageNumber - 1;
        if (messageNumber > (seqNo[senderIdx]+1) * 1000) {
            seqNo[senderIdx]++;

        }
    }
}
