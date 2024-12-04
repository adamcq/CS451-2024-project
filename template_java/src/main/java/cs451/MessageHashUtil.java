package cs451;

public class MessageHashUtil {

    public static long createMessageHash(int senderId, int batchNumber) {
        return ((long) senderId << 32) | (batchNumber & 0xFFFFFFFFL);
    }

    public static long createMessageHash(Message message) {
        return ((long) message.getSenderId() << 32) | (message.getBatchNumber() & 0xFFFFFFFFL);
    }

    public static int extractSenderId(long messageHash) {
        return (int) (messageHash >>> 32);
    }

    public static int extractMessageNumber(long messageHash) {
        return (int) messageHash;
    }
}
