package cs451.Message;

import java.util.Arrays;

public class Message {
    private byte messageType;
    private int senderId;
    private int batchNumber;
    private int[] data;

    public Message(byte messageType, int senderId, int batchNumber, int[] data) {
        this.messageType = messageType;
        this.senderId = senderId;
        this.batchNumber = batchNumber;
        this.data = data;
    }

    /**
     * @param lastCreated the Batch Number of the last created Batch
     * @return new Message
     */
    public static Message createMessage(int lastCreated, int numberOfMessages, int processId) {
        int[] data = new int[Math.min(8, numberOfMessages - lastCreated * 8)];
        for (int i = 0; i < data.length; i++) {
            data[i] = lastCreated * 8 + i + 1;
        }

        return new Message(
                (byte) 0,
                processId,
                lastCreated+1,
                data
        );
    }

    public int getMessageSize() {
        // byte type, int senderId, int batchNumber, int[] data, int relayId
        return (Byte.SIZE + Integer.SIZE + Integer.SIZE + Integer.SIZE * data.length + Integer.SIZE) / 8;
    }

    public int getBatchNumber() {
        return batchNumber;
    }

    public void setBatchNumber(int batchNumber) {
        this.batchNumber = batchNumber;
    }

    public byte getMessageType() {
        return messageType;
    }

    public void setMessageType(byte messageType) {
        this.messageType = messageType;
    }

    public int getSenderId() {
        return senderId;
    }

    public void setSenderId(int senderId) {
        this.senderId = senderId;
    }

    public int[] getData() {
        return data;
    }

    public void setData(int[] data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "Message{" +
                "messageType=" + messageType +
                ", senderId=" + senderId +
                ", batchNumber=" + batchNumber +
                ", data=" + Arrays.toString(data) +
                '}';
    }
}
