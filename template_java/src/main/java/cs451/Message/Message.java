package cs451.Message;

import java.util.Arrays;

public class Message {
    private byte messageType;
    private int senderId;
    private int batchNumber;
    private int[] data;
    private long broadcastTime;

    public Message(byte messageType, int senderId, int batchNumber, int[] data, long broadcastTime) {
        this.messageType = messageType;
        this.senderId = senderId;
        this.batchNumber = batchNumber;
        this.data = data;
        this.broadcastTime = broadcastTime;
    }

    public int getMessageSize() {
        // byte type, int senderId, int batchNumber, int[] data, int relayId, long sendTime
        return (Byte.SIZE + Integer.SIZE + Integer.SIZE + Integer.SIZE * data.length + Integer.SIZE + Long.SIZE) / 8;
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

    public long getBroadcastTime() {
        return broadcastTime;
    }

    public void setBroadcastTime(long broadcastTime) {
        this.broadcastTime = broadcastTime;
    }

    @Override
    public String toString() {
        return "Message{" +
                "messageType=" + messageType +
                ", senderId=" + senderId +
                ", batchNumber=" + batchNumber +
                ", data=" + Arrays.toString(data) +
                ", broadcastTime=" + broadcastTime +
                '}';
    }
}
