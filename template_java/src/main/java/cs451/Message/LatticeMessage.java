package cs451.Message;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

public class LatticeMessage {
    byte messageType;
    int senderId;
    int relayId;
    int proposalNumber;
    int setSize;
    Set<Integer> proposalValue;

    public LatticeMessage(byte messageType, int senderId, int relayId, int proposalNumber, int setSize, Set<Integer> proposalValue) {
        this.messageType = messageType;
        this.senderId = senderId;
        this.relayId = relayId;
        this.proposalNumber = proposalNumber;
        this.setSize = setSize;
        this.proposalValue = proposalValue;
    }

    public byte[] serialize() {
        // Calculate the size of the byte array
        int size = Byte.BYTES + // messageType
                Integer.BYTES + // senderId
                Integer.BYTES + // relayId
                Integer.BYTES + // proposalNumber
                Integer.BYTES + // setSize
                Integer.BYTES * proposalValue.size(); // proposalValue elements

        ByteBuffer buffer = ByteBuffer.allocate(size);

        // Serialize fields
        buffer.put(messageType);
        buffer.putInt(senderId);
        buffer.putInt(relayId);
        buffer.putInt(proposalNumber);
        buffer.putInt(setSize);

        // Serialize proposalValue set
        for (Integer value : proposalValue) {
            buffer.putInt(value);
        }

        return buffer.array();
    }

    public static LatticeMessage deserialize(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);

        // Deserialize fields
        byte messageType = buffer.get();
        int senderId = buffer.getInt();
        int relayId = buffer.getInt();
        int proposalNumber = buffer.getInt();
        int setSize = buffer.getInt();

        // Deserialize proposalValue set
        Set<Integer> proposalValue = new HashSet<>();
        for (int i = 0; i < setSize; i++) {
            proposalValue.add(buffer.getInt());
        }

        return new LatticeMessage(messageType, senderId, relayId, proposalNumber, setSize, proposalValue);
    }

    public static byte deserializeType(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        return buffer.get();
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

    public int getRelayId() {
        return relayId;
    }

    public void setRelayId(int relayId) {
        this.relayId = relayId;
    }

    public int getProposalNumber() {
        return proposalNumber;
    }

    public void setProposalNumber(int proposalNumber) {
        this.proposalNumber = proposalNumber;
    }

    public int getSetSize() {
        return setSize;
    }

    public void setSetSize(int setSize) {
        this.setSize = setSize;
    }

    public Set<Integer> getProposalValue() {
        return proposalValue;
    }

    public void setProposalValue(Set<Integer> proposalValue) {
        this.proposalValue = proposalValue;
    }

    @Override
    public String toString() {
        return "LatticeMessage{" +
                "messageType=" + messageType +
                ", senderId=" + senderId +
                ", relayId=" + relayId +
                ", proposalNumber=" + proposalNumber +
                ", setSize=" + setSize +
                ", proposalValue=" + proposalValue +
                '}';
    }
}


