package cs451;

import cs451.Message.Message;

public interface Link {
    public void receive();
    public void send(Message message);
}
