package cs451;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class PerfectLink {
    PerfectReceiver perfectReceiver;
    PerfectSender perfectSender;

    public PerfectLink(HashMap<Integer, AbstractMap.SimpleEntry<InetAddress, Integer>> idToAddressPort, int receiverId, int senderId, String outputPath, int numberOfMessages) throws Exception {
//        PerfectReceiver perfectReceiver = new PerfectReceiver(receiverId, numberOfMessages, idToAddressPort, outputPath);
        PerfectSender perfectSender = new PerfectSender(senderId, numberOfMessages, idToAddressPort, outputPath);
    }

    public PerfectReceiver getPerfectReceiver() {
        return perfectReceiver;
    }

    public PerfectSender getPerfectSender() {
        return perfectSender;
    }
}