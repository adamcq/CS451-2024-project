package cs451;

import cs451.Parsers.Parser;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.AbstractMap;
import java.util.HashMap;

public class RunConfig {
    private final DatagramSocket socket; // TODO verify if socket can be final
    private final HashMap<Integer, AbstractMap.SimpleEntry<InetAddress, Integer>> idToAddressPort;
    private final int processId;
    private final LogBuffer logBuffer;
    private final int numberOfMessages;
    private final int numberOfHosts;

    public RunConfig(Parser parser, int numberOfMessages) {
        this.processId = parser.myId();
        this.numberOfMessages = numberOfMessages;

        try {
            this.logBuffer = new LogBuffer(parser.output());
        } catch (IOException e) {
            System.err.println("Creating logBuffer failed");
            throw new RuntimeException(e);
        }

        this.idToAddressPort = new HashMap<>();
        for (Host host : parser.hosts()) {
            try {
                this.idToAddressPort.put(host.getId(), new AbstractMap.SimpleEntry<>(InetAddress.getByName(host.getIp()), host.getPort()));
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        }

        this.numberOfHosts = this.idToAddressPort.size();

        InetAddress broadcasterAddress = this.idToAddressPort.get(parser.myId()).getKey();
        int broadcasterPort = this.idToAddressPort.get(parser.myId()).getValue();

        try {
            this.socket = new DatagramSocket(broadcasterPort, broadcasterAddress); // this works for both sender and receiver, because we put senderId == receiverId for receiver in Main
        } catch (SocketException e) {
            System.err.println("Creating receiver socket failed. Socket is USED!!!\n" + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public DatagramSocket getSocket() {
        return socket;
    }

    public HashMap<Integer, AbstractMap.SimpleEntry<InetAddress, Integer>> getIdToAddressPort() {
        return idToAddressPort;
    }

    public int getProcessId() {
        return processId;
    }

    public LogBuffer getLogBuffer() {
        return logBuffer;
    }

    public int getNumberOfMessages() {
        return numberOfMessages;
    }

    public int getNumberOfHosts() {
        return numberOfHosts;
    }
}
