package cs451;

import java.io.IOException;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

class TestLink {
    int receiverPort = 11001;
    long received = 0;
    private volatile boolean running = true;
    InetAddress localhostAddress;

    public TestLink() throws UnknownHostException {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Received " + received + " messages.");
        }));
        localhostAddress = InetAddress.getByName("localhost");
    }

    public void receive() throws IOException {
        byte[] buffer = new byte[4];
        int length = 4;
        DatagramSocket socket = new DatagramSocket(receiverPort, localhostAddress);
        DatagramPacket packet = new DatagramPacket(buffer, length, localhostAddress, receiverPort);
        // Schedule a task to stop the receiver after 20 seconds
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.schedule(() -> running = false, 20, TimeUnit.SECONDS);

        while (running) {
            socket.receive(packet);
            received++;
        }

        // Clean up
        socket.close();
        scheduler.shutdown();
        System.out.println("Receiving stopped after 20 seconds. Total packets received: " + received);
    }

    public void send(int senderPort) throws IOException {
        byte[] buffer = new byte[4];
        int length = 4;
        DatagramSocket socket = new DatagramSocket(senderPort, localhostAddress);
        DatagramPacket packet = new DatagramPacket(buffer, length, localhostAddress, receiverPort);
        while (true) {
            socket.send(packet);
        }
    }
}

public class ThroughputTest {

    public static void main(String[] args) throws IOException {
        if (args == null)
            System.exit(0);
        TestLink link = new TestLink();
        if (args[0].equals("r")) {
            link.receive();
        } else if (args[0].equals("s")) {
            int senderPort = Integer.parseInt(args[1]);
            link.send(senderPort);
        } else {
            System.exit(1);
        }
    }
}

