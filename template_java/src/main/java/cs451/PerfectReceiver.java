package cs451;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PerfectReceiver {
    RunConfig runConfig;
    public PerfectReceiver(RunConfig runConfig) {
        this.runConfig = runConfig;
    }

    public void receive() throws IOException {
        byte[] buffer = new byte[Constants.UDP_PACKET_SIZE];
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
        while (true) {
            runConfig.getSocket().receive(packet);
//            packet.getData()
        }

    }

    public static void main(String[] args) throws Exception {
//        perfectLinkMain(args);
//        BEBMain(args);
//        latticeAgreementMain(args);

//        ExecutorService executor;
        try {
            ExecutorService executor = Executors.newFixedThreadPool(3);

        } catch (Exception e) {
            System.err.println(e.getMessage());
        }

    }


}
