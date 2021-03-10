package pl.touk.nifi.utils;

import java.io.IOException;
import java.net.ServerSocket;

public class PortFinder {

    public static int getAvailablePort() throws IOException {
        ServerSocket socket = new ServerSocket(0);
        try {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } finally {
            socket.close();
        }
    }
}
