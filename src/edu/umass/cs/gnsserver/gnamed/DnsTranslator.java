
package edu.umass.cs.gnsserver.gnamed;

import edu.umass.cs.gnscommon.utils.ThreadUtils;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.utils.GNSShutdownable;
import edu.umass.cs.utils.DelayProfiler;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;


public class DnsTranslator extends Thread implements GNSShutdownable {

  private final int port;
  private final DatagramSocket sock;
  private ExecutorService executor = null;
  private final ClientRequestHandlerInterface handler;


  public DnsTranslator(InetAddress addr, int port, ClientRequestHandlerInterface handler) throws SecurityException, SocketException, UnknownHostException {
    this.port = port;
    this.sock = new DatagramSocket(port, addr);
    this.executor = Executors.newFixedThreadPool(5);
    this.handler = handler;
  }

  @Override
  public void run() {
    NameResolution.getLogger().log(Level.INFO,
            "CCP Node starting local DNS Translator server on port {0}", port);
    while (true) {
      try {
        final short udpLength = 512;
        while (true) {
          byte[] incomingData = new byte[udpLength];
          DatagramPacket incomingPacket = new DatagramPacket(incomingData, incomingData.length);
          // Read the incoming request
          incomingPacket.setLength(incomingData.length);
          try {
            sock.receive(incomingPacket);
          } catch (InterruptedIOException e) {
            continue;
          }
          executor.execute(new LookupWorker(sock, incomingPacket, incomingData, null, null, null, handler));
          NameResolution.getLogger().fine(DelayProfiler.getStats());
        }
      } catch (IOException e) {
        NameResolution.getLogger().log(Level.SEVERE, 
                "Error in DNS Translator Server (will sleep for 3 seconds and try again): {0}", e);
        ThreadUtils.sleep(3000);
      }
    }
  }

  @Override
  public void shutdown() {
    if (executor != null) {
      executor.shutdown();
    }
  }
}
