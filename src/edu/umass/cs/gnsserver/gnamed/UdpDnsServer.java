
package edu.umass.cs.gnsserver.gnamed;

import edu.umass.cs.gnscommon.utils.ThreadUtils;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.utils.Shutdownable;
import org.xbill.DNS.Cache;
import org.xbill.DNS.SimpleResolver;

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


public class UdpDnsServer extends Thread implements Shutdownable {

  private final SimpleResolver dnsServer;
  private final SimpleResolver gnsServer;
  private final Cache dnsCache;
  private final DatagramSocket sock;
  private ExecutorService executor = null;
  private final String dnsServerIP; // just stored for informational purposes
  private final String gnsServerIP; // just stored for informational purposes
  private final ClientRequestHandlerInterface handler;


  public UdpDnsServer(InetAddress addr, int port, String dnsServerIP, String gnsServerIP,
          ClientRequestHandlerInterface handler) throws SecurityException, SocketException, UnknownHostException {
	// set it to null to make it non-recursive
    this.dnsServer = null; //dnsServerIP != null ? new SimpleResolver(dnsServerIP) : null;
    this.gnsServer = gnsServerIP != null ? new SimpleResolver(gnsServerIP) : null;
    this.dnsCache = dnsServerIP != null ? new Cache() : null;
    this.dnsServerIP = dnsServerIP;
    this.gnsServerIP = gnsServerIP;
    this.sock = new DatagramSocket(port, addr);
    this.executor = Executors.newFixedThreadPool(5);
    this.handler = handler;
  }

  @Override
  public void run() {
    NameResolution.getLogger().log(Level.INFO,
            "Starting local DNS Server on port {0}{1}fallback DNS server at {2}",
            new Object[]{sock.getLocalPort(),
              gnsServerIP != null ? (" with GNS server at " + gnsServerIP + " and ") : " with ", dnsServerIP});
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
          executor.execute(new LookupWorker(sock, incomingPacket, incomingData, gnsServer, dnsServer, dnsCache, handler));
        }
      } catch (IOException e) {
        NameResolution.getLogger().log(Level.SEVERE, 
                "Error in UDP Server (will sleep for 3 seconds and try again): {0}", e);
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
