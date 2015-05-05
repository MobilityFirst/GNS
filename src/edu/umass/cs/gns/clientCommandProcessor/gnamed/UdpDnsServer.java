/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 * Based on code written by: Emmanuel Cecchet
 */
package edu.umass.cs.gns.clientCommandProcessor.gnamed;

import edu.umass.cs.gns.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.Shutdownable;
import edu.umass.cs.gns.util.ThreadUtils;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.xbill.DNS.SimpleResolver;
import org.xbill.DNS.Cache;

/**
 * This class defines a UdpDnsServer that serves DNS requests through UDP.
 *
 * DNS requests can be handled just by the GNS server or by the GNS server
 * with a DNS server as a fallback.
 * When using DNS as a fallback we send out parallel requests and whichever returns
 * first is returned to the client as the answer.
 *
 * @author Westy
 * @version 1.0
 */
public class UdpDnsServer extends Thread implements Shutdownable {

  private final SimpleResolver dnsServer;
  private final SimpleResolver gnsServer;
  private final Cache dnsCache;
  private final DatagramSocket sock;
  private ExecutorService executor = null;
  private final String dnsServerIP; // just stored for informational purposes
  private final String gnsServerIP; // just stored for informational purposes
  private final ClientRequestHandlerInterface handler;

  /**
   * Creates a new <code>UDPServer</code> object bound to the given IP/port
   *
   * @param addr IP to bind (0.0.0.0 is acceptable)
   * @param port port to bind (53 is default for DNS)
   * @param dnsServerIP primary DNS Server to forward requests to (make this null
   * if you don't want to forward requests to a DNS server)
   *
   * A typical incantation thus looks like this:
   * <code>new UdpDnsServer(Inet4Address.getByName("0.0.0.0"), 53, "8.8.8.8")</code>
   * @param gnsServerIP
   * @param handler
   * @throws java.net.SocketException
   * @throws java.net.UnknownHostException
   */
  public UdpDnsServer(InetAddress addr, int port, String dnsServerIP, String gnsServerIP, 
          ClientRequestHandlerInterface handler) throws SecurityException, SocketException, UnknownHostException {
    this.dnsServer = dnsServerIP != null ? new SimpleResolver(dnsServerIP) : null;
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
    GNS.getLogger().info("CPP Node at starting local DNS Server on port " + sock.getLocalPort() + " with GNS server at "
            + gnsServerIP + " and fallback DNS server at " + dnsServerIP);
    if (NameResolution.debuggingEnabled) {
      GNS.getLogger().warning("******** DEBUGGING IS ENABLED IN edu.umass.cs.gns.localnameserver.gnamed.NameResolution *********");
    }
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
        GNS.getLogger().severe("Error in UDP Server (will sleep for 3 seconds and try again): " + e);
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
