/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.localnameserver.gnamed;

import edu.umass.cs.gns.localnameserver.LocalNameServer;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.util.ThreadUtils;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.xbill.DNS.SimpleResolver;

/**
 * This class defines a UdpDnsServer that serves DNS requests through UDP
 *
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class UdpDnsServer extends Thread {

  private final SimpleResolver dnsServer;
  DatagramSocket sock;
  //private GnsCredentials gnsCredentials;

  /**
   * Creates a new <code>UDPServer</code> object bound to the given IP/port
   *
   * @param addr IP to bind (0.0.0.0 is acceptable)
   * @param port port to bind (53 is default for DNS)
   * @param dnsServerIP primary DNS Server to forward requests to
   *
   * A typical incantation thus looks like this:
   * <code>new UdpDnsServer(Inet4Address.getByName("0.0.0.0"), 53, "8.8.8.8")</code>
   * @throws java.net.SocketException
   * @throws java.net.UnknownHostException
   */
  public UdpDnsServer(InetAddress addr, int port, String dnsServerIP) throws SecurityException, SocketException, UnknownHostException {
    this.dnsServer = new SimpleResolver(dnsServerIP);
    this.sock = new DatagramSocket(port, addr);
  }

  @Override
  public void run() {
    GNS.getLogger().info("LNS Node at " + LocalNameServer.getAddress() + " starting DNS Server on port " + sock.getLocalPort());
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
          new NameResolutionThread(sock, incomingPacket, incomingData, dnsServer).start();
        }
      } catch (IOException e) {
        GNS.getLogger().severe("Error in UDP Server (will sleep for 3 seconds and try again): " + e);
        ThreadUtils.sleep(3000);
      }
      // sleep 3 seconds and try again
      
    }
  }

}
