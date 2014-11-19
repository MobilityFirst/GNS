/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved
 *
 * Initial developer(s): Vijay KP
 * Based on code written in UdpDnsServer.java
 */
package edu.umass.cs.gns.localnameserver.gnamed;

import edu.umass.cs.gns.localnameserver.LocalNameServer;
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

/**
 * This class defines a DnsTranslator that serves DNS requests through UDP.
 *
 * It acts as a DNS translator for DNS requests for records in GNS.
 * @author Vijay
 * @version 1.0
 */
public class DnsTranslator extends Thread implements Shutdownable {
  int port;
  DatagramSocket sock;
  ExecutorService executor = null;

  /**
   * Creates a new <code>DnsTranslator</code> object bound to the given IP/port
   *
   * @param addr IP to bind (0.0.0.0 is acceptable)
   * @param port port to bind (53 is default for DNS)
   *
   * @throws java.net.SocketException
   * @throws java.net.UnknownHostException
   */
  public DnsTranslator(InetAddress addr, int port) throws SecurityException, SocketException, UnknownHostException {
    this.port = port;
    this.sock = new DatagramSocket(port, addr);
    this.executor = Executors.newFixedThreadPool(5);
  }

  @Override
  public void run() {
    GNS.getLogger().info("LNS Node at " + LocalNameServer.getAddress().getHostString()
            + " starting local DNS Translator server on port " + port);
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
          executor.execute(new LookupWorker(sock, incomingPacket, incomingData, null, null, null));
        }
      } catch (IOException e) {
        GNS.getLogger().severe("Error in DNS Translator Server (will sleep for 3 seconds and try again): " + e);
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
