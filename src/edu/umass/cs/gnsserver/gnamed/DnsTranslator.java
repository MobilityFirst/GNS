/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Westy, Emmanuel Cecchet
 *
 */
package edu.umass.cs.gnsserver.gnamed;

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.utils.Shutdownable;
import edu.umass.cs.gnscommon.utils.ThreadUtils;
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

/**
 * This class defines a DnsTranslator that serves DNS requests through UDP.
 *
 * It acts as a DNS translator for DNS requests for records in GNS.
 *
 * @author Vijay
 * @version 1.0
 */
public class DnsTranslator extends Thread implements Shutdownable {

  private final int port;
  private final DatagramSocket sock;
  private ExecutorService executor = null;
  private final ClientRequestHandlerInterface handler;

  /**
   * Creates a new <code>DnsTranslator</code> object bound to the given IP/port
   *
   * @param addr IP to bind (0.0.0.0 is acceptable)
   * @param port port to bind (53 is default for DNS)
   * @param handler
 * @throws SecurityException 
   *
   * @throws java.net.SocketException
   * @throws java.net.UnknownHostException
   */
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
