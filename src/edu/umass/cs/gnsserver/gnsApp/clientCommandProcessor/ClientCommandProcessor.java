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
 *  Initial developer(s): Abhigyan Sharma, Westy
 *
 */
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor;

import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.Admintercessor;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.Intercessor;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.CCPListenerAdmin;
import edu.umass.cs.gnsserver.gnamed.DnsTranslator;
import edu.umass.cs.gnsserver.gnamed.UdpDnsServer;
import edu.umass.cs.gnsserver.httpserver.GnsHttpServer;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.gnsApp.GnsApp;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import java.io.IOException;
import java.util.logging.Logger;

import edu.umass.cs.gnsserver.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gnsserver.utils.Shutdownable;
import edu.umass.cs.gnsserver.ping.PingManager;
import edu.umass.cs.nio.JSONMessenger;

import java.net.BindException;
import java.net.Inet4Address;
import java.net.InetSocketAddress;

import org.json.JSONObject;

/**
 * 
 * @author Westy
 */
public class ClientCommandProcessor implements Shutdownable {

  private final InetSocketAddress nodeAddress;
  /**
   * Handles the client support processing for the CCP.
   */
  private Intercessor intercessor;

  /**
   * Handles administrative client support for the CCP.
   */
  private Admintercessor admintercessor;
  /**
   * Ping manager object for pinging other nodes and updating ping latencies.
   */
  private PingManager<String> pingManager;
  /**
   * We also keep a pointer to the lnsListenerAdmin so we can shut it down.
   */
  private CCPListenerAdmin ccpListenerAdmin;

  ClientRequestHandlerInterface requestHandler;

  /**
   * We keep a pointer to the udpDnsServer so we can shut it down.
   */
  private UdpDnsServer udpDnsServer;

  /**
   * We keep a pointer to the dnsTranslator so we can shut it down.
   */
  private DnsTranslator dnsTranslator;

  CCPPacketDemultiplexer<String> demultiplexer;

  private final Logger log = Logger.getLogger(getClass().getName());

  /**
   * Create and instance of the ClientCommandProcessor.
   * 
   * @param messenger
   * @param nodeAddress
   * @param nodeConfig
   * @param debug
   * @param app
   * @param replicaID
   * @param dnsGnsOnly
   * @param dnsOnly
   * @param gnsServerIP
   * @throws IOException
   */
  public ClientCommandProcessor(JSONMessenger<String> messenger,
          InetSocketAddress nodeAddress,
          GNSNodeConfig<String> nodeConfig,
          boolean debug,
          GnsApp app,
          String replicaID,
          boolean dnsGnsOnly,
          boolean dnsOnly,
          String gnsServerIP) throws IOException {

    if (debug) {
      System.out.println("******** DEBUGGING IS ENABLED IN THE CCP *********");
    }
    this.demultiplexer = new CCPPacketDemultiplexer<>();
    this.intercessor = new Intercessor(nodeAddress, nodeConfig, demultiplexer);
    this.admintercessor = new Admintercessor();
    this.nodeAddress = nodeAddress;

    System.out.println("BIND ADDRESS for " + replicaID + " is " + nodeConfig.getBindAddress(replicaID));
    System.out.println("NODE ADDRESS for " + replicaID + " is " + nodeConfig.getNodeAddress(replicaID));
    RequestHandlerParameters parameters = new RequestHandlerParameters();
    parameters.setDebugMode(debug);
    
    try {
      messenger.addPacketDemultiplexer(demultiplexer);
      this.requestHandler = new ClientRequestHandler(intercessor, admintercessor, nodeAddress,
              replicaID,
              app,
              nodeConfig, messenger, parameters);
      demultiplexer.setHandler(requestHandler);
      // Start HTTP server
      GnsHttpServer.runHttp(requestHandler);
//      // Start server
//      GNS.getLogger().info("CCP running at " + nodeAddress + " started Ping server on port "
//              + gnsNodeConfig.getCcpPingPort(replicaID));
//      // Start a ping manager with no client, just a server.
//      this.pingManager = new PingManager<>(replicaID,
//              new GNSConsistentReconfigurableNodeConfig<String>(gnsNodeConfig),
//              true);
      // After starting PingManager because it accesses PingManager.
      (this.ccpListenerAdmin = new CCPListenerAdmin(requestHandler, pingManager)).start();

      // The Admintercessor needs to use the CCPListenerAdmin;
      this.admintercessor.setListenerAdmin(ccpListenerAdmin);

      try {
        if (dnsGnsOnly) {
          //if (options.containsKey(DNS_GNS_ONLY)) {
          dnsTranslator = new DnsTranslator(Inet4Address.getByName("0.0.0.0"), 53, requestHandler);
          dnsTranslator.start();
        } else if (dnsOnly) {
          //} else if (options.containsKey(DNS_ONLY)) {
          if (gnsServerIP == null) {
            //if (options.get(GNS_SERVER_IP) == null) {
            GNS.getLogger().severe("FAILED TO START DNS SERVER: GNS Server IP must be specified");
            return;
          }
          GNS.getLogger().info("GNS Server IP" + gnsServerIP);
          //GNS.getLogger().warning("gns server IP" + StartLocalNameServer.gnsServerIP);
          udpDnsServer = new UdpDnsServer(Inet4Address.getByName("0.0.0.0"), 53, "8.8.8.8",
                  gnsServerIP, requestHandler);
          udpDnsServer.start();
        } else {
          udpDnsServer = new UdpDnsServer(Inet4Address.getByName("0.0.0.0"), 53, "8.8.8.8", null, requestHandler);
          udpDnsServer.start();
        }
      } catch (BindException e) {
        GNS.getLogger().warning("Not running DNS Service because it needs root permission! "
                + "If you want DNS run the CCP using sudo.");
      }
    } catch (BindException e) {
      GNS.getLogger().severe("Failed to create nio server at " + nodeAddress + ": " + e + " ;exiting.");
      System.exit(-1);
    }
  }

  /**
   * Adds a JSON packet to the demultiplexer queue.
   * 
   * @param jsonObject
   */
  public void injectPacketIntoCCPQueue(JSONObject jsonObject) {

    boolean isPacketTypeFound = demultiplexer.handleMessage(jsonObject);
    if (isPacketTypeFound == false) {
      GNS.getLogger().severe("Packet type not found at demultiplexer: " + isPacketTypeFound);
    }
  }

  /**
   * Returns the address.
   * 
   * @return the address
   */
  public InetSocketAddress getAddress() {
    return nodeAddress;
  }

  /**
   * Returns the request handler.
   * 
   * @return the request handler
   */
  public ClientRequestHandlerInterface getRequestHandler() {
    return requestHandler;
  }

  @Override
  public void shutdown() {
    if (udpDnsServer != null) {
      udpDnsServer.shutdown();
    }
    if (dnsTranslator != null) {
      dnsTranslator.shutdown();
    }
    if (pingManager != null) {
      pingManager.shutdown();
    }
    if (ccpListenerAdmin != null) {
      ccpListenerAdmin.shutdown();
    }
  }

}
