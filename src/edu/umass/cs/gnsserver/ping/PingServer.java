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
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsserver.ping;


import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.nodeconfig.GNSConsistentNodeConfig;
import edu.umass.cs.gnsserver.nodeconfig.GNSInterfaceNodeConfig;
import edu.umass.cs.gnsserver.nodeconfig.GNSNodeConfig;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

/**
 * The PingServer class handles the server side of the GNS ping protocol.
 * It bounces back the data it gets to the client.
 * The PingClient class handles the flip side of this protocol.
 * 
 * @author westy
 * @param <NodeIDType>
 */
public class PingServer<NodeIDType> extends Thread{

  private final NodeIDType nodeID;
  private final GNSInterfaceNodeConfig<NodeIDType> nodeConfig;
  private DatagramSocket serverSocket;
  private boolean shutdown = false;

  /**
   * Create a PingServer instance.
   * 
   * @param nodeID
   * @param nodeConfig
   */
  public PingServer(final NodeIDType nodeID, final GNSInterfaceNodeConfig<NodeIDType> nodeConfig) {
    this.nodeID = nodeID;
    this.nodeConfig = nodeConfig;
  }


  @Override
  public void run() {

    try {
      serverSocket = new DatagramSocket(nodeID == null ? nodeConfig.getCcpPingPort(nodeID) : nodeConfig.getPingPort(nodeID));
      byte[] receiveData = new byte[1024];
      byte[] sendData;
      while (true) {
        try {
          DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
          serverSocket.receive(receivePacket);
          String sentence = new String(receivePacket.getData(), receivePacket.getOffset(), receivePacket.getLength());
          //GNS.getLogger().fine("RECEIVED " + receivePacket.getLength() + " bytes : |" + sentence + "|");
          // Send it right back
          InetAddress IPAddress = receivePacket.getAddress();
          int port = receivePacket.getPort();
          sendData = sentence.getBytes();
          //GNS.getLogger().fine("SENDING " + sendData.length + " bytes : |" + sentence + "|");
          DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, port);
          serverSocket.send(sendPacket);
        } catch (IOException e) {
          if (isShutdown()) {
            GNS.getLogger().warning("Ping server closing down.");
            return;
          }
          GNS.getLogger().severe("Error receiving ping packet " + e);
          Thread.sleep(2000);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      GNS.getLogger().severe("Error creating DatagramSocket " + e);
    }
  }

  /**
   * Shutdown the ping server.
   */
  public void shutdown() {
    setShutdown();
    serverSocket.close();
  }

  private synchronized void setShutdown() {
    shutdown = true;
  }

  private synchronized boolean isShutdown() {
    return shutdown;
  }

  /**
   * The main routine. For testing only.
   * 
   * @param args
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  public static void main(String args[]) throws Exception {
    String configFile = args[0];
    String nodeID = "0";
    GNSNodeConfig gnsNodeConfig = new GNSNodeConfig(configFile, nodeID);
    GNSConsistentNodeConfig nodeConfig = new GNSConsistentNodeConfig(gnsNodeConfig);
    PingServer pingServer = new PingServer(nodeID, nodeConfig);
    new Thread(pingServer).start();
//    startServer();
  }


}
