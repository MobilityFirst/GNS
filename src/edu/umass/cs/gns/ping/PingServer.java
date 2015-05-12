/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.ping;

import edu.umass.cs.gns.main.GNS;

import edu.umass.cs.gns.nodeconfig.GNSConsistentNodeConfig;
import edu.umass.cs.gns.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;
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
  private final GNSConsistentNodeConfig<NodeIDType> nodeConfig;
  private DatagramSocket serverSocket;
  private boolean shutdown = false;

  public PingServer(final NodeIDType nodeID, final GNSConsistentNodeConfig<NodeIDType> nodeConfig) {
    this.nodeID = nodeID;
    this.nodeConfig = nodeConfig;
  }


  @Override
  public void run() {

    try {
      serverSocket = new DatagramSocket(nodeID == null ? GNS.DEFAULT_CPP_PING_PORT : nodeConfig.getPingPort(nodeID));
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
