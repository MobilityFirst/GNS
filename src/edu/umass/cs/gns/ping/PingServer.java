/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.ping;

import edu.umass.cs.gns.main.GNS;

import edu.umass.cs.gns.nsdesign.GNSNodeConfig;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

/**
 * The PingServer class handles the client side of the GNS ping protocol.
 * The PingClient class handles the flip side of this protocol.
 * 
 * @author westy
 */
public class PingServer extends Thread{

  private final int nodeID;
  private final GNSNodeConfig gnsNodeConfig;
  private DatagramSocket serverSocket;
  private boolean shutdown = false;

  public PingServer(final int nodeID, final GNSNodeConfig gnsNodeConfig) {
    this.nodeID = nodeID;
    this.gnsNodeConfig = gnsNodeConfig;
  }


  @Override
  public void run() {

    try {
      serverSocket = new DatagramSocket(gnsNodeConfig.getPingPort(nodeID));
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

  public static void main(String args[]) throws Exception {
    String configFile = args[0];
    int nodeID = 0;
    GNSNodeConfig gnsNodeConfig = GNSNodeConfig.CreateGNSNodeConfigFromOldStyleFile(configFile, nodeID);
    PingServer pingServer = new PingServer(nodeID, gnsNodeConfig);
    new Thread(pingServer).start();
//    startServer();
  }


}
