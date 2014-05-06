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
import edu.umass.cs.gns.util.ThreadUtils;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;

/**
 * The PingServer class handles the client side of the GNS ping protocol.
 * The PingClient class handles the flip side of this protocol.
 * 
 * @author westy
 */
public class PingServer {

  /**
   * Starts a thread which handles ping requests sent from the client.
   * 
   * @param nodeID 
   */
  public static void startServerThread(final int nodeID, final GNSNodeConfig gnsNodeConfig) {
    (new Thread() {
      @Override
      public void run() {
        startServer(nodeID, gnsNodeConfig);
      }
    }).start();
  }

  private static void startServer(int nodeID, GNSNodeConfig gnsNodeConfig) {
    try {
      DatagramSocket serverSocket = new DatagramSocket(gnsNodeConfig.getPingPort(nodeID));
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
          GNS.getLogger().severe("Error receiving ping packet " + e);
          ThreadUtils.sleep(2000);
        }
      }
    } catch (SocketException e) {
      GNS.getLogger().severe("Error creating DatagramSocket " + e);
    }
  }

  public static void main(String args[]) throws Exception {
    String configFile = args[0];
    int nodeID = 0;
    GNSNodeConfig gnsNodeConfig = new GNSNodeConfig(configFile, nodeID);
    startServer(nodeID, gnsNodeConfig);
  }
}
