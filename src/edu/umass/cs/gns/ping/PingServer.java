/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.ping;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.utils.Util;
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
  public static void startServerThread(final int nodeID) {
    (new Thread() {
      @Override
      public void run() {
        startServer(nodeID);
      }
    }).start();
  }

  private static void startServer(int nodeID) {
    try {
      DatagramSocket serverSocket = new DatagramSocket(ConfigFileInfo.getPingPort(nodeID));
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
          Util.sleep(2000);
        }
      }
    } catch (SocketException e) {
      GNS.getLogger().severe("Error creating DatagramSocket " + e);
    }
  }

  public static void main(String args[]) throws Exception {
    String configFile = args[0];
    NameServer.setNodeID(0);
    ConfigFileInfo.readHostInfo(configFile, NameServer.getNodeID());
    startServer(0);
  }
}
