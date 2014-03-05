/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.ping;

import edu.umass.cs.gns.main.GNS;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

/**
 *
 * @author westy
 */
class PingServer {

  public static void main(String args[]) throws Exception {
    DatagramSocket serverSocket = new DatagramSocket(9876);
    byte[] receiveData = new byte[1024];
    byte[] sendData;
    while (true) {
      DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
      serverSocket.receive(receivePacket);
      String sentence = new String(receivePacket.getData(), receivePacket.getOffset(), receivePacket.getLength());
      GNS.getLogger().info("RECEIVED " + receivePacket.getLength() + " bytes : |" + sentence + "|");
      // send it right back
      InetAddress IPAddress = receivePacket.getAddress();
      int port = receivePacket.getPort();
      sendData = sentence.getBytes();
      GNS.getLogger().info("SENDING " + sendData.length + " bytes : |" + sentence + "|");
      DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, port);
      serverSocket.send(sendPacket);
    }
  }
}
