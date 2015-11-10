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
import java.net.SocketException;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The PingClient class handles the client side of the GNS ping protocol.
 * The PingServer class handles the flip side of this protocol.
 *
 * @author westy
 * @param <NodeIDType>
 */
public class PingClient<NodeIDType> {

  private DatagramSocket clientSocket;
  private final Object monitor = new Object();
  // Map between id and RTT time. Get will be null until the packet identified by id has come back.
  private final ConcurrentMap<Integer, Long> queryResultMap = new ConcurrentHashMap<Integer, Long>(10, 0.75f, 3);
  // Records the send time of each request
  private final ConcurrentMap<Integer, Long> queryTimeStamp = new ConcurrentHashMap<Integer, Long>(10, 0.75f, 3);
  private final Random randomID = new Random();
  private final GNSInterfaceNodeConfig<NodeIDType> nodeConfig;
  private Thread receiveThread;
  private boolean shutdown = false;

  /**
   * Create a PingClient instance.
   * 
   * @param nodeConfig
   */
  public PingClient(GNSInterfaceNodeConfig<NodeIDType> nodeConfig) {
    this.nodeConfig = nodeConfig;
    try {
      clientSocket = new DatagramSocket();
      startReceiveThread();
    } catch (SocketException e) {
      GNS.getLogger().severe("Error creating Datagram socket " + e);
    }
  }

  /**
   * Sends a ping request to the node.
   *
   * @param nodeId
   * @return the round trip time or INVALID_INTERVAL if the request times out
   * @throws IOException
   * @throws java.lang.InterruptedException
   */
  public long sendPing(NodeIDType nodeId) throws IOException, InterruptedException {
    InetAddress IPAddress = nodeConfig.getNodeAddress(nodeId);
    int port = nodeConfig.getPingPort(nodeId);
    if (port != GNSNodeConfig.INVALID_PORT) { // sanity checking
      byte[] sendData;
      // make an id and turn it into a string for sending out
      int id = nextRequestID();
      String sendString = Integer.toString(id);
      sendData = sendString.getBytes();
      DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, port);
      // record the send time
      queryTimeStamp.put(id, System.currentTimeMillis());
      clientSocket.send(sendPacket);
      //GNS.getLogger().fine("SENT to " + nodeId + " " + sendData.length + " bytes : |" + sendString + "|");
      waitForResponsePacket(id);
      long result = queryResultMap.get(id);
//    if (result == INVALID_INTERVAL) {
//      GNS.getLogger().fine("TIMEOUT for send to " + nodeId);
//    }
      queryResultMap.remove(id);
      return result;
    } else {
      return GNSNodeConfig.INVALID_PING_LATENCY;
    }
  }

  /**
   * Shuts down this client.
   */
  public void shutdown() {
    this.setShutdown();
    this.clientSocket.close();
  }

  // handles ping responses
  private void receiveResponses() throws InterruptedException {
    GNS.getLogger().fine("Starting receive response loop");
    while (true) {
      try {
        byte[] receiveData = new byte[1024];
        DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
        //GNS.getLogger().fine("Looking for response");
        clientSocket.receive(receivePacket);
        Long receivedTime = System.currentTimeMillis();
        String receivedString = new String(receivePacket.getData(), receivePacket.getOffset(), receivePacket.getLength());
        //GNS.getLogger().fine("RECEIVED " + receivePacket.getLength() + " bytes : |" + receivedString + "|");
        // grab the requested id from the received packet
        int id = Integer.parseInt(receivedString);
        processPingResponse(id, receivedTime);
      } catch (IOException e) {
        if (isShutdown()) {
          GNS.getLogger().warning("Ping client closing down.");
          return;
        }
        GNS.getLogger().severe("Error waiting for response " + e);
        Thread.sleep(2000); // sleep a bit so we don't grind to a halt on perpetual errors
      } catch (NumberFormatException e) {
        GNS.getLogger().severe("Error parsing response " + e);
      }
    }
  }

  private static final int TIMEOUT = 10000;

  private void waitForResponsePacket(int id) throws InterruptedException {
    //GNS.getLogger().fine("Sent packet for " + id + ", waiting for response");
//    try {
//      synchronized (monitor) {
//        while (!queryResultMap.containsKey(id)) {
//          monitor.wait();
//        }
//      }
    synchronized (monitor) {
      long timeoutExpiredMs = System.currentTimeMillis() + TIMEOUT;
      while (!queryResultMap.containsKey(id)) {
        monitor.wait(timeoutExpiredMs - System.currentTimeMillis());
        if (System.currentTimeMillis() >= timeoutExpiredMs) {
          // we timed out... only got partial results{
          queryResultMap.put(id, -1L);
          break;
        }
      }
    }
//    } catch (InterruptedException x) {
//      GNS.getLogger().severe("Wait for packet was interrupted " + x);
//    }
  }

  // updates the result map with the round trip time of the packet
  private void processPingResponse(int id, long receivedTime) {
    //GNS.getLogger().fine("Processing response for " + id);
    synchronized (monitor) {
      Long timeDif = receivedTime - queryTimeStamp.get(id);
      queryResultMap.put(id, timeDif);
      monitor.notifyAll();
    }
  }

  private void startReceiveThread() {
    this.receiveThread = (new Thread() {
      @Override
      public void run() {
        try {
          receiveResponses();
        } catch (InterruptedException e) {
          GNS.getLogger().warning("Ping client closing down.");
        }
      }
    });
    this.receiveThread.start();
  }

  private int nextRequestID() {
    int id;
    do {
      id = randomID.nextInt();
    } while (queryResultMap.containsKey(id));
    return id;
  }

  /**
   * 'synchronized' only for access to the boolean variable. There are no other synchronized methods in the class.
   */
  private synchronized void setShutdown() {
    shutdown = true;
  }

  private synchronized boolean isShutdown() {
    return shutdown;
  }

  /**
   * Main routine. For testing only.
   * @param args
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  public static void main(String args[]) throws Exception {
    String configFile = args[0];
    String nodeID = "0";
    GNSNodeConfig gnsNodeConfig = new GNSNodeConfig(configFile, nodeID);
    GNSConsistentNodeConfig nodeConfig = new GNSConsistentNodeConfig(gnsNodeConfig);
    PingClient pingClient = new PingClient(nodeConfig);
    while (true) {
      GNS.getLogger().info("RTT = " + pingClient.sendPing("0"));
      Thread.sleep(1000);
    }
  }
}
