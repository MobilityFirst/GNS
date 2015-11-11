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
package edu.umass.cs.gnsclient.client;

import edu.umass.cs.gnsclient.client.util.ServerSelectDialog;
import edu.umass.cs.gnscommon.utils.ThreadUtils;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.net.InetSocketAddress;

/**
 * The PingClient class handles the client side of the GNSClient ping protocol.
 * The PingServer class handles the flip side of this protocol.
 * 
 * @author westy
 */
public class PingClient {
  
  private static int port = 24409;

  private DatagramSocket clientSocket;
  private final Object monitor = new Object();
  // Map between id a RTT time. Get will be null until the packet identified by id has come back.
  private ConcurrentMap<Integer, Long> queryResultMap = new ConcurrentHashMap<Integer, Long>(10, 0.75f, 3);
  // Records the send time of each request
  private ConcurrentMap<Integer, Long> queryTimeStamp = new ConcurrentHashMap<Integer, Long>(10, 0.75f, 3);
  private Random randomID = new Random();

  public PingClient() {
    try {
      clientSocket = new DatagramSocket();
      startReceiveThread();
    } catch (SocketException e) {
      GNSClient.getLogger().severe("Error creating Datagram socket " + e);
    }
  }

  /**
   * Sends a ping request to the host.
   * 
   * @param nodeId
   * @return the round trip time or -1L if the request times out
   * @throws IOException 
   */
  public long sendPing(String host) throws IOException {
    InetAddress IPAddress = InetAddress.getByName(host);
    byte[] sendData;
    // make an id and turn it into a string for sending out
    int id = nextRequestID();
    String sendString = Integer.toString(id);
    sendData = sendString.getBytes();
    DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, port);
    // record the send time
    queryTimeStamp.put(id, System.currentTimeMillis());
    clientSocket.send(sendPacket);
    GNSClient.getLogger().fine("SENT to " + host + ":" + port + " " + sendData.length + " bytes : |" + sendString + "|");
    waitForResponsePacket(id);
    long result = queryResultMap.get(id);
    if (result == -1L) {
      GNSClient.getLogger().fine("TIMEOUT for send to " + host + ":" + port);
    }
    queryResultMap.remove(id);
    return result;
  }

  // handles ping responses
  private void receiveResponses() {
    GNSClient.getLogger().fine("Starting receive response loop");
    while (true) {
      try {
        byte[] receiveData = new byte[1024];
        DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
        GNSClient.getLogger().fine("Looking for response");
        clientSocket.receive(receivePacket);
        Long receivedTime = System.currentTimeMillis();
        String receivedString = new String(receivePacket.getData(), receivePacket.getOffset(), receivePacket.getLength());
        GNSClient.getLogger().fine("RECEIVED " + receivePacket.getLength() + " bytes : |" + receivedString + "|");
        // grab the requested id from the received packet
        int id = Integer.parseInt(receivedString);
        processPingResponse(id, receivedTime);
      } catch (IOException e) {
        GNSClient.getLogger().severe("Error waiting for response " + e);
        ThreadUtils.sleep(2000); // sleep a bit so we don't grind to a halt on perpetual errors
      } catch (NumberFormatException e) {
        GNSClient.getLogger().severe("Error parsing response " + e);
      }
    }
  }
  
  private static final int TIMEOUT = 10000;

  private void waitForResponsePacket(int id) {
    GNSClient.getLogger().fine("Sent packet for " + id + ", waiting for response");
    try {
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
    } catch (InterruptedException x) {
      GNSClient.getLogger().severe("Wait for packet was interrupted " + x);
    }
  }

  // updates the result map with the rtound trip time of the packet
  private void processPingResponse(int id, long receivedTime) {
    GNSClient.getLogger().fine("Processing response for " + id);
    synchronized (monitor) {
      Long timeDif = receivedTime - queryTimeStamp.get(id);
      queryResultMap.put(id, timeDif);
      monitor.notifyAll();
    }
  }

  private void startReceiveThread() {
    (new Thread() {
      @Override
      public void run() {
        receiveResponses();
      }
    }).start();
  }

  private int nextRequestID() {
    int id;
    do {
      id = randomID.nextInt();
    } while (queryResultMap.containsKey(id));
    return id;
  }

  public static void main(String args[]) throws Exception {
    InetSocketAddress address = ServerSelectDialog.selectServer();
    PingClient pingClient = new PingClient();
    while (true) {
      GNSClient.getLogger().info("RTT = " + pingClient.sendPing(address.getHostName()));
      ThreadUtils.sleep(1000);
    }
  }
}
