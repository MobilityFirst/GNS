/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.ping;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.utils.Util;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 * @author westy
 */
class PingClient {

  private DatagramSocket clientSocket;
  private final Object monitor = new Object();
  // Map between id a RTT time. Get will be null until the packet identified by id has come back.
  private ConcurrentMap<Integer, Long> queryResultMap = new ConcurrentHashMap<Integer, Long>(10, 0.75f, 3);
  // Records the send time of each request
  private ConcurrentMap<Integer, Long> queryTimeStamp = new ConcurrentHashMap<Integer, Long>(10, 0.75f, 3);
  private Random randomID = new Random();

  public PingClient() throws SocketException {
    clientSocket = new DatagramSocket();
    startReceiveThread();
  }

  private long sendPing() throws IOException {
    InetAddress IPAddress = InetAddress.getByName("localhost");
    byte[] sendData;
    int id = nextRequestID();
    String sendString = Integer.toString(id);
    sendData = sendString.getBytes();
    GNS.getLogger().info("SENDING " + sendData.length + " bytes : |" + sendString + "|");
    DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, 9876);
    queryTimeStamp.put(id, System.currentTimeMillis());
    clientSocket.send(sendPacket);
    GNS.getLogger().info("Sent packet for " + id);
    waitForResponsePacket(id);
    return queryResultMap.get(id);
  }

  private void receiveResponses() {
    while (true) {
      try {
        byte[] receiveData = new byte[1024];
        DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
        clientSocket.receive(receivePacket);
        Long receivedTime = System.currentTimeMillis();
        String receivedString = new String(receivePacket.getData(), receivePacket.getOffset(), receivePacket.getLength());
        GNS.getLogger().info("RECEIVED " + receivePacket.getLength() + " bytes : |" + receivedString + "|");
        int id = Integer.parseInt(receivedString);
        processPingResponse(id, receivedTime);
        //clientSocket.close();
      } catch (IOException e) {
        GNS.getLogger().info("Error waiting for response " + e);
        Util.sleep(2000); // sleep a bit so we don't grind to a halt on perpetual errors
      } catch (NumberFormatException e) {
        GNS.getLogger().info("Error parsing response " + e);
      }
    }
  }

  private void waitForResponsePacket(int sequenceNumber) {
    try {
      synchronized (monitor) {
        while (!queryResultMap.containsKey(sequenceNumber)) {
          monitor.wait();
        }
      }
    } catch (InterruptedException x) {
      GNS.getLogger().severe("Wait for packet was interrupted " + x);
    }
  }

  private void processPingResponse(int id, long receivedTime) {
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

  public static void main(String args[]) throws Exception {
    PingClient pingClient = new PingClient();
    while (true) {
      GNS.getLogger().info("RTT = " + pingClient.sendPing());
      Util.sleep(1000);
    }
  }

  private int nextRequestID() {
    int id;
    do {
      id = randomID.nextInt();
    } while (queryResultMap.containsKey(id));
    return id;
  }
}
