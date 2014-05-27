/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.clientsupport;

import edu.umass.cs.gns.localnameserver.LocalNameServer;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.packet.LNSToNSCommandPacket;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static edu.umass.cs.gns.nsdesign.packet.Packet.getPacketType;

/**
 * Provides methods for sending commands to a NameServer.
 *
 * @author westy
 */
public class LNSToNSCommandRequestHandler {
  
  private static final Object monitor = new Object();
  private static final ConcurrentMap<Integer, LNSToNSCommandPacket> resultsMap = new ConcurrentHashMap<>(10, 0.75f, 3);
  private static final Random randomID = new Random();
  
  /**
   * Sends a command to the LNS command handler which forwards it on to an appropriate Name Server.
   * 
   * @param command
   * @return 
   */
  public static String sendCommandRequest(JSONObject command) {
    int id = nextRequestID();
    return sendCommandHelper(id, new LNSToNSCommandPacket(id, LocalNameServer.getNodeID(), command)); 
  }
  
  private static String sendCommandHelper(int id, LNSToNSCommandPacket commandPacket) {
    try {
      GNS.getLogger().info("Sending CommandPacket #" + id + " to Intercessor");
      Intercessor.injectPacketIntoLNSQueue(commandPacket.toJSONObject());
    } catch (JSONException e) {
      GNS.getLogger().warning("Ignoring JSON error while sending Command request: " + e);
      e.printStackTrace();
      return null;
    }
    waitForResponsePacket(id);
    LNSToNSCommandPacket packet = resultsMap.get(id);
    return packet.getReturnValue();
  }
  
  /**
   * Typically called by the Intercessor to handle incoming (returning from NS) CommandRequest packets.
   * 
   * @param json 
   */
  public static void processCommandResponsePackets(JSONObject json) {
    try {
      switch (getPacketType(json)) {
        case LNS_TO_NS_COMMAND:
          try {
            LNSToNSCommandPacket response = new LNSToNSCommandPacket(json);
            int id = response.getRequestId();
            GNS.getLogger().info("Processing returning CommandPacket for " + id);
            synchronized (monitor) {
              resultsMap.put(id, response);
              monitor.notifyAll();
            }
          } catch (JSONException e) {
            GNS.getLogger().warning("JSON error during CommandPacket processing: " + e);
          }
          break;
      }
    } catch (JSONException e) {
      GNS.getLogger().warning("JSON error while getting packet type: " + e);
    }
  }
  
  private static void waitForResponsePacket(int sequenceNumber) {
    try {
      synchronized (monitor) {
        while (!resultsMap.containsKey(sequenceNumber)) {
          monitor.wait();
        }
      }
    } catch (InterruptedException x) {
      GNS.getLogger().severe("Wait for update success confirmation packet was interrupted " + x);
    }
  }
  
  private static int nextRequestID() {
    int id;
    do {
      id = randomID.nextInt();
    } while (resultsMap.containsKey(id));
    return id;
  }
  
  public static String Version = "$Revision$";
}
