/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.clientsupport;

import edu.umass.cs.gns.localnameserver.LocalNameServer;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.packet.CommandPacket;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static edu.umass.cs.gns.nsdesign.packet.Packet.getPacketType;

/**
 *
 * @author westy
 */
public class CommandRequestHandler {
  
  private static final Object monitor = new Object();
  private static ConcurrentMap<Integer, CommandPacket> resultsMap = new ConcurrentHashMap<Integer, CommandPacket>(10, 0.75f, 3);
  private static Random randomID = new Random();
  
  public static String sendCommandRequest(JSONObject command) {
    int id = nextRequestID();
    return sendCommandHelper(id, new CommandPacket(id, LocalNameServer.getNodeID(), command)); 
  }
  
  public static String sendCommandHelper(int id, CommandPacket commandPacket) {
    try {
      Intercessor.injectPacketIntoLNSQueue(commandPacket.toJSONObject());
    } catch (JSONException e) {
      GNS.getLogger().warning("Ignoring JSON error while sending Command request: " + e);
      e.printStackTrace();
      return null;
    }
    waitForResponsePacket(id);
    CommandPacket packet = resultsMap.get(id);
    if (!packet.getResponseCode().isAnError()) {
      return Defs.OKRESPONSE;
    } else {
      return Defs.BADRESPONSE + " " + packet.getResponseCode().getProtocolCode() + " " + packet.getErrorMessage();
    }
  }
  
  public static void processCommandResponsePackets(JSONObject json) {
    try {
      switch (getPacketType(json)) {
        case COMMAND:
          try {
            CommandPacket response = new CommandPacket(json);
            int id = response.getRequestId();
            GNS.getLogger().fine("Processing CommandPacket for " + id);
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
