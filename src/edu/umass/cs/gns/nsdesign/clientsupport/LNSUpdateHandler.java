/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.nsdesign.clientsupport;

import edu.umass.cs.gns.clientsupport.UpdateOperation;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurableInterface;
import edu.umass.cs.gns.util.NameRecordKey;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurable;
import edu.umass.cs.gns.nsdesign.packet.AddRecordPacket;
import edu.umass.cs.gns.nsdesign.packet.ConfirmUpdatePacket;
import edu.umass.cs.gns.nsdesign.packet.RemoveRecordPacket;
import edu.umass.cs.gns.nsdesign.packet.UpdatePacket;
import edu.umass.cs.gns.util.NSResponseCode;
import edu.umass.cs.gns.util.ResultValue;
import org.json.JSONException;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * This class handles sending Update requests from a NameServer back to an Local Name Server.
 *
 * Currently it is used by the code that does Group Guid maintenence.
 *
 *
 * @author westy
 */
public class LNSUpdateHandler {

  private static final Object monitor = new Object();
  private static ConcurrentMap<Integer, NSResponseCode> updateResultMap = new ConcurrentHashMap<Integer, NSResponseCode>(10, 0.75f, 3);
  private static ConcurrentMap<Integer, Integer> outStandingUpdates = new ConcurrentHashMap<Integer, Integer>(10, 0.75f, 3);
  private static Random randomID = new Random();

  /**
   * Sends an update request from this Name Server to a Local Name Server
   *
   * @param name
   * @param key
   * @param newValue
   * @param operation
   * @param activeReplica
   * @return
   */
  public static NSResponseCode sendUpdate(String name, String key, ResultValue newValue, UpdateOperation operation, GnsReconfigurableInterface activeReplica) {
    return sendUpdate(name, key, newValue, null, -1, operation, activeReplica);
  }

  /**
   * Sends an update request from this Name Server to a Local Name Server
   *
   * @param name
   * @param key
   * @param newValue
   * @param oldValue
   * @param argument
   * @param operation
   * @param activeReplica
   * @return
   */
  public static NSResponseCode sendUpdate(String name, String key, ResultValue newValue,
          ResultValue oldValue, int argument, UpdateOperation operation, GnsReconfigurableInterface activeReplica) {
    GNS.getLogger().fine("Node " + activeReplica.getNodeID() + "; Sending update: " + name + " : " + key + "->" + newValue.toString());
    int id = nextRequestID();
    // use this to filter out everything but the first responder
    outStandingUpdates.put(id, id);
    // activeReplica.getGNSNodeConfig() is a hack to get the first LNS
    // We need a means to find the closes LNS
    sendUpdateInternal(id, LNSQueryHandler.pickClosestLNServer(activeReplica), name, key, newValue, oldValue, argument, operation, activeReplica);
    // now we wait until the packet comes back
    waitForResponsePacket(id);
    NSResponseCode result = updateResultMap.get(id);
    updateResultMap.remove(id);
    return result;
  }

  private static void sendUpdateInternal(int updateId, int recipientId, String name, String key, ResultValue newValue,
          ResultValue oldValue, int argument, UpdateOperation operation, GnsReconfigurableInterface activeReplica) {
    UpdatePacket packet = new UpdatePacket(activeReplica.getNodeID(), updateId,
            name, new NameRecordKey(key), newValue, oldValue, argument, operation,
            -1, GNS.DEFAULT_TTL_SECONDS,
            null, null, null);
    try {
      GNS.getLogger().fine("########## Node " + activeReplica.getNodeID() + "; Sending update " + updateId + " to " + recipientId
              + "(" + activeReplica.getGNSNodeConfig().getNodeAddress(recipientId)
              + ":" + activeReplica.getGNSNodeConfig().getNodePort(recipientId) + ")"
              + " for " + name + " / " + key + ": " + packet.toJSONObject());
      activeReplica.getNioServer().sendToID(recipientId, packet.toJSONObject());
      //Packet.sendTCPPacket(activeReplica.getGNSNodeConfig(), packet.toJSONObject(), recipientId, GNS.PortType.LNS_TCP_PORT);
    } catch (JSONException e) {
      GNS.getLogger().severe("Problem converting packet to JSON Object:" + e);
    } catch (IOException e) {
      GNS.getLogger().severe("Problem sending packet to NS " + recipientId + ": " + e);
    }
  }

  public static NSResponseCode sendAddRecord(String name, String key, ResultValue value, GnsReconfigurableInterface activeReplica) {
    int id = nextRequestID();
    outStandingUpdates.put(id, id);
    int recipientId = LNSQueryHandler.pickClosestLNServer(activeReplica);
    GNS.getLogger().fine("++++++++++ Node " + activeReplica.getNodeID() + "; Sending add: " + name + " : " + key +"->" + value + " to LNS " + recipientId);
    AddRecordPacket packet = new AddRecordPacket(activeReplica.getNodeID(), id, name, new NameRecordKey(key), value, -1, GNS.DEFAULT_TTL_SECONDS);
    try {
      activeReplica.getNioServer().sendToID(recipientId, packet.toJSONObject());
    } catch (JSONException e) {
      GNS.getLogger().severe("Problem converting packet to JSON Object:" + e);
    } catch (IOException e) {
      GNS.getLogger().severe("Problem sending packet to NS " + recipientId + ": " + e);
    }
    waitForResponsePacket(id);
    NSResponseCode result = updateResultMap.get(id);
    updateResultMap.remove(id);
    return result;
  }

  public static NSResponseCode sendRemoveRecord(String name, GnsReconfigurableInterface activeReplica) {
    int id = nextRequestID();
    outStandingUpdates.put(id, id);
    int recipientId = LNSQueryHandler.pickClosestLNServer(activeReplica);
    GNS.getLogger().fine("----------- Node " + activeReplica.getNodeID() + "; Sending remove: " + name + " to LNS " + recipientId);
    RemoveRecordPacket packet = new RemoveRecordPacket(activeReplica.getNodeID(), id, name, -1);
    try {
      activeReplica.getNioServer().sendToID(recipientId, packet.toJSONObject());
    } catch (JSONException e) {
      GNS.getLogger().severe("Problem converting packet to JSON Object:" + e);
    } catch (IOException e) {
      GNS.getLogger().severe("Problem sending packet to NS " + recipientId + ": " + e);
    }
    waitForResponsePacket(id);
    NSResponseCode result = updateResultMap.get(id);
    updateResultMap.remove(id);
    return result;
  }

  /**
   * Handles a ConfirmUpdatePacket coming back to this NameServer from a Local Name Server
   *
   * @param packet
   * @param activeReplica
   */
  public static void handleConfirmUpdatePacket(ConfirmUpdatePacket packet, GnsReconfigurable activeReplica) {
    int id = packet.getRequestID();
    if (packet.isSuccess()) {
      //Packet is a response and does not have a response error
      synchronized (monitor) {
        if (outStandingUpdates.remove(id) != null) {
          GNS.getLogger().fine("First Update Response (" + id + ") Successful Received");

          updateResultMap.put(id, packet.getResponseCode());
          monitor.notifyAll();
        } else {
          GNS.getLogger().fine("Later Update Response (" + id + ") Successful Received");
        }
      }
    } else {
      synchronized (monitor) {
        if (outStandingUpdates.remove(id) != null) {
          GNS.getLogger().fine("First Error Update Response (" + id + ") Error Received: " + packet.getResponseCode().toString());
          updateResultMap.put(id, packet.getResponseCode());
          monitor.notifyAll();
        } else {
          GNS.getLogger().fine("Later Error Update Response (" + id + ") Error Received: " + packet.getResponseCode().toString());
        }
      }
    }
  }

  private static void waitForResponsePacket(int id) {
    try {
      synchronized (monitor) {
        while (!updateResultMap.containsKey(id)) {
          monitor.wait();
        }
        GNS.getLogger().fine("Query id response received: " + id);
      }
    } catch (InterruptedException x) {
      GNS.getLogger().severe("Wait for update success confirmation packet was interrupted " + x);
    }
  }

  private static int nextRequestID() {
    int id;
    do {
      id = randomID.nextInt();
    } while (updateResultMap.containsKey(id));
    return id;
  }
  public static String Version = "$Revision: 481 $";
}
