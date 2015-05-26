/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.newApp.clientSupport;

import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.UpdateOperation;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.newApp.GnsApplicationInterface;
import edu.umass.cs.gns.newApp.packet.AddRecordPacket;
import edu.umass.cs.gns.newApp.packet.ConfirmUpdatePacket;
import edu.umass.cs.gns.newApp.packet.RemoveRecordPacket;
import edu.umass.cs.gns.newApp.packet.UpdatePacket;
import edu.umass.cs.gns.util.NSResponseCode;
import edu.umass.cs.gns.util.ResultValue;
import org.json.JSONException;
import java.io.IOException;
import java.net.InetSocketAddress;
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
  private static final ConcurrentMap<Integer, NSResponseCode> updateResultMap = new ConcurrentHashMap<Integer, NSResponseCode>(10, 0.75f, 3);
  private static final ConcurrentMap<Integer, Integer> outStandingUpdates = new ConcurrentHashMap<Integer, Integer>(10, 0.75f, 3);
  private static final Random randomID = new Random();

  /**
   * Sends an update request from this Name Server to a Local Name Server
   *
   * @param name
   * @param key
   * @param newValue
   * @param operation
   * @param activeReplica
   * @param lnsAddress
   * @return
   */
  public static NSResponseCode sendUpdate(String name, String key, ResultValue newValue, UpdateOperation operation, 
          GnsApplicationInterface activeReplica, InetSocketAddress lnsAddress) {
    return sendUpdate(name, key, newValue, null, -1, operation, activeReplica, lnsAddress);
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
   * @param lnsAddress
   * @return
   */
  public static NSResponseCode sendUpdate(String name, String key, ResultValue newValue,
          ResultValue oldValue, int argument, UpdateOperation operation, GnsApplicationInterface activeReplica, InetSocketAddress lnsAddress) {
    GNS.getLogger().fine("Node " + activeReplica.getNodeID().toString() + "; Sending update: " + name + " : " + key + "->" + newValue.toString());
    int id = nextRequestID();
    // use this to filter out everything but the first responder
    outStandingUpdates.put(id, id);
    // activeReplica.getGNSNodeConfig() is a hack to toString the first LNS
    // We need a means to find the closes LNS
    sendUpdateInternal(id, lnsAddress, name, key, newValue, oldValue, argument, operation, activeReplica);
    // now we wait until the packet comes back
    waitForResponsePacket(id);
    NSResponseCode result = updateResultMap.get(id);
    updateResultMap.remove(id);
    return result;
  }

  private static void sendUpdateInternal(int updateId, InetSocketAddress lnsAddress, String name, String key, ResultValue newValue,
          ResultValue oldValue, int argument, UpdateOperation operation, GnsApplicationInterface activeReplica) {
    UpdatePacket packet = new UpdatePacket(activeReplica.getNodeID(), updateId,
            name, key, newValue, oldValue, argument, operation,
            null, GNS.DEFAULT_TTL_SECONDS,
            null, null, null);
    try {
      if (Config.debuggingEnabled) {
        GNS.getLogger().info("########## Node " + activeReplica.getNodeID().toString() + "; Sending update " + updateId + " to LNS at" + lnsAddress
                + " for " + name + " / " + key + ": " + packet.toJSONObject());
      }
      activeReplica.getNioServer().sendToAddress(lnsAddress, packet.toJSONObject());
    } catch (JSONException e) {
      GNS.getLogger().severe("Problem converting packet to JSON Object:" + e);
    } catch (IOException e) {
      GNS.getLogger().severe("Problem sending packet to LNS at " + lnsAddress + ": " + e);
    }
  }

  public static NSResponseCode sendAddRecord(String name, String key, ResultValue value, GnsApplicationInterface activeReplica,
          InetSocketAddress lnsAddress) {
    int id = nextRequestID();
    outStandingUpdates.put(id, id);
    if (Config.debuggingEnabled) {
      GNS.getLogger().fine("++++++++++ Node " + activeReplica.getNodeID().toString() + "; Sending add: " + name + " : " + key + "->" + value + " to LNS " + lnsAddress);
    }
    AddRecordPacket packet = new AddRecordPacket(activeReplica.getNodeID(), id, name, key, value, null, GNS.DEFAULT_TTL_SECONDS);
    try {
      activeReplica.getNioServer().sendToAddress(lnsAddress, packet.toJSONObject());
    } catch (JSONException e) {
      GNS.getLogger().severe("Problem converting packet to JSON Object:" + e);
    } catch (IOException e) {
      GNS.getLogger().severe("Problem sending packet to NS " + lnsAddress + ": " + e);
    }
    waitForResponsePacket(id);
    NSResponseCode result = updateResultMap.get(id);
    updateResultMap.remove(id);
    return result;
  }

  public static NSResponseCode sendRemoveRecord(String name, GnsApplicationInterface activeReplica,
          InetSocketAddress lnsAddress) {
    int id = nextRequestID();
    outStandingUpdates.put(id, id);
    if (Config.debuggingEnabled) {
      GNS.getLogger().fine("----------- Node " + activeReplica.getNodeID().toString() + "; Sending remove: " + name + " to LNS " + lnsAddress);
    }
    RemoveRecordPacket packet = new RemoveRecordPacket(activeReplica.getNodeID(), id, name, null);
    try {
      activeReplica.getNioServer().sendToAddress(lnsAddress, packet.toJSONObject());
    } catch (JSONException e) {
      GNS.getLogger().severe("Problem converting packet to JSON Object:" + e);
    } catch (IOException e) {
      GNS.getLogger().severe("Problem sending packet to NS " + lnsAddress + ": " + e);
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
  public static void handleConfirmUpdatePacket(ConfirmUpdatePacket packet, GnsApplicationInterface activeReplica) {
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
