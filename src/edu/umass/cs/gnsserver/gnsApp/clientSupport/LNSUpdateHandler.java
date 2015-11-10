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
 *  Initial developer(s): Abhigyan Sharma, Westy
 *
 */
package edu.umass.cs.gnsserver.gnsApp.clientSupport;

import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.UpdateOperation;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.gnsApp.AppReconfigurableNodeOptions;
import edu.umass.cs.gnsserver.gnsApp.GnsApplicationInterface;
import edu.umass.cs.gnsserver.gnsApp.packet.ConfirmUpdatePacket;
import edu.umass.cs.gnsserver.gnsApp.packet.UpdatePacket;
import edu.umass.cs.gnsserver.gnsApp.NSResponseCode;
import edu.umass.cs.gnsserver.utils.ResultValue;
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
   * @return a {@link NSResponseCode}
   */
  public static NSResponseCode sendUpdate(String name, String key, ResultValue newValue, UpdateOperation operation,
          GnsApplicationInterface<String> activeReplica, InetSocketAddress lnsAddress) {
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
   * @return a {@link NSResponseCode}
   */
  public static NSResponseCode sendUpdate(String name, String key, ResultValue newValue,
          ResultValue oldValue, int argument, UpdateOperation operation, 
          GnsApplicationInterface<String> activeReplica, InetSocketAddress lnsAddress) {
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
          ResultValue oldValue, int argument, UpdateOperation operation, 
          GnsApplicationInterface<String> app) {
    UpdatePacket<String> packet = new UpdatePacket<String>(app.getNodeID(), updateId,
            name, key, newValue, oldValue, argument, operation,
            null, GNS.DEFAULT_TTL_SECONDS,
            null, null, null);
    try {
      if (AppReconfigurableNodeOptions.debuggingEnabled) {
        GNS.getLogger().info("########## Node " + app.getNodeID().toString() 
                + "; Sending update " + updateId + " to LNS at" + lnsAddress
                + " for " + name + " / " + key + ": " + packet.toJSONObject());
      }
      app.sendToClient(lnsAddress, packet.toJSONObject());
    } catch (JSONException e) {
      GNS.getLogger().severe("Problem converting packet to JSON Object:" + e);
    } catch (IOException e) {
      GNS.getLogger().severe("Problem sending packet to LNS at " + lnsAddress + ": " + e);
    }
  }

//  public static NSResponseCode sendAddRecord(String name, String key, ResultValue value, GnsApplicationInterface activeReplica,
//          InetSocketAddress lnsAddress) {
//    int id = nextRequestID();
//    outStandingUpdates.put(id, id);
//    if (AppReconfigurableNodeOptions.debuggingEnabled) {
//      GNS.getLogger().fine("++++++++++ Node " + activeReplica.getNodeID().toString() + "; Sending add: " + name + " : " + key + "->" + value + " to LNS " + lnsAddress);
//    }
//    AddRecordPacket packet = new AddRecordPacket(activeReplica.getNodeID(), id, name, key, value, null);
//    try {
//      activeReplica.getMessenger().sendToAddress(lnsAddress, packet.toJSONObject());
//    } catch (JSONException e) {
//      GNS.getLogger().severe("Problem converting packet to JSON Object:" + e);
//    } catch (IOException e) {
//      GNS.getLogger().severe("Problem sending packet to NS " + lnsAddress + ": " + e);
//    }
//    waitForResponsePacket(id);
//    NSResponseCode result = updateResultMap.get(id);
//    updateResultMap.remove(id);
//    return result;
//  }
//
//  public static NSResponseCode sendRemoveRecord(String name, GnsApplicationInterface activeReplica,
//          InetSocketAddress lnsAddress) {
//    int id = nextRequestID();
//    outStandingUpdates.put(id, id);
//    if (AppReconfigurableNodeOptions.debuggingEnabled) {
//      GNS.getLogger().fine("----------- Node " + activeReplica.getNodeID().toString() + "; Sending remove: " + name + " to LNS " + lnsAddress);
//    }
//    RemoveRecordPacket packet = new RemoveRecordPacket(activeReplica.getNodeID(), id, name, null);
//    try {
//      activeReplica.getMessenger().sendToAddress(lnsAddress, packet.toJSONObject());
//    } catch (JSONException e) {
//      GNS.getLogger().severe("Problem converting packet to JSON Object:" + e);
//    } catch (IOException e) {
//      GNS.getLogger().severe("Problem sending packet to NS " + lnsAddress + ": " + e);
//    }
//    waitForResponsePacket(id);
//    NSResponseCode result = updateResultMap.get(id);
//    updateResultMap.remove(id);
//    return result;
//  }

  /**
   * Handles a ConfirmUpdatePacket coming back to this NameServer from a Local Name Server
   *
   * @param packet
   * @param activeReplica
   */
  public static void handleConfirmUpdatePacket(ConfirmUpdatePacket<String> packet, 
          GnsApplicationInterface<String> activeReplica) {
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
 
}
