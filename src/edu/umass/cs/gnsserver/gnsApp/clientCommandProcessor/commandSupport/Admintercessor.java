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
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport;

import edu.umass.cs.gnscommon.GnsProtocol;
import static edu.umass.cs.gnscommon.GnsProtocol.BAD_RESPONSE;
import edu.umass.cs.gnsserver.exceptions.FieldNotFoundException;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.CCPListenerAdmin;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.gnsApp.packet.admin.AdminRequestPacket;
import edu.umass.cs.gnsserver.gnsApp.packet.admin.AdminResponsePacket;
import edu.umass.cs.gnsserver.gnsApp.packet.admin.DumpRequestPacket;
import edu.umass.cs.gnsserver.gnsApp.packet.admin.SentinalPacket;
import edu.umass.cs.gnsserver.gnsApp.recordmap.NameRecord;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import java.io.IOException;
import java.text.ParseException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import static edu.umass.cs.gnsserver.gnsApp.packet.Packet.getPacketType;
import edu.umass.cs.gnsserver.utils.Util;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Implements some administrative functions for accessing the GNS.
 *
 * @author westy
 */
public class Admintercessor {

  private final String LINE_SEPARATOR = System.getProperty("line.separator");
  private Random randomID;

  /**
   * Used for the general admin wait / notify handling
   */
  private final Object adminResponseMonitor = new Object();
  /**
   * This is where the admin response results are put.
   */
  private ConcurrentMap<Integer, JSONObject> adminResult;
  /**
   * Used for the dump wait / notify handling
   */
  private final Object dumpMonitor = new Object();
  /**
   * This is where dump response records are collected while we're waiting for all them to come in.
   */
  private ConcurrentMap<Integer, Map<String, TreeSet<NameRecord>>> dumpStorage;
  /**
   * This is where the final dump response results are put once we see the sentinel packet.
   */
  private ConcurrentMap<Integer, Map<String, TreeSet<NameRecord>>> dumpResult;

  /**
   *
   */
  CCPListenerAdmin listenerAdmin = null;

  /**
   * Sets the listener admin.
   *
   * @param listenerAdmin
   */
  public void setListenerAdmin(CCPListenerAdmin listenerAdmin) {
    this.listenerAdmin = listenerAdmin;
  }

  {
    randomID = new Random();
    dumpStorage = new ConcurrentHashMap<Integer, Map<String, TreeSet<NameRecord>>>(10, 0.75f, 3);
    dumpResult = new ConcurrentHashMap<Integer, Map<String, TreeSet<NameRecord>>>(10, 0.75f, 3);
    adminResult = new ConcurrentHashMap<Integer, JSONObject>(10, 0.75f, 3);
  }

  /**
   * Clears the database and reinitializes all indices.
   *
   * @param handler
   * @return true if we were successful
   */
  public boolean sendResetDB(ClientRequestHandlerInterface handler) {
    try {
      sendAdminPacket(new AdminRequestPacket(AdminRequestPacket.AdminOperation.RESETDB).toJSONObject(), handler);
      return true;
    } catch (Exception e) {
      GNS.getLogger().warning("Ignoring this error while sending RESETDB request: " + e);
    }
    return false;
  }

  /**
   * Sends the delete all records command.
   *
   * @param handler
   * @return true if we were successful
   */
  public boolean sendDeleteAllRecords(ClientRequestHandlerInterface handler) {
    try {
      sendAdminPacket(new AdminRequestPacket(AdminRequestPacket.AdminOperation.DELETEALLRECORDS).toJSONObject(), handler);
      return true;
    } catch (Exception e) {
      GNS.getLogger().warning("Error while sending DELETEALLRECORDS request: " + e);
    }
    return false;
  }

  /**
   * Sends the clear cache command.
   *
   * @param handler
   * @return true if we were successful
   */
  public boolean sendClearCache(ClientRequestHandlerInterface handler) {
    try {
      sendAdminPacket(new AdminRequestPacket(AdminRequestPacket.AdminOperation.CLEARCACHE).toJSONObject(), handler);
      return true;
    } catch (Exception e) {
      GNS.getLogger().warning("Ignoring error while sending CLEARCACHE request: " + e);
    }
    return false;
  }

  /**
   * Sends the dump cache command.
   *
   * @param handler
   * @return a string containing the cache
   */
  public String sendDumpCache(ClientRequestHandlerInterface handler) {
    int id = nextAdminRequestID();
    try {
      sendAdminPacket(new AdminRequestPacket(id, AdminRequestPacket.AdminOperation.DUMPCACHE).toJSONObject(), handler);
      waitForAdminResponse(id);
      JSONObject json = adminResult.get(id);
      if (json != null) {
        return json.getString("CACHE");
      } else {
        return null;
      }
    } catch (Exception e) {
      GNS.getLogger().warning("Ignoring error while sending DUMPCACHE request: " + e);
      return null;
    }
  }

  /**
   * Sends the ping table command for a given node.
   *
   * @param node
   * @param handler
   * @return a string containing the ping results for the node
   */
  public String sendPingTable(String node, ClientRequestHandlerInterface handler) {
    int id = nextAdminRequestID();
    try {
      sendAdminPacket(new AdminRequestPacket(id, AdminRequestPacket.AdminOperation.PINGTABLE, node).toJSONObject(), handler);
      waitForAdminResponse(id);
      JSONObject json = adminResult.get(id);
      if (json != null) {
        if (json.optString("ERROR", null) != null) {
          return BAD_RESPONSE + " " + json.getString("ERROR");
        }
        return json.getString("PINGTABLE");
      } else {
        return null;
      }
    } catch (Exception e) {
      GNS.getLogger().warning("Ignoring error while sending PINGTABLE request: " + e);
      return null;
    }
  }

  /**
   * Sends the ping value command for node1 to node2 specified as integers.
   * Returns the ping value between those nodes.
   *
   * @param node1
   * @param node2
   * @param handler
   * @return the ping value between those nodes
   */
  public String sendPingValue(int node1, int node2, ClientRequestHandlerInterface handler) {
    return sendPingValue(Integer.toString(node1), Integer.toString(node1), handler);
  }

  /**
   * Sends the ping value command for node1 to node2 specified as strings.
   * Returns the ping value between those nodes.
   *
   * @param node1
   * @param node2
   * @param handler
   * @return a string containing the ping value
   */
  public String sendPingValue(String node1, String node2, ClientRequestHandlerInterface handler) {
    int id = nextAdminRequestID();
    try {
      sendAdminPacket(new AdminRequestPacket(id, AdminRequestPacket.AdminOperation.PINGVALUE, node1.toString(),
              node2.toString()).toJSONObject(), handler);
      waitForAdminResponse(id);
      JSONObject json = adminResult.get(id);
      if (json != null) {
        if (json.optString("ERROR", null) != null) {
          return BAD_RESPONSE + " " + json.getString("ERROR");
        }
        return json.getString("PINGVALUE");
      } else {
        return null;
      }
    } catch (Exception e) {
      GNS.getLogger().warning("Ignoring error while sending PINGVALUE request: " + e);
      return null;
    }
  }

  /**
   * Sends the change log level command.
   *
   * @param level
   * @param handler
   * @return true if we were successful 
   */
  public boolean sendChangeLogLevel(Level level, ClientRequestHandlerInterface handler) {
    try {
      AdminRequestPacket packet = new AdminRequestPacket(AdminRequestPacket.AdminOperation.CHANGELOGLEVEL, level.getName());
      sendAdminPacket(packet.toJSONObject(), handler);
      return true;
    } catch (Exception e) {
      GNS.getLogger().warning("Ignoring error while sending CHANGELOGLEVEL request: " + e);
    }
    return false;
  }

  private void waitForAdminResponse(int id) {
    try {
      GNS.getLogger().finer("Waiting for admin response id: " + id);
      synchronized (adminResponseMonitor) {
        while (!adminResult.containsKey(id)) {
          adminResponseMonitor.wait();
        }
      }
      GNS.getLogger().finer("Admin response id received: " + id);
    } catch (InterruptedException x) {
      GNS.getLogger().severe("Wait for return packet was interrupted " + x);
    }
  }

  /**
   * Processes incoming AdminResponse packets.
   *
   * @param json
   */
  public void handleIncomingAdminResponsePackets(JSONObject json) {
    try {
      switch (getPacketType(json)) {
        case ADMIN_RESPONSE:
          // put the records in dumpResult and let the folks waiting know they are ready
          try {
            AdminResponsePacket response = new AdminResponsePacket(json);
            int id = response.getId();
            GNS.getLogger().finer("Processing admin response for " + id);
            synchronized (adminResponseMonitor) {
              adminResult.put(id, response.getJsonObject());
              adminResponseMonitor.notifyAll();
            }
          } catch (JSONException e) {
            GNS.getLogger().warning("JSON error during admin response processing: " + e);
          } catch (ParseException e) {
            GNS.getLogger().warning("Parse error during admin response processing: " + e);
          }
          break;
      }
    } catch (JSONException e) {
      GNS.getLogger().warning("JSON error while getting packet type: " + e);
    }
  }

  // DUMP
  /**
   * Sends the dump command to the LNS.
   *
   * @param handler
   * @return a string containing the contents of the GNS
   */
  public CommandResponse<String> sendDump(ClientRequestHandlerInterface handler) {
    int id;
    if ((id = sendDumpOutputHelper(null, handler)) == -1) {
      return new CommandResponse<String>(GnsProtocol.BAD_RESPONSE + " " + GnsProtocol.QUERY_PROCESSING_ERROR + " " + "Error sending dump command to LNS");
    }
    waitForDumpResponse(id);
    Map<String, TreeSet<NameRecord>> result = dumpResult.get(id);
    dumpResult.remove(id);
    if (result != null) {
      return new CommandResponse<String>(formatDumpRecords(result, handler));
    } else {
      return new CommandResponse<String>(GnsProtocol.BAD_RESPONSE + " " + GnsProtocol.QUERY_PROCESSING_ERROR + " " + "No response to dump command!");
    }
  }

  private void waitForDumpResponse(int id) {
    try {
      GNS.getLogger().finer("Waiting for dump response id: " + id);
      synchronized (dumpMonitor) {
        long timeoutExpiredMs = System.currentTimeMillis() + 10000;
        while (!dumpResult.containsKey(id)) {
          dumpMonitor.wait(timeoutExpiredMs - System.currentTimeMillis());
          if (System.currentTimeMillis() >= timeoutExpiredMs) {
            // we timed out... only got partial results{
            Map<String, TreeSet<NameRecord>> recordsMap = dumpStorage.get(id);
            if (recordsMap != null) { // can be null if we timed out before getting any responses
              dumpResult.put(id, recordsMap);
              dumpStorage.remove(id);
            }
            break;
          }
        }
      }

      GNS.getLogger().finer("Dump response id received: " + id);
    } catch (InterruptedException x) {
      GNS.getLogger().severe("Wait for return packet was interrupted " + x);
    }
  }

  @SuppressWarnings("unchecked")
  private String formatDumpRecords(Map<String, TreeSet<NameRecord>> recordsMap,
          ClientRequestHandlerInterface handler) {
    // now process all the records we received

    StringBuilder result = new StringBuilder();
    // are there any NSs that didn't respond?
    Set<String> missingIDs = new HashSet(handler.getGnsNodeConfig().getActiveReplicas());
    missingIDs.removeAll(recordsMap.keySet());
    if (missingIDs.size() > 0) {
      result.append("Missing NSs: " + Util.setOfNodeIdToString(missingIDs));
      result.append(LINE_SEPARATOR);
    }
    // process all the entries into a nice string
    for (Map.Entry<String, TreeSet<NameRecord>> entry : recordsMap.entrySet()) {
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().info("RECEIVED DUMP RECORD FROM NS: " + entry.getKey());
      }
      result.append("Nameserver: " + entry.getKey().toString()
              + " (" + handler.getGnsNodeConfig().getNodeAddress(entry.getKey()).getHostName() + ")");
      result.append(LINE_SEPARATOR);
      for (NameRecord record : entry.getValue()) {
        try {
          result.append("  NAME: ");
          result.append(record.getName());
//        result.append(" / KEY: ");
//        result.append(record.getRecordKey().getName());
          result.append(" P: ");
          result.append(Util.setOfNodeIdToString(record.getPrimaryNameservers()));
//          result.append(" A: ");
//          result.append(record.getActiveNameServers().toString());
          result.append(" TTL: ");
          result.append(record.getTimeToLive());
          result.append(LINE_SEPARATOR);
          result.append("    VALUE: ");
          try {
            result.append(record.getValuesMap().toString(2));
          } catch (JSONException e) {
            result.append(record.getValuesMap());
          }
          result.append(LINE_SEPARATOR);
        } catch (FieldNotFoundException e) {
          GNS.getLogger().severe("FieldNotFoundException. " + e.getMessage());
        }
      }
    }
    return result.toString();
  }

  /**
   * Processes incoming Dump packets.
   *
   * @param json
   * @param handler
   */
  public void handleIncomingDumpResponsePackets(JSONObject json, ClientRequestHandlerInterface handler) {
    try {
      switch (getPacketType(json)) {
        case DUMP_REQUEST:
          try {
            DumpRequestPacket<String> dumpResponse = new DumpRequestPacket<String>(json, handler.getGnsNodeConfig());
            int id = dumpResponse.getId();
            // grab or make a new recordsMap
            Map<String, TreeSet<NameRecord>> recordsMap = dumpStorage.get(id);
            if (recordsMap == null) {
              recordsMap = new TreeMap<String, TreeSet<NameRecord>>();
              dumpStorage.put(id, recordsMap);
            }
            // pull the records out of the dump response and put them in dumpStorage
            JSONArray jsonArray = dumpResponse.getJsonArray();
            // ServerID can now be a String or and Integer
            String serverID = dumpResponse.getPrimaryNameServer();
            TreeSet<NameRecord> records = new TreeSet<NameRecord>();
            for (int i = 0; i < jsonArray.length(); i++) {
              records.add(new NameRecord(null, jsonArray.getJSONObject(i)));
            }
            recordsMap.put(serverID, records);
          } catch (JSONException e) {
            GNS.getLogger().warning("JSON error during dump reponse processing: " + e);
          }
          break;
        case SENTINAL:
          // put the records in dumpResult and let the folks waiting know they are ready
          try {
            SentinalPacket sentinal = new SentinalPacket(json);
            int id = sentinal.getId();
            GNS.getLogger().finer("Processing sentinel for " + id);
            synchronized (dumpMonitor) {
              dumpResult.put(id, dumpStorage.get(id));
              dumpStorage.remove(id);
              dumpMonitor.notifyAll();
            }
          } catch (JSONException e) {
            GNS.getLogger().warning("JSON error during dump sentinel processing: " + e);
          }
          break;
      }
    } catch (JSONException e) {
      GNS.getLogger().warning("JSON error while getting packet type: " + e);
    }
  }

  /**
   * Sends a command to collect all guids that contain the given tag.
   *
   * @param tagName
   * @param handler
   * @return a set of strings
   */
  public Set<String> collectTaggedGuids(String tagName, ClientRequestHandlerInterface handler) {
    int id;
    if ((id = sendDumpOutputHelper(tagName, handler)) == -1) {
      return null;
    }
    waitForDumpResponse(id);
    Map<String, TreeSet<NameRecord>> result = dumpResult.get(id);
    dumpResult.remove(id);

    if (result != null) {
      Set<String> allGuids = new HashSet<String>();
      for (TreeSet<NameRecord> records : result.values()) {
        try {
          for (NameRecord record : records) {
            allGuids.add(record.getName());
          }
        } catch (FieldNotFoundException e) {
        }
      }
      return allGuids;
    } else {
      return null;
    }
  }

  private int sendDumpOutputHelper(String tagName, ClientRequestHandlerInterface handler) {
    // send the request out to the local name server
    int id = nextDumpRequestID();
    GNS.getLogger().finer("Sending dump request id = " + id);
    try {
      sendAdminPacket(new DumpRequestPacket(id,
              new InetSocketAddress(handler.getNodeAddress().getAddress(), 
              handler.getGnsNodeConfig().getCcpAdminPort((String)handler.getActiveReplicaID())),
              tagName).toJSONObject(), handler);
    } catch (JSONException e) {
      GNS.getLogger().warning("Ignoring error sending DUMP request for id " + id + " : " + e);
      return -1;
    } catch (IOException e) {
      return -1;
    }
    return id;
  }

  private void sendAdminPacket(JSONObject json, ClientRequestHandlerInterface handler) throws IOException {
    if (listenerAdmin != null) {
      listenerAdmin.handlePacket(json, null, handler);
    }
  }

  private int nextDumpRequestID() {
    int id;
    do {
      id = randomID.nextInt();
    } while (dumpResult.containsKey(id));
    return id;
  }

  private int nextAdminRequestID() {
    int id;
    do {
      id = randomID.nextInt();
    } while (adminResult.containsKey(id));
    return id;
  }
}
