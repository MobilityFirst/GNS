/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.client;

import edu.umass.cs.gns.clientprotocol.Defs;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.localnameserver.LNSListenerAdmin;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.NameRecord;
import static edu.umass.cs.gns.packet.Packet.*;
import edu.umass.cs.gns.packet.admin.AdminRequestPacket;
import edu.umass.cs.gns.packet.admin.AdminResponsePacket;
import edu.umass.cs.gns.packet.admin.DumpRequestPacket;
import edu.umass.cs.gns.packet.admin.SentinalPacket;
import edu.umass.cs.gns.util.ConfigFileInfo;
import java.io.IOException;
import java.net.ServerSocket;
import java.text.ParseException;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Implements some administrative functions for accessing the GNS.
 * 
 * @author westy
 */
public class Admintercessor {

  private static final String LINE_SEPARATOR = System.getProperty("line.separator");
  private static Random randomID;
  private static int localServerID = 0;
  /**
   * Used for the general admin wait / notify handling
   */
  private static final Object adminResponseMonitor = new Object();
  /**
   * This is where the admin response results are put.
   */
  private static ConcurrentMap<Integer, JSONObject> adminResult;
  /**
   * Used for the dump wait / notify handling
   */
  private static final Object dumpMonitor = new Object();
  /**
   * This is where dump response records are collected while we're waiting for all them to come in.
   */
  private static ConcurrentMap<Integer, Map<Integer, TreeSet<NameRecord>>> dumpStorage;
  /**
   * This is where the final dump response results are put once we see the sentinel packet.
   */
  private static ConcurrentMap<Integer, Map<Integer, TreeSet<NameRecord>>> dumpResult;

  static {
    randomID = new Random();
    dumpStorage = new ConcurrentHashMap<Integer, Map<Integer, TreeSet<NameRecord>>>(10, 0.75f, 3);
    dumpResult = new ConcurrentHashMap<Integer, Map<Integer, TreeSet<NameRecord>>>(10, 0.75f, 3);
    adminResult = new ConcurrentHashMap<Integer, JSONObject>(10, 0.75f, 3);
  }

  public static int getLocalServerID() {
    return localServerID;
  }

  public static void setLocalServerID(int localServerID) {
    Admintercessor.localServerID = localServerID;

    GNS.getLogger().info("Local server id: " + localServerID
            + " Address: " + ConfigFileInfo.getIPAddress(localServerID)
            + " Port: " + ConfigFileInfo.getLNSTcpPort(localServerID));
  }

  // the nuclear option
  public static boolean sendResetDB() {
    try {
      sendAdminPacket(new AdminRequestPacket(AdminRequestPacket.AdminOperation.RESETDB).toJSONObject());
      return true;
    } catch (Exception e) {
      GNS.getLogger().warning("Ignoring this error while sending RESETDB request: " + e);
    }
    return false;
  }

  public static boolean sendDeleteAllRecords() {
    try {
      sendAdminPacket(new AdminRequestPacket(AdminRequestPacket.AdminOperation.DELETEALLRECORDS).toJSONObject());
      return true;
    } catch (Exception e) {
      GNS.getLogger().warning("Error while sending DELETEALLRECORDS request: " + e);
    }
    return false;
  }

  public static boolean sendClearCache() {
    try {
      sendAdminPacket(new AdminRequestPacket(AdminRequestPacket.AdminOperation.CLEARCACHE).toJSONObject());
      return true;
    } catch (Exception e) {
      GNS.getLogger().warning("Ignoring error while sending CLEARCACHE request: " + e);
    }
    return false;
  }

  public static String sendDumpCache() {
    int id = nextAdminRequestID();
    try {
      sendAdminPacket(new AdminRequestPacket(id, AdminRequestPacket.AdminOperation.DUMPCACHE).toJSONObject());
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
  
  public static boolean sendChangeLogLevel(Level level) {
    try {
      AdminRequestPacket packet = new AdminRequestPacket(AdminRequestPacket.AdminOperation.CHANGELOGLEVEL, level.getName());
      sendAdminPacket(packet.toJSONObject());
      return true;
    } catch (Exception e) {
      GNS.getLogger().warning("Ignoring error while sending CHANGELOGLEVEL request: " + e);
    }
    return false;
  }

  private static void waitForAdminResponse(int id) {
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

  public static void processAdminResponsePackets(JSONObject json) {
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

  private static ServerSocket getAdminResponseSocket() {
    GNS.getLogger().finer("Waiting for responses dump");
    ServerSocket adminSocket;
    try {
      adminSocket = new ServerSocket(ConfigFileInfo.getLNSAdminResponsePort(localServerID));
    } catch (Exception e) {
      GNS.getLogger().severe("Error creating admin response socket on port " + ConfigFileInfo.getLNSAdminResponsePort(localServerID) + " : " + e);
      return null;
    }
    return adminSocket;
  }

  // DUMP
  public static String sendDump() {
    int id;
    if ((id = sendDumpOutputHelper(null)) == -1) {
      return Defs.BADRESPONSE + " " + Defs.QUERYPROCESSINGERROR + " " + "Error sending dump command to LNS";
    }
    waitForDumpResponse(id);
    Map<Integer, TreeSet<NameRecord>> result = dumpResult.get(id);
    dumpResult.remove(id);
    if (result != null) {
      return formatDumpRecords(result);
    } else {
      return null;
    }
  }

  private static void waitForDumpResponse(int id) {
    try {
      GNS.getLogger().finer("Waiting for dump response id: " + id);
      synchronized (dumpMonitor) {
        long timeoutExpiredMs = System.currentTimeMillis() + 10000;
        while (!dumpResult.containsKey(id)) {
          dumpMonitor.wait(timeoutExpiredMs - System.currentTimeMillis());
          if (System.currentTimeMillis() >= timeoutExpiredMs) {
            // we timed out... only got partial results{
            dumpResult.put(id, dumpStorage.get(id));
            dumpStorage.remove(id);
            break;
          }
        }
      }

      GNS.getLogger().finer("Dump response id received: " + id);
    } catch (InterruptedException x) {
      GNS.getLogger().severe("Wait for return packet was interrupted " + x);
    }
  }

  private static String formatDumpRecords(Map<Integer, TreeSet<NameRecord>> recordsMap) {
    // now process all the records we received
    
    StringBuilder result = new StringBuilder();
    // are there any NSs that didn't respond?
    Set<Integer>missingIDs = new HashSet(ConfigFileInfo.getAllNameServerIDs());
    missingIDs.removeAll(recordsMap.keySet());
    if (missingIDs.size() > 0) {
      result.append("Missing NSs: " + missingIDs.toString());
      result.append(LINE_SEPARATOR);
    }
    // process all the entries into a nice string
    for (Map.Entry<Integer, TreeSet<NameRecord>> entry : recordsMap.entrySet()) {
      result.append("Nameserver: " + entry.getKey() + " (" + ConfigFileInfo.getIPAddress(entry.getKey()).getHostName() + ")");
      result.append(LINE_SEPARATOR);
      for (NameRecord record : entry.getValue()) {
        try {
          result.append("  NAME: ");
          result.append(record.getName());
//        result.append(" / KEY: ");
//        result.append(record.getRecordKey().getName());
          result.append(" P: ");
          result.append(record.getPrimaryNameservers().toString());
          result.append(" A: ");
          result.append(record.getActiveNameServers().toString());
          result.append(" TTL: ");
          result.append(record.getTimeToLive());
          result.append(LINE_SEPARATOR);
          result.append("    VALUE: ");
          result.append(record.getValuesMap());
          result.append(LINE_SEPARATOR);
        } catch (FieldNotFoundException e) {
          GNS.getLogger().severe("FieldNotFoundException. " + e.getMessage());
        }
      }
    }
    return result.toString();
  }

  public static void processDumpResponsePackets(JSONObject json) {
    try {
      switch (getPacketType(json)) {
        case DUMP_REQUEST:
          try {
            DumpRequestPacket dumpResponse = new DumpRequestPacket(json);
            int id = dumpResponse.getId();
            // grab or make a new recordsMap
            Map<Integer, TreeSet<NameRecord>> recordsMap = dumpStorage.get(id);
            if (recordsMap == null) {
              recordsMap = new TreeMap<Integer, TreeSet<NameRecord>>();
              dumpStorage.put(id, recordsMap);
            }
            // pull the records out of the dump response and put them in dumpStorage
            JSONArray jsonArray = dumpResponse.getJsonArray();
            int serverID = dumpResponse.getPrimaryNameServer();
            TreeSet<NameRecord> records = new TreeSet<NameRecord>();
            for (int i = 0; i < jsonArray.length(); i++) {
              records.add(new NameRecord(jsonArray.getJSONObject(i)));
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

  public static HashSet<String> collectTaggedGuids(String tagName) {
    int id;
    if ((id = sendDumpOutputHelper(tagName)) == -1) {
      return null;
    }
    waitForDumpResponse(id);
    Map<Integer, TreeSet<NameRecord>> result = dumpResult.get(id);
    dumpResult.remove(id);

    if (result != null) {
      HashSet<String> allGuids = new HashSet<String>();
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

  private static ServerSocket sendDumpGetInputSocket() {
    GNS.getLogger().finer("Waiting for responses dump");
    ServerSocket adminSocket;
    try {
      adminSocket = new ServerSocket(ConfigFileInfo.getLNSAdminDumpReponsePort(localServerID));
    } catch (Exception e) {
      GNS.getLogger().severe("Error creating admin socket on port " + ConfigFileInfo.getLNSAdminDumpReponsePort(localServerID) + " : " + e);
      return null;
    }
    return adminSocket;
  }

  private static int sendDumpOutputHelper(String tagName) {
    // send the request out to the local name server
    int id = nextDumpRequestID();
    GNS.getLogger().finer("Sending dump request id = " + id);
    try {
      sendAdminPacket(new DumpRequestPacket(id, localServerID, tagName).toJSONObject());
    } catch (JSONException e) {
      GNS.getLogger().warning("Ignoring error sending DUMP request for id " + id + " : " + e);
      return -1;
    } catch (IOException e) {
      return -1;
    }
    return id;
  }

  private static void sendAdminPacket(JSONObject json) throws IOException {
      LNSListenerAdmin.handlePacket(json, null);
  }

  private static int nextDumpRequestID() {
    int id;
    do {
      id = randomID.nextInt();
    } while (dumpResult.containsKey(id));
    return id;
  }

  private static int nextAdminRequestID() {
    int id;
    do {
      id = randomID.nextInt();
    } while (adminResult.containsKey(id));
    return id;
  }
}
