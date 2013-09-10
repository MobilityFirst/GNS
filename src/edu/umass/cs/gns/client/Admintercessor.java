/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.client;

import edu.umass.cs.gns.httpserver.Protocol;
import edu.umass.cs.gns.localnameserver.LNSListenerAdmin;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nameserver.NameRecord;
import edu.umass.cs.gns.nameserver.recordExceptions.FieldNotFoundException;
import edu.umass.cs.gns.packet.AdminRequestPacket;
import edu.umass.cs.gns.packet.DumpRequestPacket;
import edu.umass.cs.gns.packet.Packet;
import static edu.umass.cs.gns.packet.Packet.*;
import edu.umass.cs.gns.packet.SentinalPacket;
import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.gns.util.JSONUtils;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * IMplements some admin functions for accessing the GNS
 * @author westy
 */
public class Admintercessor {

  private static final String LINE_SEPARATOR = System.getProperty("line.separator");
  private static Random randomID;
  private int localServerID = 0;
  /**
   * Used for the wait / notify handling
   */
  private final Object monitor = new Object();
  /**
   * This is where dump response records are collected while we're waiting for all them to come in.
   */
  private static ConcurrentMap<Integer, Map<Integer, TreeSet<NameRecord>>> dumpStorage = new ConcurrentHashMap<Integer, Map<Integer, TreeSet<NameRecord>>>(10, 0.75f, 3);
  /**
   * This is where the final dump response results are put once we see the sentinel packet.
   */
  private static ConcurrentMap<Integer, Map<Integer, TreeSet<NameRecord>>> dumpResult = new ConcurrentHashMap<Integer, Map<Integer, TreeSet<NameRecord>>>(10, 0.75f, 3);

  public static Admintercessor getInstance() {
    return AdmintercessorHolder.INSTANCE;
  }

  private static class AdmintercessorHolder {

    private static final Admintercessor INSTANCE = new Admintercessor();
  }

  private Admintercessor() {

    randomID = new Random();
    dumpStorage = new ConcurrentHashMap<Integer, Map<Integer, TreeSet<NameRecord>>>(10, 0.75f, 3);
    dumpResult = new ConcurrentHashMap<Integer, Map<Integer, TreeSet<NameRecord>>>(10, 0.75f, 3);

    if (StartLocalNameServer.runHttpServer == false) {
      startCheckForDumpThread();
    }
  }

  public int getLocalServerID() {
    return localServerID;
  }

  public void setLocalServerID(int localServerID) {
    this.localServerID = localServerID;

    GNS.getLogger().info("Local server id: " + localServerID
            + " Address: " + ConfigFileInfo.getIPAddress(localServerID)
            + " Port: " + ConfigFileInfo.getLNSTcpPort(localServerID));
  }

  public boolean sendDeleteAllRecords() {
    try {
      sendAdminPacket(new AdminRequestPacket(AdminRequestPacket.AdminOperation.DELETEALLRECORDS).toJSONObject());
      return true;
    } catch (Exception e) {
      GNS.getLogger().warning("Error while sending DELETEALLRECORDS request: " + e);
    }
    return false;
  }

  // Miscellaneous operations
  public boolean sendClearCache() {
    try {
      sendAdminPacket(new AdminRequestPacket(AdminRequestPacket.AdminOperation.CLEARCACHE).toJSONObject());
      return true;
    } catch (Exception e) {
      GNS.getLogger().warning("Ignoring error while sending CLEARCACHE request: " + e);
    }
    return false;
  }

  // the nuclear option
  public boolean sendResetDB() {
    try {
      sendAdminPacket(new AdminRequestPacket(AdminRequestPacket.AdminOperation.RESETDB).toJSONObject());
      return true;
    } catch (Exception e) {
      GNS.getLogger().warning("Ignoring this error while sending RESETDB request: " + e);
    }
    return false;
  }

  public String sendDump() {
    int id;
    if ((id = sendDumpOutputHelper(null)) == -1) {
      return Protocol.BADRESPONSE + " " + Protocol.QUERYPROCESSINGERROR + " " + "Error sending dump command to LNS";
    }
    waitForDumpResult(id);
    Map<Integer, TreeSet<NameRecord>> result = dumpResult.get(id);
    dumpResult.remove(id);
    if (result != null) {
      return formatDumpRecords(result);
    } else {
      return null;
    }
  }

  private void waitForDumpResult(int id) {
    try {
      GNS.getLogger().finer("Waiting for dump response id: " + id);
      synchronized (monitor) {
        while (!dumpResult.containsKey(id)) {
          monitor.wait();
        }
      }
      GNS.getLogger().finer("Dump response id received: " + id);
    } catch (InterruptedException x) {
      GNS.getLogger().severe("Wait for return packet was interrupted " + x);
    }
  }

  private String formatDumpRecords(Map<Integer, TreeSet<NameRecord>> recordsMap) {
    // now process all the records we received
    StringBuilder result = new StringBuilder();
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

  private void startCheckForDumpThread() {
    new Thread("Admintercessor-checkForDump") {
      @Override
      public void run() {
        GNS.getLogger().info("Admintercessor dump check thread started.");
        ServerSocket adminSocket;
        if ((adminSocket = sendDumpGetInputSocket()) != null) {
          while (true) {
            try {
              Socket asock = adminSocket.accept();
              //Read the packet from the input stream
              JSONObject json = getJSONObjectFrame(asock);
              GNS.getLogger().finer("Read " + json.toString());
              processDumpResponsePackets(json);
            } catch (IOException e) {
              GNS.getLogger().warning("Error while reading dump response: " + e);
            } catch (JSONException e) {
              GNS.getLogger().warning("JSON Error while reading dump response: " + e);
            }
          }
        }
      }
    }.start();
  }

  public void processDumpResponsePackets(JSONObject json) {
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
            synchronized (monitor) {
              dumpResult.put(id, dumpStorage.get(id));
              dumpStorage.remove(id);
              monitor.notifyAll();
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

  public HashSet<String> collectTaggedGuids(String tagName) {
    int id;
    if ((id = sendDumpOutputHelper(tagName)) == -1) {
      return null;
    }
    waitForDumpResult(id);
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

  // uses a variation of the dump protocol to get all the guids that contain a tag
  public HashSet<String> collectTaggedGuidsOld(String tagName) {
    HashSet<String> guids = new HashSet<String>();
    if (sendDumpOutputHelper(tagName) == -1) {
      return null;
    }
    ServerSocket adminSocket;
    if ((adminSocket = sendDumpGetInputSocket()) == null) {
      return null;
    }
    while (true) {
      try {
        Socket asock = adminSocket.accept();
        //Read the packet from the input stream
        JSONObject json = getJSONObjectFrame(asock);
        GNS.getLogger().finer("Read " + json.toString());
        // keep reading until we see a packet that isn't a dump request
        if (getPacketType(json) == Packet.PacketType.DUMP_REQUEST) {
          DumpRequestPacket returnedDumpRequestPacket = new DumpRequestPacket(json);
          JSONArray jsonArray = returnedDumpRequestPacket.getJsonArray();
          guids.addAll(JSONUtils.JSONArrayToArrayList(jsonArray));
        } else { // we've read the sentinal packet
          GNS.getLogger().finer("Read sentinal" + json.toString());
          adminSocket.close();
          break;
        }
      } catch (Exception e) {
        GNS.getLogger().warning("Caught and ignoring error : " + e);

      }
    }
    return guids;
  }

  private ServerSocket sendDumpGetInputSocket() {
    GNS.getLogger().finer("Waiting for responses dump");
    ServerSocket adminSocket;
    try {
      adminSocket = new ServerSocket(ConfigFileInfo.getDumpReponsePort(localServerID));
    } catch (Exception e) {
      GNS.getLogger().severe("Error creating admin socket on port " + ConfigFileInfo.getDumpReponsePort(localServerID) + " : " + e);
      return null;
    }
    return adminSocket;
  }

  private int sendDumpOutputHelper(String tagName) {
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

  private void sendAdminPacket(JSONObject json) throws IOException {
    if (StartLocalNameServer.runHttpServer) {
      LNSListenerAdmin.handlePacket(json, null);
    } else {
      sendTCPPacket(json, localServerID, GNS.PortType.LNS_ADMIN_PORT);
    }
  }

  private int nextDumpRequestID() {
    int id;
    do {
      id = randomID.nextInt();
    } while (dumpResult.containsKey(id));
    return id;
  }
}
