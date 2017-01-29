
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport;

import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.exceptions.server.FieldNotFoundException;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ListenerAdmin;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientCommandProcessorConfig;
import edu.umass.cs.gnsserver.gnsapp.packet.admin.AdminRequestPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.admin.AdminResponsePacket;
import edu.umass.cs.gnsserver.gnsapp.packet.admin.DumpRequestPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.admin.SentinalPacket;
import edu.umass.cs.gnsserver.gnsapp.recordmap.NameRecord;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.text.ParseException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

import edu.umass.cs.gnsserver.utils.Util;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import edu.umass.cs.gnsserver.gnsapp.packet.Packet;


public class Admintercessor {

  private final String LINE_SEPARATOR = System.getProperty("line.separator");
  private final Random randomID;


  private final Object adminResponseMonitor = new Object();

  private final ConcurrentMap<Integer, JSONObject> adminResult;

  private final Object dumpMonitor = new Object();

  private final ConcurrentMap<Integer, Map<String, TreeSet<NameRecord>>> dumpStorage;

  private final ConcurrentMap<Integer, Map<String, TreeSet<NameRecord>>> dumpResult;


  private ListenerAdmin listenerAdmin = null;


  public void setListenerAdmin(ListenerAdmin listenerAdmin) {
    this.listenerAdmin = listenerAdmin;
  }

  {
    randomID = new Random();
    dumpStorage = new ConcurrentHashMap<>(10, 0.75f, 3);
    dumpResult = new ConcurrentHashMap<>(10, 0.75f, 3);
    adminResult = new ConcurrentHashMap<>(10, 0.75f, 3);
  }


  public boolean sendResetDB(ClientRequestHandlerInterface handler) {
    try {
      sendAdminPacket(new AdminRequestPacket(AdminRequestPacket.AdminOperation.RESETDB).toJSONObject(), handler);
      return true;
    } catch (JSONException | IOException e) {
      ClientCommandProcessorConfig.getLogger().log(Level.WARNING, "Ignoring this error while sending RESETDB request: {0}", e);
    }
    return false;
  }


  public boolean sendDeleteAllRecords(ClientRequestHandlerInterface handler) {
    try {
      sendAdminPacket(new AdminRequestPacket(AdminRequestPacket.AdminOperation.DELETEALLRECORDS).toJSONObject(), handler);
      return true;
    } catch (JSONException | IOException e) {
      ClientCommandProcessorConfig.getLogger().log(Level.WARNING, "Error while sending DELETEALLRECORDS request: {0}", e);
    }
    return false;
  }


  // Keep this around for future use.
  public boolean sendClearCache(ClientRequestHandlerInterface handler) {
//    try {
//      sendAdminPacket(new AdminRequestPacket(AdminRequestPacket.AdminOperation.CLEARCACHE).toJSONObject(), handler);
//      return true;
//    } catch (JSONException | IOException e) {
//      ClientCommandProcessorConfig.getLogger().log(Level.WARNING, "Ignoring error while sending CLEARCACHE request: {0}", e);
//    }
    return false;
  }


  // Keep this around for future use.
  public String sendDumpCache(ClientRequestHandlerInterface handler) {
//    int id = nextAdminRequestID();
//    try {
//      sendAdminPacket(new AdminRequestPacket(id, AdminRequestPacket.AdminOperation.DUMPCACHE).toJSONObject(), handler);
//      waitForAdminResponse(id);
//      JSONObject json = adminResult.get(id);
//      if (json != null) {
//        return json.getString("CACHE");
//      } else {
//        return null;
//      }
//    } catch (JSONException | IOException e) {
//      ClientCommandProcessorConfig.getLogger().log(Level.WARNING, "Ignoring error while sending DUMPCACHE request: {0}", e);
//      return null;
//    }
    return "Currently not supported.";
  }


  public boolean sendChangeLogLevel(Level level, ClientRequestHandlerInterface handler) {
    try {
      AdminRequestPacket packet = new AdminRequestPacket(AdminRequestPacket.AdminOperation.CHANGELOGLEVEL, level.getName());
      sendAdminPacket(packet.toJSONObject(), handler);
      return true;
    } catch (JSONException | IOException e) {
      ClientCommandProcessorConfig.getLogger().log(Level.WARNING, "Ignoring error while sending CHANGELOGLEVEL request: {0}", e);
    }
    return false;
  }

  private void waitForAdminResponse(int id) {
    try {
      ClientCommandProcessorConfig.getLogger().log(Level.FINER, "Waiting for admin response id: {0}", id);
      synchronized (adminResponseMonitor) {
        while (!adminResult.containsKey(id)) {
          adminResponseMonitor.wait();
        }
      }
      ClientCommandProcessorConfig.getLogger().log(Level.FINER, "Admin response id received: {0}", id);
    } catch (InterruptedException x) {
      ClientCommandProcessorConfig.getLogger().log(Level.SEVERE, "Wait for return packet was interrupted {0}", x);
    }
  }


  public void handleIncomingAdminResponsePackets(JSONObject json) {
    try {
      switch (Packet.getPacketType(json)) {
        case ADMIN_RESPONSE:
          // put the records in dumpResult and let the folks waiting know they are ready
          try {
            AdminResponsePacket response = new AdminResponsePacket(json);
            int id = response.getId();
            ClientCommandProcessorConfig.getLogger().log(Level.FINER, "Processing admin response for {0}", id);
            synchronized (adminResponseMonitor) {
              adminResult.put(id, response.getJsonObject());
              adminResponseMonitor.notifyAll();
            }
          } catch (JSONException e) {
            ClientCommandProcessorConfig.getLogger().log(Level.WARNING, "JSON error during admin response processing: {0}", e);
          } catch (ParseException e) {
            ClientCommandProcessorConfig.getLogger().log(Level.WARNING, "Parse error during admin response processing: {0}", e);
          }
          break;
      }
    } catch (JSONException e) {
      ClientCommandProcessorConfig.getLogger().log(Level.WARNING, "JSON error while getting packet type: {0}", e);
    }
  }

  // DUMP

  public CommandResponse sendDump(ClientRequestHandlerInterface handler) {
    int id;
    if ((id = sendDumpOutputHelper(null, handler)) == -1) {
      return new CommandResponse(ResponseCode.QUERY_PROCESSING_ERROR,
              GNSProtocol.BAD_RESPONSE.toString()
              + " " + GNSProtocol.QUERY_PROCESSING_ERROR.toString() + " "
              + "Error sending dump command to replica");
    }
    waitForDumpResponse(id);
    Map<String, TreeSet<NameRecord>> result = dumpResult.get(id);
    dumpResult.remove(id);
    if (result != null) {
      return new CommandResponse(ResponseCode.NO_ERROR, formatDumpRecords(result, handler));
    } else {
      return new CommandResponse(ResponseCode.QUERY_PROCESSING_ERROR,
              GNSProtocol.BAD_RESPONSE.toString()
              + " " + GNSProtocol.QUERY_PROCESSING_ERROR.toString()
              + " " + "No response to dump command!");
    }
  }

  private void waitForDumpResponse(int id) {
    try {
      ClientCommandProcessorConfig.getLogger().log(Level.FINER, "Waiting for dump response id: {0}", id);
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

      ClientCommandProcessorConfig.getLogger().log(Level.FINER, "Dump response id received: {0}", id);
    } catch (InterruptedException x) {
      ClientCommandProcessorConfig.getLogger().log(Level.SEVERE, "Wait for return packet was interrupted {0}", x);
    }
  }

  @SuppressWarnings("unchecked")
  private String formatDumpRecords(Map<String, TreeSet<NameRecord>> recordsMap,
          ClientRequestHandlerInterface handler) {
    // now process all the records we received

    StringBuilder result = new StringBuilder();
    // are there any NSs that didn't respond?
    Set<String> missingIDs = new HashSet<>(handler.getGnsNodeConfig().getActiveReplicas());
    missingIDs.removeAll(recordsMap.keySet());
    if (missingIDs.size() > 0) {
      result.append("Missing NSs: ");
      result.append(Util.setOfNodeIdToString(missingIDs));
      result.append(LINE_SEPARATOR);
    }
    // process all the entries into a nice string
    for (Map.Entry<String, TreeSet<NameRecord>> entry : recordsMap.entrySet()) {
      ClientCommandProcessorConfig.getLogger().log(Level.FINE, "RECEIVED DUMP RECORD FROM NS: {0}", entry.getKey());
      result.append("========================================================================");
      result.append(LINE_SEPARATOR);
      result.append("Nameserver: ");
      result.append(entry.getKey());
      result.append(" (").append(handler.getGnsNodeConfig().getNodeAddress(entry.getKey()).getHostName());
      result.append(":");
      result.append(handler.getGnsNodeConfig().getNodePort(entry.getKey()));
      result.append(")");
      result.append(LINE_SEPARATOR);
      for (NameRecord record : entry.getValue()) {
        try {
          result.append("  NAME: ");
          result.append(record.getName());
//          result.append(" P: ");
//          result.append(Util.setOfNodeIdToString(record.getPrimaryNameservers()));
          result.append(LINE_SEPARATOR);
          result.append("    VALUE: ");
          try {
            result.append(record.getValuesMap().toString(2));
          } catch (JSONException e) {
            result.append(record.getValuesMap());
          }
          result.append(LINE_SEPARATOR);
        } catch (FieldNotFoundException e) {
          ClientCommandProcessorConfig.getLogger().log(Level.SEVERE, "FieldNotFoundException. {0}", e.getMessage());
        }
      }
    }
    return result.toString();
  }


  public void handleIncomingDumpResponsePackets(JSONObject json, ClientRequestHandlerInterface handler) {
    try {
      switch (Packet.getPacketType(json)) {
        case DUMP_REQUEST:
          try {
            DumpRequestPacket<String> dumpResponse = new DumpRequestPacket<>(json, handler.getGnsNodeConfig());
            int id = dumpResponse.getId();
            // grab or make a new recordsMap
            Map<String, TreeSet<NameRecord>> recordsMap = dumpStorage.get(id);
            if (recordsMap == null) {
              recordsMap = new TreeMap<>();
              dumpStorage.put(id, recordsMap);
            }
            // pull the records out of the dump response and put them in dumpStorage
            JSONArray jsonArray = dumpResponse.getJsonArray();
            String serverID = dumpResponse.getPrimaryNameServer();
            TreeSet<NameRecord> records = new TreeSet<>();
            for (int i = 0; i < jsonArray.length(); i++) {
              records.add(new NameRecord(null, jsonArray.getJSONObject(i)));
            }
            recordsMap.put(serverID, records);
          } catch (JSONException e) {
            ClientCommandProcessorConfig.getLogger().log(Level.WARNING, "JSON error during dump reponse processing: {0}", e);
          }
          break;
        case SENTINAL:
          // put the records in dumpResult and let the folks waiting know they are ready
          try {
            SentinalPacket sentinal = new SentinalPacket(json);
            int id = sentinal.getId();
            ClientCommandProcessorConfig.getLogger().log(Level.FINER, "Processing sentinel for {0}", id);
            synchronized (dumpMonitor) {
              dumpResult.put(id, dumpStorage.get(id));
              dumpStorage.remove(id);
              dumpMonitor.notifyAll();
            }
          } catch (JSONException e) {
            ClientCommandProcessorConfig.getLogger().log(Level.WARNING, "JSON error during dump sentinel processing: {0}", e);
          }
          break;
      }
    } catch (JSONException e) {
      ClientCommandProcessorConfig.getLogger().log(Level.WARNING, "JSON error while getting packet type: {0}", e);
    }
  }


  public Set<String> collectTaggedGuids(String tagName, ClientRequestHandlerInterface handler) {
    int id;
    if ((id = sendDumpOutputHelper(tagName, handler)) == -1) {
      return null;
    }
    waitForDumpResponse(id);
    Map<String, TreeSet<NameRecord>> result = dumpResult.get(id);
    dumpResult.remove(id);

    if (result != null) {
      Set<String> allGuids = new HashSet<>();
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
    ClientCommandProcessorConfig.getLogger().log(Level.INFO, "Sending dump request id = {0}", id);
    try {
      sendAdminPacket(new DumpRequestPacket<String>(id,
              new InetSocketAddress(handler.getNodeAddress().getAddress(),
                      handler.getGnsNodeConfig().getCcpAdminPort(handler.getActiveReplicaID())),
              tagName).toJSONObject(), handler);
    } catch (JSONException e) {
      ClientCommandProcessorConfig.getLogger().log(Level.WARNING, "Ignoring error sending DUMP request for id {0} : {1}", new Object[]{id, e});
      return -1;
    } catch (IOException e) {
      return -1;
    }
    return id;
  }

  private void sendAdminPacket(JSONObject json, ClientRequestHandlerInterface handler) throws IOException {
    if (listenerAdmin != null) {
      ClientCommandProcessorConfig.getLogger().log(Level.INFO, "Sending admin packet = {0}", json);
      listenerAdmin.handlePacket(json, null, handler);
    } else {
      ClientCommandProcessorConfig.getLogger().log(Level.INFO, "LISTENER ADMIN HAS NOT BEEN SET!");
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
