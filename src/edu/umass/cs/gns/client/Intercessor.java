package edu.umass.cs.gns.client;

import edu.umass.cs.gns.httpserver.Protocol;
import edu.umass.cs.gns.localnameserver.LNSListener;
import edu.umass.cs.gns.localnameserver.LocalNameServer;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.GNS.PortType;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nameserver.NameRecord;
import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.nameserver.ValuesMap;
import edu.umass.cs.gns.nameserver.recordExceptions.FieldNotFoundException;
import edu.umass.cs.gns.packet.AddRecordPacket;
import edu.umass.cs.gns.packet.AdminRequestPacket;
import edu.umass.cs.gns.packet.ConfirmUpdateLNSPacket;
import edu.umass.cs.gns.packet.DNSPacket;
import edu.umass.cs.gns.packet.DNSRecordType;
import edu.umass.cs.gns.packet.DumpRequestPacket;
import edu.umass.cs.gns.packet.Header;
import edu.umass.cs.gns.packet.Packet;
import static edu.umass.cs.gns.packet.Packet.*;
import edu.umass.cs.gns.packet.Packet.PacketType;
import edu.umass.cs.gns.packet.RemoveRecordPacket;
import edu.umass.cs.gns.packet.Transport;
import edu.umass.cs.gns.packet.UpdateAddressPacket;
import edu.umass.cs.gns.packet.UpdateOperation;
import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.gns.util.JSONUtils;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.commons.cli.Options;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

//import edu.umass.cs.gnrs.nameserver.NameRecord;
/**
 *
 * @author westy
 */
public class Intercessor {

  public static final int PORT = 17768;
  private static final String LINE_SEPARATOR = System.getProperty("line.separator");

  // make it a singleton
  public static Intercessor getInstance() {
    return IntercessorHolder.INSTANCE;
  }

  private static class IntercessorHolder {

    private static final Intercessor INSTANCE = new Intercessor();
  }
  private int localServerID = 0;
  private static Options commandLineOptions;
  /**
   * Used by the wait/notify calls *
   */
  private final Object monitor = new Object();
  /**
   * We use a ValuesMap for return values even when returning a signal value. This lets us use the same structure for single and
   * multiple value returns.
   */
  private static ConcurrentMap<Integer, ValuesMap> queryResult;
  private static final ValuesMap ERRORQUERYRESULT = new ValuesMap();
  private static Random randomID;
  /* Used for sending updates and getting confirmations */
  public static Transport transport;
  /* Used by update confirmation */
  private final Object monitorUpdate = new Object();
  private static ConcurrentMap<Integer, Boolean> updateSuccessResult;

  public int getLocalServerID() {
    return localServerID;
  }

  public void setLocalServerID(int localServerID) {
    this.localServerID = localServerID;

    GNS.getLogger().info("Local server id: " + localServerID
            + " Address: " + ConfigFileInfo.getIPAddress(localServerID)
            + " Port: " + ConfigFileInfo.getLNSTcpPort(localServerID));
  }

//  /**
//   * call method setLocalServerID before you create transport object
//   */
//  public void createTransportObject() {
//    int port = ConfigFileInfo.getLNSUpdatePort(localServerID) + 10;
//    transport = new Transport(-1, port); // -1 means use address instead of Host ID
//
//  }
  private Intercessor() {

    randomID = new Random();
    queryResult = new ConcurrentHashMap<Integer, ValuesMap>(10, 0.75f, 3);
    updateSuccessResult = new ConcurrentHashMap<Integer, Boolean>(10, 0.75f, 3);
    StartLocalNameServer.debugMode = true;
//    try {
//
//    } catch (IOException e) {
//      e.printStackTrace();
//    }

    if (StartLocalNameServer.runHttpServer == false) {
      transport = new Transport(-1, PORT); // -1 means use address instead of Host ID
      startCheckForResultThread();
    }

//    startCheckForQueryResultThread();
//    startCheckForUpdateResultThread();
  }

  /**
   * Query (lookup), update, add, remove commands are sent and received on UPDATE port.
   */
  private void startCheckForResultThread() {
    new Thread("Intercessor-checkForResult") {
      @Override
      public void run() {
        GNS.getLogger().info("Intercessor update result thread started.");
        while (true) {
          JSONObject json = transport.readPacket();
          checkForResult(json);
        }
      }
    }.start();
  }

  public void checkForResult(JSONObject json) {
    try {

      if (StartLocalNameServer.debugMode) {
//        GNS.getLogger().fine("Intercessor Recvd result: " + json);
      }
      switch (getPacketType(json)) {
        case CONFIRM_UPDATE_LNS:
        case CONFIRM_ADD_LNS:
        case CONFIRM_REMOVE_LNS:
//              if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("Intercessor: RECVD UPDATE CONFIRM PACKET");
          ConfirmUpdateLNSPacket packet = new ConfirmUpdateLNSPacket(json);
          int id = packet.getRequestID();
          //Packet is a response and does not have a response error
          if (StartLocalNameServer.debugMode) {
            GNS.getLogger().finer((packet.isSuccess() ? "Successful" : "Error") + " Update (" + id + "): " + packet.getName() + "/" + packet.getRecordKey().getName());
          }
          synchronized (monitorUpdate) {
            updateSuccessResult.put(id, packet.isSuccess());
            monitorUpdate.notifyAll();
          }
          break;
        case DNS:
          DNSPacket dnsResponsePacket = new DNSPacket(json);
          id = dnsResponsePacket.getQueryId();
//              if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("Intercessor: RECVD LOOKUP CONFIRM PACKET");
          if (dnsResponsePacket.isResponse() && !dnsResponsePacket.containsAnyError()) {
            //Packet is a response and does not have a response error
            if (StartLocalNameServer.debugMode) {
              GNS.getLogger().fine("Query (" + id + "): "
                      + dnsResponsePacket.getQname() + "/" + dnsResponsePacket.getQrecordKey()
                      + " Successful Received");//  + nameRecordPacket.toJSONObject().toString());
            }
            synchronized (monitor) {
              queryResult.put(id, dnsResponsePacket.getRecordValue());
              monitor.notifyAll();
            }
          } else {
            if (StartLocalNameServer.debugMode) {
              GNS.getLogger().fine("Intercessor: Query (" + id + "): "
                      + dnsResponsePacket.getQname() + "/" + dnsResponsePacket.getQrecordKey()
                      + " Error Received. ");// + nameRecordPacket.toJSONObject().toString());
            }
            synchronized (monitor) {
              queryResult.put(id, ERRORQUERYRESULT);
              monitor.notifyAll();
            }
          }
          break;
      }

    } catch (Exception e) {
      if (StartLocalNameServer.debugMode) {
        GNS.getLogger().severe("Error check for update result: " + e);
      }
      e.printStackTrace();
    }
  }

  public ArrayList<String> sendQuery(String name, String key) {
    ValuesMap result = sendMultipleReturnValueQuery(name, key);
    if (result != null) {
      return result.get(key);
    } else {
      return null;
    }
  }

  // QUERYING
  public ValuesMap sendMultipleReturnValueQuery(String name, String key) {
    int id = randomID.nextInt();
    GNS.getLogger().fine("sending query ... " + name + " " + key);
    //Generate unique id for the query
    while (queryResult.containsKey(id)) {
      id = randomID.nextInt();
    }
    Header header = new Header(id, DNSRecordType.QUERY, DNSRecordType.RCODE_NO_ERROR);
    DNSPacket queryrecord = new DNSPacket(header, name, new NameRecordKey(key), LocalNameServer.nodeID);
    JSONObject json;
    try {
      json = queryrecord.toJSONObjectQuestion();

      sendPacket(json);

    } catch (JSONException e) {
      e.printStackTrace();
      return null;
    }

    // now we wait until the correct packet comes back
    try {
      GNS.getLogger().fine("waiting for query id ... " + id);
      synchronized (monitor) {
        while (!queryResult.containsKey(id)) {

          monitor.wait();
        }
      }
      GNS.getLogger().fine("query id response received ... " + id);
    } catch (InterruptedException x) {
      if (StartLocalNameServer.debugMode) {
        GNS.getLogger().severe("Wait for return packet was interrupted " + x);
      }
    }
    ValuesMap result = queryResult.get(id);
    queryResult.remove(id);
    if (StartLocalNameServer.debugMode) {
      GNS.getLogger().fine("Query (" + id + "): " + name + "/" + key + "\n  Returning: " + result.toString());
    }

    if (result == ERRORQUERYRESULT) {
      return null;
    } else {
      return result;
    }
  }

  /**
   * Sends dns query to local name server and returns. Does not wait for response
   *
   * @param name
   * @param key
   * @return true if query sent out successfully, false otherwise
   */
  public boolean sendQueryNoWait(String name, String key) {
    // ABHIGYAN
    int id = randomID.nextInt();

    //Generate unique id for the query
    while (queryResult.containsKey(id)) {
      id = randomID.nextInt();
    }
    Header header = new Header(id, DNSRecordType.QUERY, DNSRecordType.RCODE_NO_ERROR);
    DNSPacket queryrecord = new DNSPacket(header, name, new NameRecordKey(key), LocalNameServer.nodeID);
    JSONObject json;
    try {
      json = queryrecord.toJSONObjectQuestion();
      sendPacket(json);

    } catch (JSONException e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }

  public boolean sendAddRecordWithConfirmation(String name, String key, String value) {
    return sendAddRecordWithConfirmation(name, key, new ArrayList<String>(Arrays.asList(value)));
  }

  public boolean sendAddRecordWithConfirmation(String name, String key, ArrayList<String> value) {
    int id = nextUpdateRequestID();
    if (StartLocalNameServer.debugMode) {
      GNS.getLogger().fine("sending add: " + name + "->" + value);
    }
    AddRecordPacket pkt = new AddRecordPacket(id, name, new NameRecordKey(key), value, localServerID);
    try {
      JSONObject json = pkt.toJSONObject();
      sendPacket(json);

    } catch (JSONException e) {
      e.printStackTrace();
    }
    waitForUpdateConfirmationPacket(id);
    boolean result = updateSuccessResult.get(id);
    updateSuccessResult.remove(id);
    if (StartLocalNameServer.debugMode) {
      GNS.getLogger().fine("Add (" + id + "): " + name + "/" + key + "\n  Returning: " + result);
    }
    return result;
  }

  public boolean sendRemoveRecordWithConfirmation(String name) {
    int id = nextUpdateRequestID();
    if (StartLocalNameServer.debugMode) {
      GNS.getLogger().fine("sending remove: " + name);
    }
    RemoveRecordPacket pkt = new RemoveRecordPacket(id, name, localServerID);
    try {
      JSONObject json = pkt.toJSONObject();
      sendPacket(json);
    } catch (JSONException e) {
      e.printStackTrace();
    }
    waitForUpdateConfirmationPacket(id);
    boolean result = updateSuccessResult.get(id);
    updateSuccessResult.remove(id);
    if (StartLocalNameServer.debugMode) {
      GNS.getLogger().fine("Remove (" + id + "): " + name + "\n  Returning: " + result);
    }
    return result;
  }

  public boolean sendUpdateRecordWithConfirmation(String name, String key, String newValue, String oldValue, UpdateOperation operation) {
    return sendUpdateRecordWithConfirmation(name, key,
            new ArrayList<String>(Arrays.asList(newValue)),
            oldValue != null ? new ArrayList<String>(Arrays.asList(oldValue)) : null,
            operation);
  }

  public boolean sendUpdateRecordWithConfirmation(String name, String key, ArrayList<String> newValue, ArrayList<String> oldValue, UpdateOperation operation) {
    int id = nextUpdateRequestID();
    sendUpdateWithSequenceNumber(name, key, newValue, oldValue, id, 0, operation);
    // now we wait until the correct packet comes back
    waitForUpdateConfirmationPacket(id);
    boolean result = updateSuccessResult.get(id);
    updateSuccessResult.remove(id);
    GNS.getLogger().fine("Update (" + id + "): " + name + "/" + key + "\n  Returning: " + result);
    return result;
  }

  /**
   *
   * Sends an update packet to the GNRS with a host of values.
   *
   * Abighyan should explain the sequence numbers and how they are used here.
   *
   * @param name
   * @param key
   * @param newValue
   * @param id
   * @param sequenceNumber
   * @param operation
   */
  public void sendUpdateWithSequenceNumber(String name, String key, ArrayList<String> newValue,
          ArrayList<String> oldValue, int id, int sequenceNumber, UpdateOperation operation) {

    GNS.getLogger().fine("sending update: " + name + " : " + key + " newValue: " + newValue + " oldValue: " + oldValue);
    UpdateAddressPacket pkt = new UpdateAddressPacket(Packet.PacketType.UPDATE_ADDRESS_LNS,
            sequenceNumber, id,
            name, new NameRecordKey(key),
            newValue,
            oldValue,
            operation, localServerID);
    try {
      JSONObject json = pkt.toJSONObject();
      sendPacket(json);

    } catch (JSONException e) {
      e.printStackTrace();
    }
  }

  // Miscellaneous operations
  public void sendDeleteAllRecords() {
    AdminRequestPacket packet = new AdminRequestPacket(AdminRequestPacket.AdminOperation.DELETEALLRECORDS);
    try {
      sendTCPPacket(packet.toJSONObject(), localServerID, GNS.PortType.LNS_ADMIN_PORT);
    } catch (Exception e) {
      if (StartLocalNameServer.debugMode) {
        GNS.getLogger().warning("Ignoring this error while sending DELETEALLRECORDS request: " + e);
      }
      e.printStackTrace();
    }
  }

  // Miscellaneous operations
  public void sendClearCache() {
    AdminRequestPacket packet = new AdminRequestPacket(AdminRequestPacket.AdminOperation.CLEARCACHE);
    try {
      sendTCPPacket(packet.toJSONObject(), localServerID, GNS.PortType.LNS_ADMIN_PORT);
    } catch (Exception e) {
      if (StartLocalNameServer.debugMode) {
        GNS.getLogger().warning("Ignoring this error while sending CLEARCACHE request: " + e);
      }
      e.printStackTrace();
    }
  }

  // the nuclear option
  public void sendResetDB() {
    AdminRequestPacket packet = new AdminRequestPacket(AdminRequestPacket.AdminOperation.RESETDB);
    try {
      sendTCPPacket(packet.toJSONObject(), localServerID, GNS.PortType.LNS_ADMIN_PORT);
    } catch (Exception e) {
      if (StartLocalNameServer.debugMode) {
        GNS.getLogger().warning("Ignoring this error while sending RESETDB request: " + e);
      }
      e.printStackTrace();
    }
  }

  public String sendDump() {
    if (!sendDumpOutputHelper(null)) {
      return Protocol.BADRESPONSE + " " + Protocol.QUERYPROCESSINGERROR + " " + "Error sending dump command to LNS";
    }
    ServerSocket adminSocket;
    if ((adminSocket = sendDumpGetInputSocket()) == null) {
      return Protocol.BADRESPONSE + " " + Protocol.QUERYPROCESSINGERROR + " " + "Error while waiting for dump response";
    }
    // Read the results
    Map<Integer, TreeSet<NameRecord>> recordsMap = new TreeMap<Integer, TreeSet<NameRecord>>();

    while (true) {
      try {
        Socket asock = adminSocket.accept();
        //Read the packet from the input stream
        JSONObject json = getJSONObjectFrame(asock);
        if (StartLocalNameServer.debugMode) {
          GNS.getLogger().fine("read " + json.toString());
        }
        // keep reading until we see a packet that isn't a dump request
        if (getPacketType(json) == PacketType.DUMP_REQUEST) {
          DumpRequestPacket returnedDumpRequestPacket = new DumpRequestPacket(json);
          //if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("read " + returnedDumpRequestPacket.toString());
          JSONArray jsonArray = returnedDumpRequestPacket.getJsonArray();
          int serverID = returnedDumpRequestPacket.getPrimaryNameServer();
          TreeSet<NameRecord> records = new TreeSet<NameRecord>();
          for (int i = 0; i < jsonArray.length(); i++) {
            records.add(new NameRecord(jsonArray.getJSONObject(i)));
          }
          recordsMap.put(serverID, records);
        } else { // we've read the sentinal packet
          if (StartLocalNameServer.debugMode) {
            GNS.getLogger().fine("read sentinal" + json.toString());
          }
          adminSocket.close();
          break;
        }
        asock.close();
      } catch (Exception e) {
        if (StartLocalNameServer.debugMode) {
          GNS.getLogger().warning("Caught and ignoring error : " + e);
        }
      }
    }
    // now process all the records we received
    StringBuilder result = new StringBuilder();
    for (Entry<Integer, TreeSet<NameRecord>> entry : recordsMap.entrySet()) {
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
          GNS.getLogger().severe(" FieldNotFoundException. " + e.getMessage());
          e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
      }
    }
    return result.toString();
  }

  // uses a variation of the dump protocol to get all the guids that contain a tag
  public HashSet<String> collectTaggedGuids(String tagName) {
    HashSet<String> guids = new HashSet<String>();
    if (!sendDumpOutputHelper(tagName)) {
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
        if (StartLocalNameServer.debugMode) {
          GNS.getLogger().fine("read " + json.toString());
        }
        // keep reading until we see a packet that isn't a dump request
        if (getPacketType(json) == PacketType.DUMP_REQUEST) {
          DumpRequestPacket returnedDumpRequestPacket = new DumpRequestPacket(json);
          //if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("read " + returnedDumpRequestPacket.toString());
          JSONArray jsonArray = returnedDumpRequestPacket.getJsonArray();
          guids.addAll(JSONUtils.JSONArrayToArrayList(jsonArray));
        } else { // we've read the sentinal packet
          if (StartLocalNameServer.debugMode) {
            GNS.getLogger().fine("read sentinal" + json.toString());
          }
          adminSocket.close();
          break;
        }
      } catch (Exception e) {
        if (StartLocalNameServer.debugMode) {
          GNS.getLogger().warning("Caught and ignoring error : " + e);
        }
      }
    }
    return guids;
  }

  private ServerSocket sendDumpGetInputSocket() {
    if (StartLocalNameServer.debugMode) {
      GNS.getLogger().fine("waiting for responses dump");
    }
    ServerSocket adminSocket;
    try {
      adminSocket = new ServerSocket(ConfigFileInfo.getDumpReponsePort(localServerID));
    } catch (Exception e) {
      if (StartLocalNameServer.debugMode) {
        GNS.getLogger().warning("Error creating admin socket: " + e);
      }
      e.printStackTrace();
      return null;
    }
    return adminSocket;
  }

  private boolean sendDumpOutputHelper(String tagName) {
    // send the request out to the local name server
    if (StartLocalNameServer.debugMode) {
      GNS.getLogger().fine("sending dump");
    }
    DumpRequestPacket dumpRequestPacket = new DumpRequestPacket(localServerID, tagName);
    try {
      sendTCPPacket(dumpRequestPacket.toJSONObject(), localServerID, GNS.PortType.LNS_ADMIN_PORT);
    } catch (Exception e) {
      if (StartLocalNameServer.debugMode) {
        GNS.getLogger().warning("Ignoring error sending DUMP request: " + e);
      }
      e.printStackTrace();
      return false;
    }
    return true;
  }

  // helpers
  private int nextUpdateRequestID() {
    int id;
    do {
      id = randomID.nextInt();
    } while (updateSuccessResult.containsKey(id));
    return id;
  }

  private void waitForUpdateConfirmationPacket(int sequenceNumber) {
    try {
      synchronized (monitorUpdate) {
        while (!updateSuccessResult.containsKey(sequenceNumber)) {
          monitorUpdate.wait();
        }
      }
    } catch (InterruptedException x) {
      if (StartLocalNameServer.debugMode) {
        GNS.getLogger().severe("Wait for update success confirmation packet was interrupted " + x);
      }
    }
  }

  private void sendPacket(JSONObject jsonObject) {
    if (StartLocalNameServer.runHttpServer) {
      LNSListener.demultiplexLNSPackets(jsonObject);
    } else {
      try {
        transport.sendPacket(jsonObject, localServerID, PortType.LNS_UDP_PORT);
      } catch (JSONException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    }

  }
}
