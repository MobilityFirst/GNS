package edu.umass.cs.gns.client;

import edu.umass.cs.gns.localnameserver.LNSListener;
import edu.umass.cs.gns.localnameserver.LocalNameServer;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.GNS.PortType;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.nameserver.ValuesMap;
import edu.umass.cs.gns.packet.AddRecordPacket;
import edu.umass.cs.gns.packet.ConfirmUpdateLNSPacket;
import edu.umass.cs.gns.packet.DNSPacket;
import edu.umass.cs.gns.packet.DNSRecordType;
import edu.umass.cs.gns.packet.Header;
import edu.umass.cs.gns.packet.Packet;
import static edu.umass.cs.gns.packet.Packet.*;
import edu.umass.cs.gns.packet.RemoveRecordPacket;
import edu.umass.cs.gns.packet.Transport;
import edu.umass.cs.gns.packet.UpdateAddressPacket;
import edu.umass.cs.gns.util.ConfigFileInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.json.JSONException;
import org.json.JSONObject;

//import edu.umass.cs.gnrs.nameserver.NameRecord;
/**
 *
 * @author westy
 */
public class Intercessor {
  public static final int PORT = 17768;

  // make it a singleton
  public static Intercessor getInstance() {
    return IntercessorHolder.INSTANCE;
  }

  private static class IntercessorHolder {

    private static final Intercessor INSTANCE = new Intercessor();
  }
  //
  private int localServerID = 0;
  /* Used by the wait/notify calls */
  private final Object monitor = new Object();
  /* Used by update confirmation */
  private final Object monitorUpdate = new Object();
  /**
   * We use a ValuesMap for return values even when returning a single value. This lets us use the same structure for single and
   * multiple value returns.
   */
  private static ConcurrentMap<Integer, ValuesMap> queryResult;
  private static final ValuesMap ERRORQUERYRESULT = new ValuesMap();
  private static Random randomID;
  /* Used for sending updates and getting confirmations */
  public static Transport transport;
  private static ConcurrentMap<Integer, Boolean> updateSuccessResult;

  public int getLocalServerID() {
    return localServerID;
  }

  public void setLocalServerID(int localServerID) {
    this.localServerID = localServerID;

    GNS.getLogger().info("Local server id: " + localServerID
            + " Address: " + ConfigFileInfo.getIPAddress(localServerID)
            + " LNS TCP Port: " + ConfigFileInfo.getLNSTcpPort(localServerID));
  }

  private Intercessor() {

    randomID = new Random();
    queryResult = new ConcurrentHashMap<Integer, ValuesMap>(10, 0.75f, 3);
    updateSuccessResult = new ConcurrentHashMap<Integer, Boolean>(10, 0.75f, 3);

    if (StartLocalNameServer.runHttpServer == false) {
      transport = new Transport(-1, PORT); // -1 means use address instead of Host ID
      startCheckForResultThread();
      //startCheckForDumpThread();
    }
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
      switch (getPacketType(json)) {
        case CONFIRM_UPDATE_LNS:
        case CONFIRM_ADD_LNS:
        case CONFIRM_REMOVE_LNS:
          ConfirmUpdateLNSPacket packet = new ConfirmUpdateLNSPacket(json);
          int id = packet.getRequestID();
          //Packet is a response and does not have a response error
          GNS.getLogger().finer((packet.isSuccess() ? "Successful" : "Error") + " Update (" + id + ") ");// + packet.getName() + "/" + packet.getRecordKey().getName());
          synchronized (monitorUpdate) {
            updateSuccessResult.put(id, packet.isSuccess());
            monitorUpdate.notifyAll();
          }
          break;
        case DNS:
          DNSPacket dnsResponsePacket = new DNSPacket(json);
          id = dnsResponsePacket.getQueryId();
          if (dnsResponsePacket.isResponse() && !dnsResponsePacket.containsAnyError()) {
            //Packet is a response and does not have a response error
            GNS.getLogger().finer("Query (" + id + "): "
                    + dnsResponsePacket.getQname() + "/" + dnsResponsePacket.getQrecordKey()
                    + " Successful Received");//  + nameRecordPacket.toJSONObject().toString());
            synchronized (monitor) {
              queryResult.put(id, dnsResponsePacket.getRecordValue());
              monitor.notifyAll();
            }
          } else {
            GNS.getLogger().finer("Intercessor: Query (" + id + "): "
                    + dnsResponsePacket.getQname() + "/" + dnsResponsePacket.getQrecordKey()
                    + " Error Received. ");// + nameRecordPacket.toJSONObject().toString());
            synchronized (monitor) {
              queryResult.put(id, ERRORQUERYRESULT);
              monitor.notifyAll();
            }
          }
          break;
      }
    } catch (JSONException e) {
      GNS.getLogger().severe("JSON error: " + e);
    }
  }

  // QUERYING
  public ArrayList<String> sendQuery(String name, String key) {
    ValuesMap result = sendMultipleReturnValueQuery(name, key);
    if (result != null) {
      return result.get(key);
    } else {
      return null;
    }
  }

  public ValuesMap sendMultipleReturnValueQuery(String name, String key) {
    return sendMultipleReturnValueQuery(name, key, false);
  }

  public ValuesMap sendMultipleReturnValueQuery(String name, String key, boolean removeInternalFields) {
    GNS.getLogger().finer("Sending query ... " + name + " " + key);
    int id = nextQueryRequestID();
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
      GNS.getLogger().finer("waiting for query id ... " + id);
      synchronized (monitor) {
        while (!queryResult.containsKey(id)) {

          monitor.wait();
        }
      }
      GNS.getLogger().finer("query id response received ... " + id);
    } catch (InterruptedException x) {
      GNS.getLogger().severe("Wait for return packet was interrupted " + x);

    }
    ValuesMap result = queryResult.get(id);
    queryResult.remove(id);
    GNS.getLogger().finer("Query (" + id + "): " + name + "/" + key + "\n  Returning: " + result.toString());
    
    if (removeInternalFields) {
      result = removeInternalFields(result);
    }

    if (result == ERRORQUERYRESULT) {
      return null;
    } else {
      return result;
    }
  }
  
  /**
   * Remove any keys / value pairs used internally by the GNS.
   * 
   * @param valuesMap
   * @return 
   */
  private ValuesMap removeInternalFields(ValuesMap valuesMap) {
    for (String key : valuesMap.keySet()) {
      if (GNS.isInternalField(key)) {
        valuesMap.remove(key);
      }
    }
    return valuesMap;
  }

  /**
   * Sends dns query to local name server and returns. Does not wait for response
   *
   * @param name
   * @param key
   * @return true if query sent out successfully, false otherwise
   */
  public boolean sendQueryNoWait(String name, String key) {
    int id = nextQueryRequestID();
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
    GNS.getLogger().finer("Sending add: " + name + "->" + value);
    AddRecordPacket pkt = new AddRecordPacket(id, name, new NameRecordKey(key), value, localServerID, GNS.DEFAULTTTLINSECONDS);
    try {
      JSONObject json = pkt.toJSONObject();
      sendPacket(json);

    } catch (JSONException e) {
      e.printStackTrace();
    }
    waitForUpdateConfirmationPacket(id);
    boolean result = updateSuccessResult.get(id);
    updateSuccessResult.remove(id);
    GNS.getLogger().finer("Add (" + id + "): " + name + "/" + key + "\n  Returning: " + result);
    return result;
  }

  public boolean sendRemoveRecordWithConfirmation(String name) {
    int id = nextUpdateRequestID();
    GNS.getLogger().finer("Sending remove: " + name);
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
    GNS.getLogger().finer("Remove (" + id + "): " + name + "\n  Returning: " + result);
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
    GNS.getLogger().finer("Update (" + id + "): " + name + "/" + key + "\n  Returning: " + result);
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

    GNS.getLogger().finer("sending update: " + name + " : " + key + " newValue: " + newValue + " oldValue: " + oldValue);
    UpdateAddressPacket pkt = new UpdateAddressPacket(Packet.PacketType.UPDATE_ADDRESS_LNS,
            id,
            name, new NameRecordKey(key),
            newValue,
            oldValue,
            operation, localServerID, GNS.DEFAULTTTLINSECONDS);
    try {
      JSONObject json = pkt.toJSONObject();
      sendPacket(json);

    } catch (JSONException e) {
      e.printStackTrace();
    }
  }

  private int nextUpdateRequestID() {
    int id;
    do {
      id = randomID.nextInt();
    } while (updateSuccessResult.containsKey(id));
    return id;
  }

  private int nextQueryRequestID() {
    int id;
    do {
      id = randomID.nextInt();
    } while (queryResult.containsKey(id));
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
      GNS.getLogger().severe("Wait for update success confirmation packet was interrupted " + x);
    }
  }

  private void sendPacket(JSONObject jsonObject) {
    if (StartLocalNameServer.runHttpServer) {
      LNSListener.demultiplexLNSPackets(jsonObject);
    } else {
      try {
        transport.sendPacket(jsonObject, localServerID, PortType.LNS_UDP_PORT);
      } catch (JSONException e) {
        GNS.getLogger().warning("JSON error during send packet: " + e);
      }
    }
  }
}
