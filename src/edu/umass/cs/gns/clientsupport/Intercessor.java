/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.clientsupport;

import edu.umass.cs.gns.localnameserver.ClientRequestHandlerInterface;
import edu.umass.cs.gns.localnameserver.LNSPacketDemultiplexer;
import edu.umass.cs.gns.localnameserver.LocalNameServer;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nsdesign.packet.*;
import edu.umass.cs.gns.util.NSResponseCode;
import edu.umass.cs.gns.util.NameRecordKey;
import edu.umass.cs.gns.util.ResultValue;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static edu.umass.cs.gns.nsdesign.packet.Packet.getPacketType;

/**
 * One of a number of class that implement client support in the GNS server.
 *
 * The intercessor is the primary liason class between the servers (HTTP and new
 * TCP) and the Command Module which handles incoming requests from the servers
 * and the the Local Name Server.
 * It provides support for the AccountAccess, Field Access,
 * FieldMetaData, GroupAccess, and SelectHandler classes.
 *
 * Provides basic methods for reading and writing fields in the GNS. Used
 * by the various classes in the client package to implement writing of fields
 * (for both user data and system data), meta data, groups and perform more
 * sophisticated queries (the select queries).
 *
 * @author westy
 */
public class Intercessor {

  private static int localServerID = 0;
  /* Used by the wait/notify calls */
  private static final Object monitor = new Object();
  /* Used by update confirmation */
  private static final Object monitorUpdate = new Object();
  /**
   * We use a ValuesMap for return values even when returning a single value. This lets us use the same structure for single and
   * multiple value returns.
   */
  private static ConcurrentMap<Integer, QueryResult> queryResultMap;
  private static Random randomID;
  /* Used for sending updates and getting confirmations */
  public static Transport transport;
  private static ConcurrentMap<Integer, NSResponseCode> updateSuccessResult;
  // Instrumentation
  private static ConcurrentMap<Integer, Long> queryTimeStamp;
  

  static {
    randomID = new Random();
    queryResultMap = new ConcurrentHashMap<Integer, QueryResult>(10, 0.75f, 3);
    queryTimeStamp = new ConcurrentHashMap<Integer, Long>(10, 0.75f, 3);
    updateSuccessResult = new ConcurrentHashMap<Integer, NSResponseCode>(10, 0.75f, 3);
  }

  // local instance of LNSPacketDemultiplexer class.
  private static LNSPacketDemultiplexer lnsPacketDemultiplexer;

  
  public static void init(ClientRequestHandlerInterface handler) {
    lnsPacketDemultiplexer = new LNSPacketDemultiplexer(handler);
  }

  public static int getLocalServerID() {
    return localServerID;
  }

  public static void setLocalServerID(int localServerID) {
    Intercessor.localServerID = localServerID;

    GNS.getLogger().info("Local server id: " + localServerID
            + " Address: " + LocalNameServer.getGnsNodeConfig().getNodeAddress(localServerID)
            + " LNS TCP Port: " + LocalNameServer.getGnsNodeConfig().getLNSTcpPort(localServerID));
  }
  /**
   * This is invoked to receive packets. It updates the appropriate map
   * for the id and notifies the appropriate monitor to wake the
   * original caller.
   *
   * @param json
   */
  public static void handleIncomingPackets(JSONObject json) {
    try {
      switch (getPacketType(json)) {
        case CONFIRM_UPDATE:
        case CONFIRM_ADD:
        case CONFIRM_REMOVE:
          ConfirmUpdatePacket packet = new ConfirmUpdatePacket(json);
          int id = packet.getRequestID();
          //Packet is a response and does not have a response error
          if (StartLocalNameServer.debugMode) GNS.getLogger().fine((packet.isSuccess() ? "Successful" : "Error") + " Update (" + id + ") ");
          synchronized (monitorUpdate) {
            updateSuccessResult.put(id, packet.getResponseCode());
            monitorUpdate.notifyAll();
          }
          break;
        case DNS:
          DNSPacket dnsResponsePacket = new DNSPacket(json);
          id = dnsResponsePacket.getQueryId();
          if (dnsResponsePacket.isResponse() && !dnsResponsePacket.containsAnyError()) {
            //Packet is a response and does not have a response error
            if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Query (" + id + "): "
                    + dnsResponsePacket.getGuid() + "/" + dnsResponsePacket.getKey()
                    + " Successful Received");//  + nameRecordPacket.toJSONObject().toString());
            synchronized (monitor) {
              queryResultMap.put(id, new QueryResult(dnsResponsePacket.getRecordValue(), dnsResponsePacket.getResponder()));
              monitor.notifyAll();
            }
          } else {
            if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Intercessor: Query (" + id + "): "
                    + dnsResponsePacket.getGuid() + "/" + dnsResponsePacket.getKey()
                    + " Error Received: " + dnsResponsePacket.getHeader().getResponseCode().name());// + nameRecordPacket.toJSONObject().toString());
            synchronized (monitor) {
              queryResultMap.put(id, new QueryResult(dnsResponsePacket.getHeader().getResponseCode(), dnsResponsePacket.getResponder()));
              monitor.notifyAll();
            }
          }
          break;
        case SELECT_RESPONSE:
          SelectHandler.processSelectResponsePackets(json);
          break;
        case LNS_TO_NS_COMMAND:
          LNSToNSCommandRequestHandler.processCommandResponsePackets(json);
      }
    } catch (JSONException e) {
      GNS.getLogger().severe("JSON error: " + e);
    }
  }

  /**
   * This one performs signature and acl checks at the NS unless you set reader (and sig, message) to null).
   */
  public static QueryResult sendQuery(String name, String key, String reader, String signature, String message) {
    GNS.getLogger().info("Sending query: " + name + " " + key);
    int id = nextQueryRequestID();

    DNSPacket queryrecord = new DNSPacket(DNSPacket.LOCAL_SOURCE_ID, id, name, new NameRecordKey(key), reader, signature, message);
    JSONObject json;
    try {
      json = queryrecord.toJSONObjectQuestion();
      queryTimeStamp.put(id, System.currentTimeMillis()); // rtt instrumentation
      injectPacketIntoLNSQueue(json);

    } catch (JSONException e) {
      e.printStackTrace();
      return null;
    }

    // now we wait until the correct packet comes back
    try {
      GNS.getLogger().info("Waiting for query id: " + id);
      synchronized (monitor) {
        while (!queryResultMap.containsKey(id)) {
          monitor.wait();
        }
      }
      GNS.getLogger().info("Query id response received: " + id);
    } catch (InterruptedException x) {
      GNS.getLogger().severe("Wait for return packet was interrupted " + x);

    }
    Long receiptTime = System.currentTimeMillis(); // instrumentation
    QueryResult result = queryResultMap.get(id);
    queryResultMap.remove(id);
    Long sentTime = queryTimeStamp.get(id); // instrumentation
    queryTimeStamp.remove(id); // instrumentation
    long rtt = receiptTime - sentTime;
    GNS.getLogger().fine("Query (" + id + ") RTT = " + rtt + "ms");
    GNS.getLogger().finer("Query (" + id + "): " + name + "/" + key + "\n  Returning: " + result.toString());
    result.setRoundTripTime(rtt);
    return result;
  }

  /**
   * This version bypasses any signature checks and is meant for "system" use.
   */
  public static QueryResult sendQueryBypassingAuthentication(String name, String key) {
    return sendQuery(name, key, null, null, null);
  }

  /**
   * Sends an AddRecord packet to the Local Name Server with an initial value.
   *
   * @param name
   * @param key
   * @param value
   * @return
   */
  public static NSResponseCode sendAddRecord(String name, String key, ResultValue value) {
    int id = nextUpdateRequestID();
    GNS.getLogger().info("Sending add: " + name + "->" + value);
    AddRecordPacket pkt = new AddRecordPacket(AddRecordPacket.LOCAL_SOURCE_ID, id, name, new NameRecordKey(key), value, localServerID, GNS.DEFAULT_TTL_SECONDS);
    GNS.getLogger().info("#####PACKET: " + pkt.toString());
    try {
      JSONObject json = pkt.toJSONObject();
      injectPacketIntoLNSQueue(json);

    } catch (JSONException e) {
      e.printStackTrace();
    }
    waitForUpdateConfirmationPacket(id);
    NSResponseCode result = updateSuccessResult.get(id);
    updateSuccessResult.remove(id);
    GNS.getLogger().info("Add (" + id + "): " + name + "/" + key + "\n  Returning: " + result);
    return result;
  }

  public static NSResponseCode sendRemoveRecord(String name) {
    int id = nextUpdateRequestID();
    GNS.getLogger().fine("Sending remove: " + name);
    RemoveRecordPacket pkt = new RemoveRecordPacket(RemoveRecordPacket.INTERCESSOR_SOURCE_ID, id, name, localServerID);
    try {
      JSONObject json = pkt.toJSONObject();
      injectPacketIntoLNSQueue(json);
    } catch (JSONException e) {
      e.printStackTrace();
    }
    waitForUpdateConfirmationPacket(id);
    NSResponseCode result = updateSuccessResult.get(id);
    updateSuccessResult.remove(id);
    GNS.getLogger().fine("Remove (" + id + "): " + name + "\n  Returning: " + result);
    return result;
  }

  /**
   * Sends an update request for a single value.
   *
   * @param name
   * @param key
   * @param newValue - the new value to update with
   * @param oldValue - the old value to update with for substitute
   * @param argument - the index for the set operation
   * @param operation
   * @return
   */
  public static NSResponseCode sendUpdateRecord(String name, String key, String newValue, String oldValue,
          int argument, UpdateOperation operation,
          String writer, String signature, String message) {
    return sendUpdateRecord(name, key,
            new ResultValue(Arrays.asList(newValue)),
            oldValue != null ? new ResultValue(Arrays.asList(oldValue)) : null,
            argument,
            operation,
            writer, signature, message);
  }

  /**
   * Sends an update request for a list.
   *
   * @param name
   * @param key
   * @param newValue - the new value to update with
   * @param oldValue - the old value to update with for substitute
   * @param argument - the index for the set operation
   * @param operation
   * @return
   */
  public static NSResponseCode sendUpdateRecord(String name, String key, ResultValue newValue, ResultValue oldValue,
          int argument, UpdateOperation operation,
          String writer, String signature, String message) {
    int id = nextUpdateRequestID();
    sendUpdateRecordHelper(id, name, key, newValue, oldValue, argument, operation, writer, signature, message);
    // now we wait until the correct packet comes back
    waitForUpdateConfirmationPacket(id);
    NSResponseCode result = updateSuccessResult.get(id);
    updateSuccessResult.remove(id);
    GNS.getLogger().finer("Update (" + id + "): " + name + "/" + key + "\n  Returning: " + result);
    return result;
  }

  /**
   * Used internally by the system to send update requests for lists. Ignores signatures and access.
   *
   * @param name
   * @param key
   * @param newValue
   * @param oldValue
   * @param operation
   * @return
   */
  public static NSResponseCode sendUpdateRecordBypassingAuthentication(String name, String key, ResultValue newValue,
          ResultValue oldValue, UpdateOperation operation) {
    // currently don't support the argument parameter
    return sendUpdateRecord(name, key, newValue, oldValue, -1, operation, null, null, null);
  }

  /**
   * Used internally by the system to send update requests. Ignores signatures and access.
   *
   * @param name
   * @param key
   * @param newValue
   * @param oldValue
   * @param operation
   * @return
   */
  public static NSResponseCode sendUpdateRecordBypassingAuthentication(String name, String key, String newValue,
          String oldValue, UpdateOperation operation) {
    // currently don't support the argument parameter
    return sendUpdateRecord(name, key, newValue, oldValue, -1, operation, null, null, null);
  }

  private static void sendUpdateRecordHelper(int id, String name, String key, ResultValue newValue,
          ResultValue oldValue, int argument, UpdateOperation operation,
          String writer, String signature, String message) {

    GNS.getLogger().finer("Sending update: " + name + " : " + key + " newValue: " + newValue + " oldValue: " + oldValue);
    UpdatePacket pkt = new UpdatePacket(
            UpdatePacket.LOCAL_SOURCE_ID, // means it came from Intercessor
            id,
            name, new NameRecordKey(key),
            newValue,
            oldValue,
            argument,
            operation, localServerID, GNS.DEFAULT_TTL_SECONDS,
            writer, signature, message);
    try {
      JSONObject json = pkt.toJSONObject();
      injectPacketIntoLNSQueue(json);

    } catch (JSONException e) {
      e.printStackTrace();
    }
  }

  private static int nextUpdateRequestID() {
    int id;
    do {
      id = randomID.nextInt();
    } while (updateSuccessResult.containsKey(id));
    return id;
  }

  private static int nextQueryRequestID() {
    int id;
    do {
      id = randomID.nextInt();
    } while (queryResultMap.containsKey(id));
    return id;
  }

  private static void waitForUpdateConfirmationPacket(int sequenceNumber) {
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

  /**
   * Helper function for sending JSON packets to the Local Name Server.
   * This does not require a socket based send (just a dispatch)
   * as the LNS runs in the same process as the HTTP server.
   *
   * @param jsonObject
   */
  public static void injectPacketIntoLNSQueue(JSONObject jsonObject) {
   
    boolean isPacketTypeFound = lnsPacketDemultiplexer.handleJSONObject(jsonObject);
    if (isPacketTypeFound == false) {
      GNS.getLogger().severe("Packet type not found at demultiplexer: " + isPacketTypeFound);
    }
  }
}
