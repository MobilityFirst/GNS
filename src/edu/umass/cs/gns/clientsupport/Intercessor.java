/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.clientsupport;

import edu.umass.cs.gns.database.ColumnFieldType;
import edu.umass.cs.gns.localnameserver.ClientRequestHandlerInterface;
import edu.umass.cs.gns.localnameserver.IntercessorInterface;
import edu.umass.cs.gns.localnameserver.LNSPacketDemultiplexer;
import edu.umass.cs.gns.localnameserver.LocalNameServer;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.packet.*;
import edu.umass.cs.gns.util.NSResponseCode;
import edu.umass.cs.gns.util.ResultValue;
import org.json.JSONException;
import org.json.JSONObject;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import static edu.umass.cs.gns.nsdesign.packet.Packet.getPacketType;
import edu.umass.cs.gns.util.ValuesMap;
import java.util.ArrayList;

/**
 * One of a number of class that implement client support in the GNS server.
 *
 * The intercessor is the primary liason class between the servers (HTTP and new
 * TCP) and the Command Module which handles incoming requests from the servers
 * and the the Local Name Server.
 *
 * Provides support for the AccountAccess, Field Access,
 * FieldMetaData, GroupAccess, and SelectHandler classes.
 *
 * Provides basic methods for reading and writing fields in the GNS. Used
 * by the various classes in the client package to implement writing of fields
 * (for both user data and system data), meta data, groups and perform more
 * sophisticated queries (the select queries).
 *
 * The Intercessor maintains maps of all the read and write queries coming in to the
 * Local Name Server in order to direct incoming responses back to the appropriate sender.
 * Intstrumentation of query response times is also done here.
 *
 * @author westy
 */
public class Intercessor implements IntercessorInterface {
  
  /* Used by the wait/notify calls */
  private static final Object monitor = new Object();
  /* Used by update confirmation */
  private static final Object monitorUpdate = new Object();
  /**
   * We use a ValuesMap for return values even when returning a single value. This lets us use the same structure for single and
   * multiple value returns.
   */
  private static final ConcurrentMap<Integer, QueryResult> queryResultMap;
  private static final Random randomID;

  //public static Transport transport;
  private static final ConcurrentMap<Integer, NSResponseCode> updateSuccessResult;
  // Instrumentation
  private static final ConcurrentMap<Integer, Long> queryTimeStamp;

  private static boolean debuggingEnabled = false;

  static {
    randomID = new Random();
    queryResultMap = new ConcurrentHashMap<Integer, QueryResult>(10, 0.75f, 3);
    queryTimeStamp = new ConcurrentHashMap<Integer, Long>(10, 0.75f, 3);
    updateSuccessResult = new ConcurrentHashMap<Integer, NSResponseCode>(10, 0.75f, 3);
  }

  // local instance of LNSPacketDemultiplexer class.
  private static LNSPacketDemultiplexer lnsPacketDemultiplexer;

  /**
   * Initializes the Intercessor.
   * 
   * @param handler
   */
  public static void init(ClientRequestHandlerInterface handler) {
    lnsPacketDemultiplexer = new LNSPacketDemultiplexer(handler);
  }

//  /**
//   * Returns the local server ID associate with the Intercessor.
//   * 
//   * @return
//   */
//  public static int getLocalServerID() {
//    return localServerID;
//  }

//  /**
//   * Sets the local server ID associate with the Intercessor.
//   * 
//   * @param localServerID
//   */
//  public static void setLocalServerID(int localServerID) {
//    Intercessor.localServerID = localServerID;
//
//    GNS.getLogger().info("Local server id: " + localServerID
//            + " Address: " + LocalNameServer.getGnsNodeConfig().getNodeAddress(localServerID)
//            + " LNS TCP Port: " + GNS.DEFAULT_LNS_TCP_PORT);
//            //+ " LNS TCP Port: " + LocalNameServer.getGnsNodeConfig().getLNSTcpPort(localServerID));
//  }

  /**
   * This is invoked to receive packets. It updates the appropriate map
   * for the id and notifies the appropriate monitor to wake the
   * original caller.
   *
   * @param json
   */
  @Override
  public void handleIncomingPacket(JSONObject json) {
    try {
      switch (getPacketType(json)) {
        case CONFIRM_UPDATE:
        case CONFIRM_ADD:
        case CONFIRM_REMOVE:
          ConfirmUpdatePacket packet = new ConfirmUpdatePacket(json);
          int id = packet.getRequestID();
          //Packet is a response and does not have a response error
          if (debuggingEnabled) {
            GNS.getLogger().fine((packet.isSuccess() ? "Successful" : "Error") + " Update (" + id + ") ");
          }
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
            if (debuggingEnabled) {
              GNS.getLogger().fine("Query (" + id + "): "
                      + dnsResponsePacket.getGuid() + "/" + dnsResponsePacket.getKeyOrKeysString()
                      + " Successful Received: " + dnsResponsePacket.toJSONObject().toString());
            }
            synchronized (monitor) {
              queryResultMap.put(id, new QueryResult(dnsResponsePacket.getRecordValue(), dnsResponsePacket.getResponder()));
              monitor.notifyAll();
            }
          } else {
            if (debuggingEnabled) {
              GNS.getLogger().info("Intercessor: Query (" + id + "): "
                      + dnsResponsePacket.getGuid() + "/" + dnsResponsePacket.getKeyOrKeysString()
                      + " Error Received: " + dnsResponsePacket.getHeader().getResponseCode().name());// + nameRecordPacket.toJSONObject().toString());
            }
            synchronized (monitor) {
              queryResultMap.put(id, new QueryResult(dnsResponsePacket.getHeader().getResponseCode(), dnsResponsePacket.getResponder()));
              monitor.notifyAll();
            }
          }
          break;
        case SELECT_RESPONSE:
          SelectHandler.processSelectResponsePackets(json);
          break;
//        case LNS_TO_NS_COMMAND:
//          LNSToNSCommandRequestHandler.processCommandResponsePackets(json);
//          break;
      }
    } catch (JSONException e) {
      GNS.getLogger().severe("JSON error: " + e);
    }
  }

  /**
   * Sends a query to the Nameserver for a field in a guid.
   * Field is a string naming the field. Field can us dot notation to indicate subfields.
   * Field can also be +ALL+ meaning retrieve all the fields (including internal system fields).
   * Return format should be one of ColumnFieldType.USER_JSON signifying new JSONObject format or
   * ColumnFieldType.LIST_STRING signifying old JSONArray of strings format.
   *
   * This one performs signature and acl checks at the NS unless you set reader (and sig, message) to null).
   *
   * @param name
   * @param field
   * @param reader
   * @param signature
   * @param message
   * @param returnFormat
   * @return
   */
  public static QueryResult sendQuery(String name, String field, String reader, String signature, String message, ColumnFieldType returnFormat) {
    return sendQueryInternal(name, field, null, reader, signature, message, returnFormat);
  }
  
  /**
   * Sends a query to the Nameserver for multiple fields in a guid.
   * Fields is a list if strings naming the fields. Fields can us dot notation to indicate subfields.
   * Return format should be one of ColumnFieldType.USER_JSON signifying new JSONObject format or
   * ColumnFieldType.LIST_STRING signifying old JSONArray of strings format.
   * 
   * This one performs signature and acl checks at the NS unless you set reader (and sig, message) to null).
   * 
   * @param name
   * @param fields
   * @param reader
   * @param signature
   * @param message
   * @param returnFormat
   * @return 
   */
  public static QueryResult sendMultiFieldQuery(String name, ArrayList<String> fields, String reader, String signature, String message, ColumnFieldType returnFormat) {
    return sendQueryInternal(name, null, fields, reader, signature, message, returnFormat);
  }
  
  private static QueryResult sendQueryInternal(String name, String field, ArrayList<String> fields, String reader, String signature, String message, ColumnFieldType returnFormat) {  
    if (debuggingEnabled) {
      GNS.getLogger().fine("Sending query: " + name + " " + field);
    }
    int id = nextQueryRequestID();

    DNSPacket queryrecord = new DNSPacket(DNSPacket.LOCAL_SOURCE_ID, id, name, field, fields,
            returnFormat, reader, signature, message);
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
      if (debuggingEnabled) {
        GNS.getLogger().fine("Waiting for query id: " + id);
      }
      synchronized (monitor) {
        while (!queryResultMap.containsKey(id)) {
          monitor.wait();
        }
      }
      if (debuggingEnabled) {
        GNS.getLogger().fine("Query id response received: " + id);
      }
    } catch (InterruptedException x) {
      GNS.getLogger().severe("Wait for return packet was interrupted " + x);

    }
    Long receiptTime = System.currentTimeMillis(); // instrumentation
    QueryResult result = queryResultMap.get(id);
    queryResultMap.remove(id);
    Long sentTime = queryTimeStamp.get(id); // instrumentation
    queryTimeStamp.remove(id); // instrumentation
    long rtt = receiptTime - sentTime;
    if (debuggingEnabled) {
      GNS.getLogger().fine("Query (" + id + ") RTT = " + rtt + "ms");
      GNS.getLogger().info("Query (" + id + "): " + name + "/" + field + "\n  Returning: " + result.toString());
    }
    result.setRoundTripTime(rtt);
    return result;
  }

  /**
   * This version bypasses any signature checks and is meant for "system" use.
   * @param name
   * @param field
   * @return 
   */
  public static QueryResult sendQueryBypassingAuthentication(String name, String field) {
    return sendQuery(name, field, null, null, null, ColumnFieldType.LIST_STRING);
  }

  /**
   * Sends an AddRecord packet to the Local Name Server with an initial value.
   *
   * @param name
   * @param field
   * @param value
   * @return
   */
  public static NSResponseCode sendAddRecord(String name, String field, ResultValue value) {
    int id = nextUpdateRequestID();
    if (debuggingEnabled) {
      GNS.getLogger().fine("Sending add: " + name + " / " + field + "->" + value);
    }
    AddRecordPacket pkt = new AddRecordPacket(AddRecordPacket.LOCAL_SOURCE_ID, id, name, field, value, LocalNameServer.getAddress(), GNS.DEFAULT_TTL_SECONDS);
    if (debuggingEnabled) {
      GNS.getLogger().fine("#####PACKET: " + pkt.toString());
    }
    try {
      JSONObject json = pkt.toJSONObject();
      injectPacketIntoLNSQueue(json);

    } catch (JSONException e) {
      e.printStackTrace();
    }
    waitForUpdateConfirmationPacket(id);
    NSResponseCode result = updateSuccessResult.get(id);
    updateSuccessResult.remove(id);
    if (debuggingEnabled) {
      GNS.getLogger().fine("Add (" + id + "): " + name + "/" + field + "\n  Returning: " + result);
    }
    return result;
  }

  /**
   * Sends an RemoveRecord packet to the Local Name Server.
   * @param name
   * @return
   */
  public static NSResponseCode sendRemoveRecord(String name) {
    int id = nextUpdateRequestID();
    if (debuggingEnabled) {
      GNS.getLogger().fine("Sending remove: " + name);
    }
    RemoveRecordPacket pkt = new RemoveRecordPacket(RemoveRecordPacket.LOCAL_SOURCE_ID, id, name, LocalNameServer.getAddress());
    try {
      JSONObject json = pkt.toJSONObject();
      injectPacketIntoLNSQueue(json);
    } catch (JSONException e) {
      GNS.getLogger().severe("Problem converting packet before injecting in LNS Queue: " + e);
    }
    waitForUpdateConfirmationPacket(id);
    NSResponseCode result = updateSuccessResult.get(id);
    updateSuccessResult.remove(id);
    if (debuggingEnabled) {
      GNS.getLogger().fine("Remove (" + id + "): " + name + "\n  Returning: " + result);
    }
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
   * @param writer
   * @param signature
   * @param message
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
   * @param writer
   * @param signature
   * @param message
   * @return
   */
  public static NSResponseCode sendUpdateRecord(String name, String key, ResultValue newValue, ResultValue oldValue,
          int argument, UpdateOperation operation,
          String writer, String signature, String message) {
    int id = nextUpdateRequestID();
    sendUpdateRecordHelper(id, name, key, newValue, oldValue, argument, null, operation, writer, signature, message);
    // now we wait until the correct packet comes back
    waitForUpdateConfirmationPacket(id);
    NSResponseCode result = updateSuccessResult.get(id);
    updateSuccessResult.remove(id);
    if (debuggingEnabled) {
      GNS.getLogger().fine("Update (" + id + "): " + name + "/" + key + "\n  Returning: " + result);
    }
    return result;
  }

  /**
   * Sends an update request for an entire JSON Object.
   *
   * @param name
   * @param userJSON
   * @param operation
   * @param writer
   * @param signature
   * @param message
   * @return
   */
  public static NSResponseCode sendUpdateUserJSON(String name, ValuesMap userJSON, UpdateOperation operation,
          String writer, String signature, String message) {
    int id = nextUpdateRequestID();
    sendUpdateRecordHelper(id, name, null, null, null, -1, userJSON, operation, writer, signature, message);
    // now we wait until the correct packet comes back
    waitForUpdateConfirmationPacket(id);
    NSResponseCode result = updateSuccessResult.get(id);
    updateSuccessResult.remove(id);
    if (debuggingEnabled) {
      GNS.getLogger().fine("Update userJSON (" + id + "): " + name + "\n  Returning: " + result);
    }
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
          ResultValue oldValue, int argument, ValuesMap userJSON, UpdateOperation operation,
          String writer, String signature, String message) {

    if (userJSON != null) {
      if (debuggingEnabled) {
        GNS.getLogger().finer("Sending userJSON update: " + name + " : " + userJSON);
      }
    } else {
      if (debuggingEnabled) {
        GNS.getLogger().finer("Sending single field update: " + name + " : " + key + " newValue: " + newValue + " oldValue: " + oldValue);
      }
    }
    UpdatePacket packet = new UpdatePacket(
            UpdatePacket.LOCAL_SOURCE_ID, // means it came from Intercessor
            id,
            name,
            key,
            newValue,
            oldValue,
            argument,
            userJSON,
            operation, LocalNameServer.getAddress(), GNS.DEFAULT_TTL_SECONDS,
            writer, signature, message);
    try {
      JSONObject json = packet.toJSONObject();
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
