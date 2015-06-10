  /*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport;

import edu.umass.cs.gns.database.ColumnFieldType;
import edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport.IntercessorInterface;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.newApp.AppReconfigurableNodeOptions;
import edu.umass.cs.gns.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.newApp.packet.AddRecordPacket;
import edu.umass.cs.gns.newApp.packet.ConfirmUpdatePacket;
import edu.umass.cs.gns.newApp.packet.DNSPacket;
import edu.umass.cs.gns.util.NSResponseCode;
import edu.umass.cs.gns.util.ResultValue;

import org.json.JSONException;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static edu.umass.cs.gns.newApp.packet.Packet.getPacketType;
import edu.umass.cs.gns.newApp.packet.RemoveRecordPacket;
import edu.umass.cs.gns.newApp.packet.UpdatePacket;
import edu.umass.cs.gns.util.ValuesMap;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;

import java.net.InetSocketAddress;
import java.util.ArrayList;

/**
 * One of a number of class that implement client support in the GNS server.
 *
 * The intercessor is the primary liason class between the servers (HTTP and new
 * TCP) and the Command Module which handles incoming requests from the clients
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
public class Intercessor<NodeIDType> implements IntercessorInterface {

  /* Used by the wait/notify calls */
  private final Object monitor = new Object();
  /* Used by update confirmation */
  private final Object monitorUpdate = new Object();
  /**
   * We use a ValuesMap for return values even when returning a single value. This lets us use the same structure for single and
   * multiple value returns.
   */
  private final ConcurrentMap<Integer, QueryResult> queryResultMap;
  private final Random randomID;

  //public Transport transport;
  private final ConcurrentMap<Integer, NSResponseCode> updateSuccessResult;
  // Instrumentation
  private final ConcurrentMap<Integer, Long> queryTimeStamp;

  public boolean debuggingEnabled = AppReconfigurableNodeOptions.debuggingEnabled;

  {
    randomID = new Random();
    queryResultMap = new ConcurrentHashMap<Integer, QueryResult>(10, 0.75f, 3);
    queryTimeStamp = new ConcurrentHashMap<Integer, Long>(10, 0.75f, 3);
    updateSuccessResult = new ConcurrentHashMap<Integer, NSResponseCode>(10, 0.75f, 3);
  }

  private AbstractJSONPacketDemultiplexer ccpPacketDemultiplexer;
  //private ClientRequestHandlerInterface<NodeIDType> handler;
  private GNSNodeConfig<NodeIDType> nodeConfig;
  private InetSocketAddress nodeAddress;

  public Intercessor(InetSocketAddress nodeAddress, GNSNodeConfig<NodeIDType> nodeConfig,
          AbstractJSONPacketDemultiplexer ccpPacketDemultiplexer) {
    //this.handler = handler;
    this.nodeConfig = nodeConfig;
    this.nodeAddress = nodeAddress;
    this.ccpPacketDemultiplexer = ccpPacketDemultiplexer;
    if (debuggingEnabled) {
      GNS.getLogger().warning("******** DEBUGGING IS ENABLED IN edu.umass.cs.gns.clientsupport.Intercessor *********");
    }
  }

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
        case UPDATE_CONFIRM:
        case ADD_CONFIRM:
        case REMOVE_CONFIRM:
          ConfirmUpdatePacket<NodeIDType> packet = new ConfirmUpdatePacket<NodeIDType>(json,
                  nodeConfig);
          int id = packet.getRequestID();
          //Packet is a response and does not have a response error
          if (debuggingEnabled) {
            GNS.getLogger().fine((packet.isSuccess() ? "Successful" : "Error") + " Update/Add/Remove (" + id + ") ");
          }
          synchronized (monitorUpdate) {
            updateSuccessResult.put(id, packet.getResponseCode());
            monitorUpdate.notifyAll();
          }
          break;
        case DNS:
          DNSPacket<NodeIDType> dnsResponsePacket = new DNSPacket<NodeIDType>(json, nodeConfig);
          id = dnsResponsePacket.getQueryId();
          if (dnsResponsePacket.isResponse() && !dnsResponsePacket.containsAnyError()) {
            //Packet is a response and does not have a response error
            if (debuggingEnabled) {
              GNS.getLogger().fine("Query (" + id + "): "
                      + dnsResponsePacket.getGuid() + "/" + dnsResponsePacket.getKeyOrKeysString()
                      + " Successful Received: " + dnsResponsePacket.toJSONObject().toString());
            }
            synchronized (monitor) {
              queryResultMap.put(id,
                      new QueryResult<NodeIDType>(dnsResponsePacket.getRecordValue(),
                              dnsResponsePacket.getResponder(),
                              dnsResponsePacket.getLookupTime()));
              monitor.notifyAll();
            }
          } else {
            if (debuggingEnabled) {
              GNS.getLogger().info("Intercessor: Query (" + id + "): "
                      + dnsResponsePacket.getGuid() + "/" + dnsResponsePacket.getKeyOrKeysString()
                      + " Error Received: " + dnsResponsePacket.getHeader().getResponseCode().name());// + nameRecordPacket.toJSONObject().toString());
            }
            synchronized (monitor) {
              queryResultMap.put(id,
                      new QueryResult<NodeIDType>(dnsResponsePacket.getHeader().getResponseCode(),
                              dnsResponsePacket.getResponder(),
                              dnsResponsePacket.getLookupTime()));
              monitor.notifyAll();
            }
          }
          break;
        case SELECT_RESPONSE:
          SelectHandler.processSelectResponsePackets(json, nodeConfig);
          break;
      }
    } catch (JSONException e) {
      GNS.getLogger().severe("JSON error: " + e);
    }
  }

  /**
   * Sends a query to the Nameserver for a field in a guid.
   * Field is a string naming the field. Field can use dot notation to indicate subfields.
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
  public QueryResult sendQuery(String name, String field, String reader, String signature, String message, ColumnFieldType returnFormat) {
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
  public QueryResult sendMultiFieldQuery(String name, ArrayList<String> fields, String reader, String signature, String message, ColumnFieldType returnFormat) {
    return sendQueryInternal(name, null, fields, reader, signature, message, returnFormat);
  }

  private QueryResult sendQueryInternal(String name, String field, ArrayList<String> fields, String reader, String signature, String message, ColumnFieldType returnFormat) {
    if (debuggingEnabled) {
      GNS.getLogger().fine("Sending query: " + name + " " + field);
    }
    int id = nextQueryRequestID();

    DNSPacket<NodeIDType> queryrecord = new DNSPacket<NodeIDType>(null, id, name, field, fields,
            returnFormat, reader, signature, message);
    JSONObject json;
    try {
      json = queryrecord.toJSONObjectQuestion();
      queryTimeStamp.put(id, System.currentTimeMillis()); // rtt instrumentation
      injectPacketIntoCCPQueue(json);

    } catch (JSONException e) {
      e.printStackTrace();
      return null;
    }

    // now we wait until the correct packet comes back
//    try {
//      if (debuggingEnabled) {
//        GNS.getLogger().fine("Waiting for query id: " + id);
//      }
//      final Long waitStart = System.currentTimeMillis(); // instrumentation
//      synchronized (monitor) {
//        while (!queryResultMap.containsKey(id)) {
//          monitor.wait();
//        }
//      }
//      DelayProfiler.update("IntercessorWait", waitStart);
//      if (debuggingEnabled) {
//        GNS.getLogger().fine("Query id response received: " + id);
//      }
//    } catch (InterruptedException x) {
//      GNS.getLogger().severe("Wait for return packet was interrupted " + x);
//
//    }
    Long receiptTime = System.currentTimeMillis(); // instrumentation
    QueryResult result = queryResultMap.remove(id);
    //queryResultMap.remove(id);
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
   *
   * @param name
   * @param field
   * @return
   */
  public QueryResult sendQueryBypassingAuthentication(String name, String field) {
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
  public NSResponseCode sendAddRecord(String name, String field, ResultValue value) {
    int id = nextUpdateRequestID();
    if (debuggingEnabled) {
      GNS.getLogger().info("Sending add: " + name + " / " + field + "->" + value);
    }
    AddRecordPacket<NodeIDType> pkt = new AddRecordPacket<NodeIDType>(null, id, name, field, value,
            nodeAddress, GNS.DEFAULT_TTL_SECONDS);
    if (debuggingEnabled) {
      GNS.getLogger().fine("#####PACKET: " + pkt.toString());
    }
    try {
      JSONObject json = pkt.toJSONObject();
      injectPacketIntoCCPQueue(json);

    } catch (JSONException e) {
      e.printStackTrace();
    }
    waitForUpdateConfirmationPacket(id);
    NSResponseCode result = updateSuccessResult.get(id);
    updateSuccessResult.remove(id);
    if (debuggingEnabled) {
      GNS.getLogger().info("Add (" + id + "): " + name + "/" + field + "\n  Returning: " + result);
    }
    return result;
  }

  /**
   * Sends an RemoveRecord packet to the Local Name Server.
   *
   * @param name
   * @return
   */
  public NSResponseCode sendRemoveRecord(String name) {
    int id = nextUpdateRequestID();
    if (debuggingEnabled) {
      GNS.getLogger().fine("Sending remove: " + name);
    }
    RemoveRecordPacket<NodeIDType> pkt = new RemoveRecordPacket<NodeIDType>(null, id, name,
            nodeAddress);
    try {
      JSONObject json = pkt.toJSONObject();
      injectPacketIntoCCPQueue(json);
    } catch (JSONException e) {
      GNS.getLogger().severe("Problem converting packet before injecting in CCP Queue: " + e);
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
  public NSResponseCode sendUpdateRecord(String name, String key, String newValue, String oldValue,
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
  public NSResponseCode sendUpdateRecord(String name, String key, ResultValue newValue, ResultValue oldValue,
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
  public NSResponseCode sendUpdateUserJSON(String name, ValuesMap userJSON, UpdateOperation operation,
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
  public NSResponseCode sendUpdateRecordBypassingAuthentication(String name, String key, ResultValue newValue,
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
  public NSResponseCode sendUpdateRecordBypassingAuthentication(String name, String key, String newValue,
          String oldValue, UpdateOperation operation) {
    // currently don't support the argument parameter
    return sendUpdateRecord(name, key, newValue, oldValue, -1, operation, null, null, null);
  }

  private void sendUpdateRecordHelper(int id, String name, String key, ResultValue newValue,
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
    UpdatePacket<NodeIDType> packet = new UpdatePacket<NodeIDType>(
            null, // means it came from Intercessor
            id,
            name,
            key,
            newValue,
            oldValue,
            argument,
            userJSON,
            operation, nodeAddress, GNS.DEFAULT_TTL_SECONDS,
            writer, signature, message);
    try {
      JSONObject json = packet.toJSONObject();
      injectPacketIntoCCPQueue(json);

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
    } while (queryResultMap.containsKey(id));
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

  /**
   * Helper function for sending JSON packets to the Client Command Processor.
   * This does not require a socket based send (just a dispatch)
   * as the CCP runs in the same process.
   *
   * @param jsonObject
   */
  public void injectPacketIntoCCPQueue(JSONObject jsonObject) {

    boolean isPacketTypeFound = ccpPacketDemultiplexer.handleMessage(jsonObject);
    if (isPacketTypeFound == false) {
      GNS.getLogger().severe("Packet type not found at demultiplexer: " + isPacketTypeFound);
    }
  }
}
