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
import edu.umass.cs.gnsserver.gnsApp.QueryResult;
import edu.umass.cs.gnsserver.database.ColumnFieldType;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.IntercessorInterface;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.gnsApp.AppReconfigurableNodeOptions;
import edu.umass.cs.gnsserver.gnsApp.packet.AddRecordPacket;
import edu.umass.cs.gnsserver.gnsApp.packet.ConfirmUpdatePacket;
import edu.umass.cs.gnsserver.gnsApp.packet.DNSPacket;
import edu.umass.cs.gnsserver.gnsApp.NSResponseCode;
import edu.umass.cs.gnsserver.gnsApp.packet.AddBatchRecordPacket;
import edu.umass.cs.gnsserver.utils.ResultValue;
import org.json.JSONException;
import org.json.JSONObject;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import static edu.umass.cs.gnsserver.gnsApp.packet.Packet.getPacketType;
import edu.umass.cs.gnsserver.gnsApp.packet.RemoveRecordPacket;
import edu.umass.cs.gnsserver.gnsApp.packet.UpdatePacket;
import edu.umass.cs.gnsserver.nodeconfig.GNSInterfaceNodeConfig;
import edu.umass.cs.gnsserver.utils.Util;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DeleteServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.utils.DelayProfiler;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

/**
 * One of a number of classes that implement client support in the GNS server.
 *
 * The intercessor is the primary liason class between the servers (HTTP and new
 * TCP) and the Command Module which handles incoming requests from the clients
 * and the the Local Name Server.
 *
 * Provides support for the {@link AccountAccess}, {@link FieldAccess},
 * {@link FieldMetaData}, {@link GroupAccess}, and {@link SelectHandler} classes.
 *
 * Provides basic methods for reading and writing fields in the GNS. Used
 * by the various classes in the client package to implement writing of fields
 * (for both user data and system data), meta data, groups and perform more
 * sophisticated queries (the select queries).
 *
 * The Intercessor maintains maps of all the read and write queries coming in to the
 * server in order to direct incoming responses back to the appropriate sender.
 * 
 * Intstrumentation of query response times is also done here.
 *
 * @author westy
 */
public class Intercessor implements IntercessorInterface {

  /* Used by the wait/notify calls */
  private final Object monitor = new Object();
  /* Used by update confirmation */
  private final Object monitorUpdate = new Object();
  private final Object monitorCreate = new Object();
  private final Object monitorDelete = new Object();
  /**
   * We use a {@link QueryResult} for return values even when returning a single value. 
   * This lets us use the same structure for single and
   * multiple value returns.
   */
  private final ConcurrentMap<Integer, QueryResult<String>> queryResultMap;
  
  private final ConcurrentMap<Integer, NSResponseCode> updateSuccessResult;
  // Instrumentation
  private final ConcurrentMap<Integer, Long> queryTimeStamp;

  private final ConcurrentMap<String, NSResponseCode> createSuccessResult;
  private final ConcurrentMap<String, NSResponseCode> deleteSuccessResult;

  private final Random randomID;
  
  /**
   * True if debugging is enabled.
   */
  public boolean debuggingEnabled = AppReconfigurableNodeOptions.debuggingEnabled;

  {
    randomID = new Random();
    queryResultMap = new ConcurrentHashMap<>(10, 0.75f, 3);
    queryTimeStamp = new ConcurrentHashMap<>(10, 0.75f, 3);
    updateSuccessResult = new ConcurrentHashMap<>(10, 0.75f, 3);
    createSuccessResult = new ConcurrentHashMap<>(10, 0.75f, 3);
    deleteSuccessResult = new ConcurrentHashMap<>(10, 0.75f, 3);
  }

  private final AbstractJSONPacketDemultiplexer ccpPacketDemultiplexer;
  //private ClientRequestHandlerInterface handler;
  private final GNSInterfaceNodeConfig<String> nodeConfig;
  private final InetSocketAddress nodeAddress;

  /**
   * Creates an instance of the Intercessor.
   * 
   * @param nodeAddress
   * @param nodeConfig
   * @param ccpPacketDemultiplexer
   */
  public Intercessor(InetSocketAddress nodeAddress, GNSInterfaceNodeConfig<String> nodeConfig,
          AbstractJSONPacketDemultiplexer ccpPacketDemultiplexer) {
    //this.handler = handler;
    this.nodeConfig = nodeConfig;
    this.nodeAddress = nodeAddress;
    this.ccpPacketDemultiplexer = ccpPacketDemultiplexer;
    if (debuggingEnabled) {
      GNS.getLogger().warning("******** DEBUGGING IS ENABLED IN edu.umass.cs.gnsserver.clientsupport.Intercessor *********");
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
      if (ReconfigurationPacket.isReconfigurationPacket(json)) {
        switch (ReconfigurationPacket.getReconfigurationPacketType(json)) {
          case CREATE_SERVICE_NAME:
            CreateServiceName csnPacket = new CreateServiceName(json, nodeConfig);
            createSuccessResult.put(csnPacket.getServiceName(),
                    csnPacket.isFailed() ? NSResponseCode.ERROR : NSResponseCode.NO_ERROR);
            monitorCreate.notifyAll();
            break;
          case DELETE_SERVICE_NAME:
            DeleteServiceName dsnPacket = new DeleteServiceName(json, nodeConfig);
            synchronized (monitorDelete) {
              deleteSuccessResult.put(dsnPacket.getServiceName(),
                      dsnPacket.isFailed() ? NSResponseCode.ERROR : NSResponseCode.NO_ERROR);
              monitorDelete.notifyAll();
            }
          default:
            break;
        }
      } else {
        switch (getPacketType(json)) {
          case UPDATE_CONFIRM:
          case ADD_CONFIRM:
          case REMOVE_CONFIRM:
            ConfirmUpdatePacket<String> packet = new ConfirmUpdatePacket<String>(json,
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
            DNSPacket<String> dnsResponsePacket = new DNSPacket<>(json, nodeConfig);
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
                        new QueryResult<>(dnsResponsePacket.getRecordValue(),
                                dnsResponsePacket.getResponder()
                                //,dnsResponsePacket.getLookupTime()
                        ));
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
                        new QueryResult<>(dnsResponsePacket.getHeader().getResponseCode(),
                                dnsResponsePacket.getResponder()
                                //,dnsResponsePacket.getLookupTime()
                        ));
                monitor.notifyAll();
              }
            }
            break;
          case SELECT_RESPONSE:
            SelectHandler.processSelectResponsePackets(json, nodeConfig);
            break;
        }
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
   * @param name the record name
   * @param field
   * @param reader
   * @param signature
   * @param message
   * @param returnFormat
   * @return a {@link QueryResult}
   */
  public QueryResult<String> sendSingleFieldQuery(String name, String field, String reader, String signature, String message, ColumnFieldType returnFormat) {
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
   * @param name the record name
   * @param fields
   * @param reader
   * @param signature
   * @param message
   * @param returnFormat
   * @return a {@link QueryResult}
   */
  public QueryResult<String> sendMultiFieldQuery(String name, ArrayList<String> fields, String reader, String signature, String message, ColumnFieldType returnFormat) {
    return sendQueryInternal(name, null, fields, reader, signature, message, returnFormat);
  }

  private QueryResult<String> sendQueryInternal(String name, String field, ArrayList<String> fields, String reader, String signature, String message, ColumnFieldType returnFormat) {
    final Long startTime = System.currentTimeMillis(); // instrumentation
    if (debuggingEnabled) {
      GNS.getLogger().fine("Sending query: " + name + " " + field);
    }
    int id = nextQueryRequestID();

    DNSPacket<String> queryrecord = new DNSPacket<>(null, id, name, field, fields,
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

    Long receiptTime = System.currentTimeMillis(); // instrumentation
    QueryResult<String> result = queryResultMap.remove(id);
    Long sentTime = queryTimeStamp.get(id); // instrumentation
    queryTimeStamp.remove(id); // instrumentation
    long rtt = receiptTime - sentTime;
    if (debuggingEnabled) {
      GNS.getLogger().finer("Query (" + id + ") RTT = " + rtt + "ms");
      GNS.getLogger().fine("Query (" + id + "): " + name + "/" 
              + field + "\n  Returning: " + result.toString());
              //+ field + "\n  Returning: " + result.toReasonableString());
    }
    result.setRoundTripTime(rtt);
    DelayProfiler.updateDelay("sendQueryInternal", startTime);
    return result;

  }

  /**
   * This version bypasses any signature checks and is meant for "system" use.
   *
   * @param name the record name
   * @param field
   * @return a {@link QueryResult}
   */
  public QueryResult<String> sendSingleFieldQueryBypassingAuthentication(String name, String field) {
    return sendSingleFieldQuery(name, field, null, null, null, ColumnFieldType.LIST_STRING);
  }

  /**
   * Sends a query to the Nameserver for all of the fields in a guid.
   * 
   * @param name the record name
   * @param field
   * @return a {@link QueryResult}
   */
  public QueryResult<String> sendFullQueryBypassingAuthentication(String name, String field) {
    return sendSingleFieldQuery(name, GnsProtocol.ALL_FIELDS, null, null, null, ColumnFieldType.USER_JSON);
  }

  /**
   * Sends an AddRecord packet to the CCP with an initial value using a single field.
   *
   * @param name the record name
   * @param field
   * @param value
   * @return a {@link NSResponseCode}
   */
  public NSResponseCode sendAddRecordWithSingleField(String name, String field, ResultValue value) {
    int id = nextUpdateRequestID();
    if (debuggingEnabled) {
      GNS.getLogger().info("Sending add: " + name + " / " + field + "->" + value);
    }
    AddRecordPacket<String> pkt = new AddRecordPacket<>(null, id, name, field, value, nodeAddress);
    if (debuggingEnabled) {
      GNS.getLogger().fine("#####PACKET: " + pkt.toString());
    }
    try {
      JSONObject json = pkt.toJSONObject();
      injectPacketIntoCCPQueue(json);

    } catch (JSONException e) {
      e.printStackTrace();
    }
    // need to wait for a confirmation packet because these requests will be sent out
    // to a reconfigurator
    waitForUpdateConfirmationPacket(id);
    NSResponseCode result = updateSuccessResult.get(id);
    updateSuccessResult.remove(id);
    if (debuggingEnabled) {
      GNS.getLogger().info("Add (" + id + "): " + name + "/" + field + "\n  Returning: " + result);
    }
    return result;
  }

  /**
   * Sends an add record to the Nameserver for all the fields in a JSONObject.
   * 
   * @param name the record name
   * @param value
   * @return a {@link NSResponseCode}
   */
  public NSResponseCode sendFullAddRecord(String name, JSONObject value) {
    int id = nextUpdateRequestID();
    if (debuggingEnabled) {
      GNS.getLogger().info("Sending add: " + name + value);
    }
    AddRecordPacket<String> pkt = new AddRecordPacket<>(null, id, name, value, nodeAddress);
    if (debuggingEnabled) {
      GNS.getLogger().fine("#####PACKET: " + pkt.toString());
    }
    try {
      JSONObject json = pkt.toJSONObject();
      injectPacketIntoCCPQueue(json);

    } catch (JSONException e) {
      e.printStackTrace();
    }
    // need to wait for a confirmation packet because these requests will be sent out
    // to a reconfigurator
    waitForUpdateConfirmationPacket(id);
    NSResponseCode result = updateSuccessResult.get(id);
    updateSuccessResult.remove(id);
    if (debuggingEnabled) {
      GNS.getLogger().info("Add (" + id + "): " + name + "\n  Returning: " + result);
    }
    return result;
  }
  
  /**
   * Sends an AddBatchRecordPacket to the CCP.
   * 
   * @param names
   * @param values
   * @return an {@link NSResponseCode}
   */
  public NSResponseCode sendAddBatchRecord(Set<String> names, Map<String, JSONObject> values) {
    int id = nextUpdateRequestID();
    AddBatchRecordPacket<String> pkt = new AddBatchRecordPacket<>(null, id, names, values, nodeAddress);
    if (debuggingEnabled) {
      //GNS.getLogger().fine("#####PACKET: " + pkt.toString());
      GNS.getLogger().fine("#####PACKET: " + pkt.toReasonableString());
    }
    try {
      JSONObject json = pkt.toJSONObject();
      injectPacketIntoCCPQueue(json);

    } catch (JSONException e) {
      e.printStackTrace();
    }
    // need to wait for a confirmation packet because these requests will be sent out
    // to a reconfigurator
    waitForUpdateConfirmationPacket(id);
    NSResponseCode result = updateSuccessResult.get(id);
    updateSuccessResult.remove(id);
    if (debuggingEnabled) {
      GNS.getLogger().info("Add (" + id + "): " + names.size() + " names\n  Returning: " + result);
    }
    return result;
  }
  
  

  /**
   * Sends an RemoveRecord packet to the server.
   *
   * @param name the record name
   * @return a {@link NSResponseCode}
   */
  public NSResponseCode sendRemoveRecord(String name) {
    int id = nextUpdateRequestID();
    if (debuggingEnabled) {
      GNS.getLogger().fine("Sending remove: " + name);
    }
    RemoveRecordPacket<String> pkt = new RemoveRecordPacket<>(null, id, name,
            nodeAddress);
    try {
      JSONObject json = pkt.toJSONObject();
      injectPacketIntoCCPQueue(json);
    } catch (JSONException e) {
      GNS.getLogger().severe("Problem converting packet before injecting in CCP Queue: " + e);
    }
    // need to wait for a confirmation packet because these requests will be sent out
    // to a reconfigurator
    waitForUpdateConfirmationPacket(id);
    NSResponseCode result = updateSuccessResult.get(id);
    updateSuccessResult.remove(id);
    if (debuggingEnabled) {
      GNS.getLogger().fine("Remove (" + id + "): " + name + "\n  Returning: " + result);
    }
    return result;
  }

  /**
   * Sends an update request for a single value to the server.
   *
   * @param name the record name
   * @param key the field name
   * @param newValue the new value to update with
   * @param oldValue the old value to update with for substitute
   * @param argument the index for the set operation
   * @param operation the {@link UpdateOperation} to perform
   * @param writer the record doing the update
   * @param signature
   * @param message
   * @return a {@link NSResponseCode}
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
   * Sends an update request for a list to the server and waits for a response.
   *
   * @param name the record name
   * @param key the field name
   * @param newValue the new value to update with
   * @param oldValue the old value to update with for substitute
   * @param argument the index for the set operation
   * @param operation the {@link UpdateOperation} to perform
   * @param writer the record doing the update
   * @param signature
   * @param message
   * @return a {@link NSResponseCode}
   */
  public NSResponseCode sendUpdateRecord(String name, String key, ResultValue newValue, ResultValue oldValue,
          int argument, UpdateOperation operation,
          String writer, String signature, String message) {
    return sendUpdateRecord(name, key, newValue, oldValue, argument, operation, writer, signature, message, true);
  }

  /**
   * Sends an update request for a list to the server.
   * 
   * @param name the record name
   * @param key the field name
   * @param newValue the new value to update with
   * @param oldValue the old value to update with for substitute
   * @param argument the index for the set operation
   * @param operation the {@link UpdateOperation} to perform
   * @param writer the record doing the writing
   * @param signature
   * @param message
   * @param wait determines whether we wait for a response
   * @return a {@link NSResponseCode}
   */
  public NSResponseCode sendUpdateRecord(String name, String key, ResultValue newValue, ResultValue oldValue,
          int argument, UpdateOperation operation,
          String writer, String signature, String message, boolean wait) {
    int id = nextUpdateRequestID();
    sendUpdateRecordHelper(id, name, key, newValue, oldValue, argument, null, operation, writer, signature, message);
    // now we wait until the correct packet comes back, but only for
    // updates that are sent out to ARs and not handled locally
    if (wait) {
      waitForUpdateConfirmationPacket(id);
    }
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
   * @param name the record name
   * @param userJSON we're replacing multiple fields
   * @param operation the {@link UpdateOperation} to perform
   * @param writer the record doing the update
   * @param signature
   * @param message
   * @return a {@link NSResponseCode}
   */
  public NSResponseCode sendUpdateUserJSON(String name, ValuesMap userJSON, UpdateOperation operation,
          String writer, String signature, String message) {
    return sendUpdateUserJSON(name, userJSON, operation, writer, signature, message, true);
  }

  /**
   * Sends an update request for an entire JSON Object.
   * 
   * @param name the record name
   * @param userJSON we're replacing multiple fields
   * @param operation the {@link UpdateOperation} to perform
   * @param writer the record doing the update
   * @param signature
   * @param message
   * @param wait indicates if we wait for a confirmation packet
   * @return a {@link NSResponseCode}
   */
  public NSResponseCode sendUpdateUserJSON(String name, ValuesMap userJSON, UpdateOperation operation,
          String writer, String signature, String message, boolean wait) {
    int id = nextUpdateRequestID();
    sendUpdateRecordHelper(id, name, null, null, null, -1, userJSON, operation, writer, signature, message);
    // now we wait until the correct packet comes back, but only for
    // updates that are sent out to ARs and not handled locally
    if (wait) {
      waitForUpdateConfirmationPacket(id);
    }
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
   * @param name the record name
   * @param key the field name
   * @param newValue the new value to update with
   * @param oldValue the old value to update with for substitute
   * @param operation the {@link UpdateOperation} to perform
   * @return a {@link NSResponseCode}
   */
  public NSResponseCode sendUpdateRecordBypassingAuthentication(String name, String key, ResultValue newValue,
          ResultValue oldValue, UpdateOperation operation) {
    // currently don't support the argument parameter
    return sendUpdateRecord(name, key, newValue, oldValue, -1, operation, null, null, null);
  }

  /**
   * Used internally by the system to send update requests. Ignores signatures and access.
   *
   * @param name the record name
   * @param key the field name
   * @param newValue the new value to update with
   * @param oldValue the old value to update with for substitute
   * @param operation the {@link UpdateOperation} to perform
   * @return a {@link NSResponseCode}
   */
  public NSResponseCode sendUpdateRecordBypassingAuthentication(String name, String key, String newValue,
          String oldValue, UpdateOperation operation) {
    // currently don't support the argument parameter
    return sendUpdateRecord(name, key, newValue, oldValue, -1, operation, null, null, null);
  }

  /**
   * Sends either a full JSON Object update or (newer style) or
   * an old-style single field update depending on the values of
   * newValue (old-style) and userJSON (new-style) one of which should be null
   * in normal use.
   *
   * @param id
   * @param name the record name
   * @param key the field name
   * @param newValue the new value to update with
   * @param oldValue the old value to update with for substitute
   * @param argument the index for the set operation
   * @param userJSON used instead of newValue, oldValue and argument when we're replacing multiple fields
   * @param operation the {@link UpdateOperation} to perform
   * @param writer the record doing the update
   * @param signature
   * @param message
   */
  private void sendUpdateRecordHelper(int id, String name, String key, ResultValue newValue,
          ResultValue oldValue, int argument, ValuesMap userJSON, UpdateOperation operation,
          String writer, String signature, String message) {

    if (userJSON != null) {
      if (debuggingEnabled) {
        //GNS.getLogger().finer("Sending userJSON update: " + name + " : " + userJSON.toString());
        GNS.getLogger().finer("Sending userJSON update: " + name + " : " + userJSON.toReasonableString());
      }
    } else {
      if (debuggingEnabled) {
        GNS.getLogger().finer("Sending single field update: " + name + " : "
                + key + " newValue: " + newValue + " oldValue: " + oldValue);
      }
    }
    UpdatePacket<String> packet = new UpdatePacket<String>(
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
