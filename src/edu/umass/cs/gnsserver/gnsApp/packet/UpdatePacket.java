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
package edu.umass.cs.gnsserver.gnsApp.packet;

import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.UpdateOperation;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.utils.JSONUtils;
import edu.umass.cs.gnsserver.utils.ResultValue;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;

import java.net.InetSocketAddress;

import net.sourceforge.sizeof.SizeOf;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * Implements the packet transmitted from a local name server to update the name to value mapping.
 *
 * A name could have several keys and values, but this packet contains only has option to specify a single key.
 * Therefore, we can only modify one key of the name record using this packet.
 *
 * There are two classes of update operations: (1) updates that modify the keys or values of a name record.
 * (2) Upserts which update an existing record or else create a new name record.
 *
 * Regarding TTL field: If we are using upserts, if the upserts leads to creating a new name record, then TTL value
 * in this packet is used to set ttl for this name record. The value given in the TTL field in not
 * considered in other cases.
 *
 * Future work: I think we should allow clients to change TTLs for a name record after a record is created.
 *
 * A client must set the <code>requestID</code> field correctly to received a response.
 *
 * Once this packet reaches local name server, local name server sets the
 * <code>localNameServerID</code> and <code>CCPRequestID</code> field correctly before forwarding packet
 * to name server.
 *
 * When name server replies to the client, it uses a different packet type: <code>ConfirmUpdateCCPPacket</code>.
 * But it uses fields in this packet in sending the reply.
 *
 * @author Westy
 * @param <NodeIDType>
 */
public class UpdatePacket<NodeIDType> extends BasicPacketWithSignatureInfoAndNSAndCCP implements
        ReplicableRequest {

  private final static String REQUESTID = "reqID";
  private final static String CCPREQUESTID = "CCPreqID";
  private final static String NAME = "name";
  private final static String RECORDKEY = "recordkey";
  private final static String NEWVALUE = "newvalue";
  private final static String OLDVALUE = "oldvalue";
  private final static String USERJSON = "userjson";
  private final static String SOURCE_ID = "sourceId";
  private final static String TTL = "ttl";
  private final static String OPERATION = "operation";
  private final static String ARGUMENT = "argument";

  //
  // We have two ids in here. First one (requestID) is used by the entity making the initial request (often the intercessor).
  // Second (CCPRequestID) is used by the CCP to keep track of it's update records.
  //
  /**
   * Unique identifier used by the entity making the initial request to confirm
   */
  private int requestID;
  /**
   * The ID the CCP uses to for bookkeeping
   */
  private int CCPRequestID;
  /**
   * Name (the GUID) *
   */
  private final String name;
  /**
   * The key of the value key pair.
   */
  private final String key;
  /**
   * Value for updating.
   * This is mutually exclusive with userJSON below - one or the other will be used in any operation, but not both.
   * For simplicity's sake even when we are just updating with a single value it is converted into a
   * list (which is what ResultValue really is).
   *
   */
  private final ResultValue updateValue;
  /**
   * The old value to replace in a substitute operation.
   * Not used when userJSON is specified.
   * For simplicity's sake even when we are just updating with a single value it is converted into a
   * list (which is what ResultValue really is).
   */
  private final ResultValue oldValue;
  /**
   * Currently this is used in as the index in the set operation.
   * Not used when userJSON is specified.
   * When used with set the value in updateValue will be the one to set the element to.
   */
  private final int argument;
  /**
   * Allows us to update the entire user value of this guid record at once.
   * This is mutually exclusive with updateValue (and oldValue, argument) above -
   * one or the other will be used in any operation, but not both.
   */
  private final ValuesMap userJSON;
  /**
   * The operation to perform *
   */
  private final UpdateOperation operation;

  /**
   * The originator of this packet, if it is LOCAL_SOURCE_ID (ie, -1) that means go back the Intercessor otherwise
   * it came from another server.
   */
  private NodeIDType sourceId;
  /**
   * Time to live
   */
  private final int ttl;
  /**
   * The stop requests needsCoordination() method must return true by default.
   */
  private boolean needsCoordination = true;

  /**
   * Constructs a new UpdateAddressPacket with the given parameters.
   * Used by client support to create a packet to send to the CCP to update a single field.
   *
   * @param sourceId
   * @param requestID
   * @param name
   * @param recordKey
   * @param newValue
   * @param oldValue
   * @param argument
   * @param operation
   * @param lnsAddress
   * @param ttl
   * @param writer
   * @param signature
   * @param message
   */
  public UpdatePacket(NodeIDType sourceId, int requestID, String name, String recordKey,
          ResultValue newValue, ResultValue oldValue, int argument, UpdateOperation operation, InetSocketAddress lnsAddress, int ttl,
          String writer, String signature, String message) {
    this(sourceId, requestID, -1, name, recordKey, newValue, oldValue, argument, null, operation, lnsAddress,
            null, ttl, writer, signature, message);
  }

  /**
   * Constructs a new UpdateAddressPacket with the given parameters.
   * Used by client support to create a packet to send to the CCP with a JSONObject as the update value.
   *
   * @param sourceId
   * @param requestID
   * @param name
   * @param userJSON
   * @param operation
   * @param lnsAddress
   * @param ttl
   * @param writer
   * @param signature
   * @param message
   */
  public UpdatePacket(NodeIDType sourceId, int requestID, String name, ValuesMap userJSON, UpdateOperation operation, InetSocketAddress lnsAddress, int ttl,
          String writer, String signature, String message) {
    this(sourceId, requestID, -1, name, null, null, null, -1, userJSON, operation, lnsAddress,
            null, ttl, writer, signature, message);
  }

  /**
   * Constructs a new UpdateAddressPacket with the given parameters.
   * Used by client support to create a packet to send to the CCP.
   *
   * @param sourceId
   * @param requestID
   * @param name
   * @param recordKey
   * @param newValue
   * @param oldValue
   * @param argument
   * @param userJSON - if this is specified newValue and oldValue will be null
   * @param operation
   * @param lnsAddress
   * @param ttl
   * @param writer
   * @param signature
   * @param message
   */
  public UpdatePacket(NodeIDType sourceId, int requestID, String name, String recordKey,
          ResultValue newValue, ResultValue oldValue, int argument, ValuesMap userJSON, UpdateOperation operation, InetSocketAddress lnsAddress, int ttl,
          String writer, String signature, String message) {
    this(sourceId, requestID, -1, name, recordKey, newValue, oldValue, argument, userJSON, operation, lnsAddress,
            null, ttl, writer, signature, message);
  }

  /**
   * Constructs a new UpdateAddressPacket with the given parameters.
   * Used by the CCP to create a packet to send to the NS.
   *
   * @param sourceId
   * @param requestID
   * @param CCPRequestID
   * @param name
   * @param recordKey
   * @param newValue
   * @param argument
   * @param oldValue
   * @param userJSON
   * @param operation
   * @param lnsAddress
   * @param nameServerId
   * @param ttl
   * @param writer
   * @param signature
   * @param message
   */
  @SuppressWarnings("unchecked")
  public UpdatePacket(
          NodeIDType sourceId,
          int requestID, int CCPRequestID,
          String name, String recordKey,
          ResultValue newValue,
          ResultValue oldValue,
          int argument,
          ValuesMap userJSON,
          UpdateOperation operation,
          InetSocketAddress lnsAddress, NodeIDType nameServerId, int ttl,
          // signature info
          String writer, String signature, String message) {
    // include the signature info
    super(nameServerId, lnsAddress, writer, signature, message);
    this.type = Packet.PacketType.UPDATE;
    this.sourceId = sourceId != null ? sourceId : null;
    this.requestID = requestID;
    this.CCPRequestID = CCPRequestID;
    this.name = name;
    this.key = recordKey;
    this.operation = operation;
    this.updateValue = newValue;
    this.oldValue = oldValue;
    this.argument = argument;
    this.userJSON = userJSON;
    this.ttl = ttl;
  }

  /**
   * Constructs a new UpdateAddressPacket from a JSONObject.
   *
   * @param json JSONObject that represents UpdatedAddressPacket.
   * @param unstringer
   * @throws org.json.JSONException
   */
  @SuppressWarnings("unchecked")
  public UpdatePacket(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
    // include the address and signature info
    super(json.has(NAMESERVER_ID) ? unstringer.valueOf(json.getString(NAMESERVER_ID)) : null,
            json.optString(CCP_ADDRESS, null), json.optInt(CCP_PORT, INVALID_PORT),
            json.optString(ACCESSOR, null), json.optString(SIGNATURE, null), json.optString(MESSAGE, null));
    this.type = Packet.getPacketType(json);
    this.sourceId = json.has(SOURCE_ID) ? unstringer.valueOf(json.getString(SOURCE_ID)) : null;
    this.requestID = json.getInt(REQUESTID);
    this.CCPRequestID = json.getInt(CCPREQUESTID);
    this.name = json.getString(NAME);
    this.key = json.has(RECORDKEY) ? json.getString(RECORDKEY) : null;
    this.operation = UpdateOperation.valueOf(json.getString(OPERATION));
    this.updateValue = json.has(NEWVALUE) ? JSONUtils.JSONArrayToResultValue(json.getJSONArray(NEWVALUE)) : null;
    this.argument = json.optInt(ARGUMENT, -1);
    this.userJSON = json.has(USERJSON) ? new ValuesMap(json.getJSONObject(USERJSON)) : null;
    this.oldValue = json.has(OLDVALUE) ? JSONUtils.JSONArrayToResultValue(json.getJSONArray(OLDVALUE)) : null;
    this.ttl = json.getInt(TTL);
  }

  /**
   *
   * Converts a UpdatedAddressPacket to a JSONObject
   *
   * @return JSONObject that represents UpdatedAddressPacket
   * @throws org.json.JSONException
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    addToJSONObject(json);
    return json;
  }

  /**
   * Add the fields of this UpdatedAddressPacket to an existing JSONObject.
   *
   * @param json
   * @throws org.json.JSONException
   */
  @Override
  public void addToJSONObject(JSONObject json) throws JSONException {
    Packet.putPacketType(json, getType());
    super.addToJSONObject(json); // include the signature info and other info
    json.put(REQUESTID, requestID);
    json.put(SOURCE_ID, sourceId);
    json.put(CCPREQUESTID, CCPRequestID);
    json.put(NAME, name);
    if (key != null) {
      json.put(RECORDKEY, key);
    }

    if (updateValue != null) {
      json.put(NEWVALUE, new JSONArray(updateValue));
    }
    if (oldValue != null) {
      json.put(OLDVALUE, new JSONArray(oldValue));
    }
    if (argument != -1) {
      json.put(ARGUMENT, argument);
    }
    if (userJSON != null) {
      json.put(USERJSON, userJSON);
    }
    json.put(OPERATION, operation.name());
    //json.put(LOCAL_NAMESERVER_ID, localNameServerId);
    //json.put(NAMESERVER_ID, nameServerId.toString());
    json.put(TTL, ttl);
  }

  /**
   * Return the id used the client support for bookkeeping.
   *
   * @return an int
   */
  public int getRequestID() {
    return requestID;
  }

  /**
   * Return the CCP request id.
   *
   * @return the id which is used by the CCP for bookkeeping
   */
  public int getCCPRequestID() {
    return CCPRequestID;
  }

  /**
   * Set the CCP request id.
   *
   * @param id the id to set
   */
  public void setCCPRequestID(int id) {
    this.CCPRequestID = id;
  }

  /**
   * Return the name.
   *
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * Return the key.
   *
   * @return the recordKey
   */
  public String getKey() {
    return key;
  }

  /**
   * Return the value.
   *
   * @return the new value
   */
  public ResultValue getUpdateValue() {
    return updateValue;
  }

  /**
   * Return the old value.
   *
   * @return the old value
   */
  public ResultValue getOldValue() {
    return oldValue;
  }

  /**
   * Return the argument.
   *
   * @return the argument
   */
  public int getArgument() {
    return argument;
  }

  /**
   * Return the user JSON Object.
   *
   * @return ValuesMap
   */
  public ValuesMap getUserJSON() {
    return userJSON;
  }

  /**
   * Return the source id.
   *
   * @return the source id
   */
  public NodeIDType getSourceId() {
    return sourceId;
  }

  /**
   * Return the ttl.
   *
   * @return the ttl
   */
  public int getTTL() {
    return ttl;
  }

  /**
   * Return the operation.
   *
   * @return the operation
   */
  public UpdateOperation getOperation() {
    return operation;
  }

  // For InterfaceRequest
  @Override
  public String getServiceName() {
    return this.name;
  }

  /**
   * Set the request id.
   *
   * @param requestID
   */
  public void setRequestID(int requestID) {
    this.requestID = requestID;
  }

  // For InterfaceReplicableRequest
  @Override
  public boolean needsCoordination() {
    return needsCoordination;
  }

  @Override
  public void setNeedsCoordination(boolean needsCoordination) {
    this.needsCoordination = needsCoordination;
  }

  //
  // TEST CODE
  //
  /**
   * The main routine. For testing.
   *
   * @param args
   */
  @SuppressWarnings("unchecked")
  public static void main(String[] args) {
    ResultValue x = new ResultValue();
    x.add("12345678");
    //
    UpdatePacket<String> up = new UpdatePacket(null, 32234234, 123, "12322323",
            "EdgeRecord", x, null, -1, null, UpdateOperation.SINGLE_FIELD_APPEND_WITH_DUPLICATION, null, "123",
            GNS.DEFAULT_TTL_SECONDS, null, null, null);

    SizeOf.skipStaticField(true); //java.sizeOf will not compute static fields
    SizeOf.skipFinalField(false); //java.sizeOf will not compute final fields
    SizeOf.skipFlyweightObject(false); //java.sizeOf will not compute well-known flyweight objects
    printSize(up);
    int size = 0;
    try {
      size = up.toJSONObject().toString().getBytes().length;
    } catch (JSONException e) {
      e.printStackTrace();
    }
    System.out.println("Network Size: " + size + " bytes");

  }

  static void printSize(Object object) {
    System.out.println("Java Size: " + SizeOf.deepSizeOf(object) + " bytes"); //this will print the object size in bytes
  }
}
