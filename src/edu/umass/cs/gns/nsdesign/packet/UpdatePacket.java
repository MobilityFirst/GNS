/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.nsdesign.packet;

import edu.umass.cs.gns.clientsupport.UpdateOperation;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;
import edu.umass.cs.gns.util.JSONUtils;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.util.ValuesMap;
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
 * <code>localNameServerID</code> and <code>LNSRequestID</code> field correctly before forwarding packet
 * to name server.
 *
 * When name server replies to the client, it uses a different packet type: <code>ConfirmUpdateLNSPacket</code>.
 * But it uses fields in this packet in sending the reply.
 *
 * @author Westy
 */
public class UpdatePacket extends BasicPacketWithSignatureInfoAndLnsAddress {

  private final static String REQUESTID = "reqID";
  private final static String LocalNSREQUESTID = "LNSreqID";
  private final static String NAME = "name";
  private final static String RECORDKEY = "recordkey";
  private final static String NEWVALUE = "newvalue";
  private final static String OLDVALUE = "oldvalue";
  private final static String USERJSON = "userjson";
  private final static String NAMESERVER_ID = "nsID";
  //private final static String LOCAL_NAMESERVER_ID = "lnsID";
  private final static String SOURCE_ID = "sourceId";
  private final static String TTL = "ttl";
  private final static String OPERATION = "operation";
  private final static String ARGUMENT = "argument";

  /**
   * This is the source ID of a packet that should be returned to the intercessor of the LNS.
   * Otherwise the sourceId field contains the number of the NS who made the request.
   */
  public final static NodeId<String> LOCAL_SOURCE_ID = GNSNodeConfig.INVALID_NAME_SERVER_ID;
  //
  // We have two ids in here. First one (requestID) is used by the entity making the initial request (often the intercessor).
  // Second (LNSRequestID) is used by the LNS to keep track of it's update records.
  //
  /**
   * Unique identifier used by the entity making the initial request to confirm
   */
  private int requestID;
  /**
   * The ID the LNS uses to for bookkeeping
   */
  private int LNSRequestID;
  /**
   * Name (the GUID) *
   */
  private String name;
  /**
   * The key of the value key pair.
   */
  private String recordKey;
  /**
   * Value for updating.
   * This is mutually exclusive with userJSON below - one or the other will be used in any operation, but not both.
   * For simplicity's sake even when we are just updating with a single value it is converted into a
   * list (which is what ResultValue really is).
   *
   */
  private ResultValue updateValue;
  /**
   * The old value to replace in a substitute operation.
   * Not used when userJSON is specified.
   * For simplicity's sake even when we are just updating with a single value it is converted into a
   * list (which is what ResultValue really is).
   */
  private ResultValue oldValue;
  /**
   * Currently this is used in as the index in the set operation.
   * Not used when userJSON is specified.
   * When used with set the value in updateValue will be the one to set the element to.
   */
  private int argument;
  /**
   * Allows us to update the entire user value of this guid record at once.
   * This is mutually exclusive with updateValue (and oldValue, argument) above -
   * one or the other will be used in any operation, but not both.
   */
  private ValuesMap userJSON;
  /**
   * The operation to perform *
   */
  private UpdateOperation operation;
  /**
   * Name server transmitting this packet *
   */
  private NodeId<String> nameServerId;
  /**
   * The originator of this packet, if it is LOCAL_SOURCE_ID (ie, -1) that means go back the Intercessor otherwise
   * it came from another server.
   */
  private NodeId<String> sourceId;
  /**
   * Time to live
   */
  private int ttl;

  /**
   * Constructs a new UpdateAddressPacket with the given parameters.
   * Used by client support to create a packet to send to the LNS to update a single field.
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
  public UpdatePacket(NodeId<String> sourceId, int requestID, String name, String recordKey,
          ResultValue newValue, ResultValue oldValue, int argument, UpdateOperation operation, InetSocketAddress lnsAddress, int ttl,
          String writer, String signature, String message) {
    this(sourceId, requestID, -1, name, recordKey, newValue, oldValue, argument, null, operation, lnsAddress, 
            GNSNodeConfig.INVALID_NAME_SERVER_ID, ttl,
            writer, signature, message);
  }

  /**
   * Constructs a new UpdateAddressPacket with the given parameters.
   * Used by client support to create a packet to send to the LNS with a JSONObject as the update value.
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
  public UpdatePacket(NodeId<String> sourceId, int requestID, String name, ValuesMap userJSON, UpdateOperation operation, InetSocketAddress lnsAddress, int ttl,
          String writer, String signature, String message) {
    this(sourceId, requestID, -1, name, null, null, null, -1, userJSON, operation, lnsAddress, 
            GNSNodeConfig.INVALID_NAME_SERVER_ID, ttl,
            writer, signature, message);
  }

  /**
   * Constructs a new UpdateAddressPacket with the given parameters.
   * Used by client support to create a packet to send to the LNS.
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
  public UpdatePacket(NodeId<String> sourceId, int requestID, String name, String recordKey,
          ResultValue newValue, ResultValue oldValue, int argument, ValuesMap userJSON, UpdateOperation operation, InetSocketAddress lnsAddress, int ttl,
          String writer, String signature, String message) {
    this(sourceId, requestID, -1, name, recordKey, newValue, oldValue, argument, userJSON, operation, lnsAddress, 
            GNSNodeConfig.INVALID_NAME_SERVER_ID, ttl,
            writer, signature, message);
  }

  /**
   * Constructs a new UpdateAddressPacket with the given parameters.
   * Used by the LNS to create a packet to send to the NS.
   *
   * @param sourceId
   * @param requestID
   * @param LNSRequestID
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
  public UpdatePacket(
          NodeId<String> sourceId,
          int requestID, int LNSRequestID,
          String name, String recordKey,
          ResultValue newValue,
          ResultValue oldValue,
          int argument,
          ValuesMap userJSON,
          UpdateOperation operation,
          InetSocketAddress lnsAddress, NodeId<String> nameServerId, int ttl,
          // signature info
          String writer, String signature, String message) {
    // include the signature info
    super(lnsAddress, writer, signature, message);
    this.type = Packet.PacketType.UPDATE;
    this.sourceId = sourceId;
    this.requestID = requestID;
    this.LNSRequestID = LNSRequestID;
    this.name = name;
    this.recordKey = recordKey;
    this.operation = operation;
    this.updateValue = newValue;
    this.oldValue = oldValue;
    this.argument = argument;
    this.userJSON = userJSON;
    //this.localNameServerId = localNameServerId;
    this.nameServerId = nameServerId;
    this.ttl = ttl;
  }

  /**
   * Constructs a new UpdateAddressPacket from a JSONObject.
   *
   * @param json JSONObject that represents UpdatedAddressPacket.
   * @throws org.json.JSONException
   */
  public UpdatePacket(JSONObject json) throws JSONException {
    // include the address and signature info
    super(json.optString(LNS_ADDRESS, null), json.optInt(LNS_PORT, INVALID_PORT),
            json.optString(ACCESSOR, null), json.optString(SIGNATURE, null), json.optString(MESSAGE, null));
    this.type = Packet.getPacketType(json);
    this.sourceId = new NodeId<String>(json.getString(SOURCE_ID));
    this.requestID = json.getInt(REQUESTID);
    this.LNSRequestID = json.getInt(LocalNSREQUESTID);
//    this.NSRequestID = json.getInt(NameServerREQUESTID);
    this.name = json.getString(NAME);
    this.recordKey = json.has(RECORDKEY) ? json.getString(RECORDKEY) : null;
    this.operation = UpdateOperation.valueOf(json.getString(OPERATION));
    this.updateValue = json.has(NEWVALUE) ? JSONUtils.JSONArrayToResultValue(json.getJSONArray(NEWVALUE)) : null;
    this.argument = json.optInt(ARGUMENT, -1);
    this.userJSON = json.has(USERJSON) ? new ValuesMap(json.getJSONObject(USERJSON)) : null;
    this.oldValue = json.has(OLDVALUE) ? JSONUtils.JSONArrayToResultValue(json.getJSONArray(OLDVALUE)) : null;
    //this.localNameServerId = json.getInt(LOCAL_NAMESERVER_ID);
    this.nameServerId = new NodeId<String>(json.getString(NAMESERVER_ID));
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
    super.addToJSONObject(json); // include the signature info
    Packet.putPacketType(json, getType());
    super.addToJSONObject(json);
    json.put(REQUESTID, requestID);
    json.put(SOURCE_ID, sourceId.get());
    json.put(LocalNSREQUESTID, LNSRequestID);
    json.put(NAME, name);
    if (recordKey != null) {
      json.put(RECORDKEY, recordKey);
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
    json.put(NAMESERVER_ID, nameServerId.get());
    json.put(TTL, ttl);
  }

  /**
   * Return the id used the client support for bookkeeping.
   *
   * @return
   */
  public int getRequestID() {
    return requestID;
  }

  /**
   * @return the id which is used by the LNS for bookkeeping
   */
  public int getLNSRequestID() {
    return LNSRequestID;
  }

  /**
   * @param id the id to set
   */
  public void setLNSRequestID(int id) {
    this.LNSRequestID = id;
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @return the recordKey
   */
  public String getRecordKey() {
    return recordKey;
  }

  /**
   * @return the new value
   */
  public ResultValue getUpdateValue() {
    return updateValue;
  }

  public ResultValue getOldValue() {
    return oldValue;
  }

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

//  /**
//   * @return the localNameServerId
//   */
//  public int getLocalNameServerId() {
//    return localNameServerId;
//  }
//
//  public void setLocalNameServerId(int localNameServerId) {
//    this.localNameServerId = localNameServerId;
//  }
  /**
   * @return the nameServerId
   */
  public NodeId<String> getNameServerId() {
    return nameServerId;
  }

  public void setNameServerId(NodeId<String> nameServerId) {
    this.nameServerId = nameServerId;
  }

  public NodeId<String> getSourceId() {
    return sourceId;
  }

  /**
   * @return the ttl
   */
  public int getTTL() {
    return ttl;
  }

  /**
   * @return the operation
   */
  public UpdateOperation getOperation() {
    return operation;
  }

  /**
   * @param operation
   */
  public void setOperation(UpdateOperation operation) {
    this.operation = operation;
  }

  //
  // TEST CODE
  //
  public static void main(String[] args) {
    ResultValue x = new ResultValue();
    x.add("12345678");
    //
    UpdatePacket up = new UpdatePacket(GNSNodeConfig.INVALID_NAME_SERVER_ID, 32234234, 123, "12322323",
            "EdgeRecord", x, null, -1, null, UpdateOperation.SINGLE_FIELD_APPEND_WITH_DUPLICATION, null, new NodeId<String>("123"),
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

  public void setRequestID(int requestID) {
    this.requestID = requestID;
  }
}
