/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.gnsApp.packet;

import edu.umass.cs.gigapaxos.InterfaceRequest;
import edu.umass.cs.gns.util.JSONUtils;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.nio.Stringifiable;
import java.net.InetSocketAddress;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * This packet is sent by a CCP to a name server to add a name to GNS.
 *
 * The packet contains request IDs which are used by local name server, and the client (end-user).
 *
 * A client sending this packet sets an initial key/fieldValue pair associated with the name or
 * the entire JSONObject associated with the name.
 *
 * A client must set the <code>requestID</code> field correctly to received a response.
 *
 * Once this packet reaches CCP, local name server sets the
 * <code>localNameServerID</code> and <code>CCPRequestID</code> fields before forwarding packet
 * to name server.
 *
 * When name server replies to the client after adding the record, it uses a different packet type:
 * <code>ConfirmUpdatePacket</code>. But it uses fields in this packet in sending the reply.
 *
 * @param <NodeIDType>
 */
public class AddRecordPacket<NodeIDType> extends BasicPacketWithNSAndCCP<NodeIDType> implements InterfaceRequest {

  private final static String REQUESTID = "reqID";
  private final static String LNSREQID = "lnreqID";
  private final static String NAME = "name";
  private final static String FIELD = "field";
  private final static String FIELDVALUE = "fieldValue";
  private final static String SOURCE_ID = "sourceId";
  private final static String VALUES = "values";
  //private final static String TIME_TO_LIVE = "ttlAddress";
  //private final static String ACTIVE_NAMESERVERS = "actives";

  /**
   * Unique identifier used by the entity making the initial request to confirm
   */
  private int requestID;

  /**
   * The ID the CCP uses to for bookkeeping
   */
  private int CCPRequestID;

  /**
   * Host/domain/device name *
   */
  private final String name;

  /**
   * The key of the fieldValue key pair. *
   */
  private final String field;

  /**
   * the fieldValue *
   */
  private final ResultValue fieldValue;

  private final JSONObject values;

//  /**
//   * Time interval (in seconds) that the record may be cached before it should be discarded
//   */
//  private final int ttl;
  /**
   * The originator of this packet, if it is LOCAL_SOURCE_ID (ie, null) that means go back the Intercessor otherwise
   * it came from another server.
   */
  private final NodeIDType sourceId;

//  /**
//   * Initial set of active replicas for this name. Used by RC's to inform an active replica of the initial active
//   * replica set.
//   */
//  private Set<NodeIDType> activeNameServers = null;
  /**
   * Constructs a new AddRecordPacket with the given name, fieldValue, and TTL.
   * This constructor does not specify one fields in this packet: <code>CCPRequestID</code>.
   * <code>CCPRequestID</code> can be set by calling <code>setCCPRequestID</code>.
   *
   * We can also change the <code>localNameServerID</code> field in this packet by calling
   * <code>setLocalNameServerID</code>.
   *
   * @param sourceId
   * @param requestID Unique identifier used by the entity making the initial request to confirm
   * @param name Host/domain/device name
   * @param field The initial key that will be stored in the name record.
   * @param fieldValue The inital fieldValue of the key that is specified.
   * @param lnsAddress
   */
  public AddRecordPacket(NodeIDType sourceId, int requestID, String name, String field,
          ResultValue fieldValue, InetSocketAddress lnsAddress) {
    super(null, lnsAddress);
    this.type = Packet.PacketType.ADD_RECORD;
    this.sourceId = sourceId != null ? sourceId : null;
    this.requestID = requestID;
    this.name = name;
    this.values = null;
    this.field = field;
    this.fieldValue = fieldValue;
  }

  /**
   * Constructs a new AddRecordPacket with the given name and values.
   *
   * @param sourceId
   * @param requestID
   * @param name
   * @param values
   * @param lnsAddress
   */
  public AddRecordPacket(NodeIDType sourceId, int requestID, String name,
          JSONObject values, InetSocketAddress lnsAddress) {
    super(null, lnsAddress);
    this.type = Packet.PacketType.ADD_RECORD;
    this.sourceId = sourceId != null ? sourceId : null;
    this.requestID = requestID;
    this.name = name;
    this.values = values;
    this.field = null;
    this.fieldValue = null;
  }

  /**
   * Constructs a new AddRecordPacket from a JSONObject
   *
   * @param json JSONObject that represents this packet
   * @param unstringer
   * @throws org.json.JSONException
   */
  public AddRecordPacket(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
    super(json.has(NAMESERVER_ID) ? unstringer.valueOf(json.getString(NAMESERVER_ID)) : null,
            json.optString(CCP_ADDRESS, null), json.optInt(CCP_PORT, INVALID_PORT));
    if (Packet.getPacketType(json) != Packet.PacketType.ADD_RECORD //&& Packet.getPacketType(json) != Packet.PacketType.ACTIVE_ADD
            //&& Packet.getPacketType(json) != Packet.PacketType.ACTIVE_ADD_CONFIRM
            ) {
      throw new JSONException("AddRecordPacket: wrong packet type " + Packet.getPacketType(json));
    }
    this.type = Packet.getPacketType(json);
    this.sourceId = json.has(SOURCE_ID) ? unstringer.valueOf(json.getString(SOURCE_ID)) : null;
    this.requestID = json.getInt(REQUESTID);
    this.CCPRequestID = json.getInt(LNSREQID);
    this.name = json.getString(NAME);
    this.field = json.optString(FIELD, null);
    if (json.has(FIELDVALUE)) {
      this.fieldValue = JSONUtils.JSONArrayToResultValue(json.getJSONArray(FIELDVALUE));
    } else {
      this.fieldValue = null;
    }
    this.values = json.optJSONObject(VALUES);
  }

  /**
   * Converts AddRecordPacket object to a JSONObject
   *
   * @return JSONObject that represents this packet
   * @throws org.json.JSONException
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    super.addToJSONObject(json);
    json.put(SOURCE_ID, sourceId);
    json.put(REQUESTID, getRequestID());
    json.put(LNSREQID, getCCPRequestID());
    json.put(NAME, getName());
    if (field != null) {
      json.put(FIELD, field);
    }
    if (fieldValue != null) {
      json.put(FIELDVALUE, new JSONArray(fieldValue));
    }
    if (values != null) {
      json.put(VALUES, values);
    }
    return json;
  }

  /**
   * Return the request id.
   * 
   * @return the request id
   */
  public int getRequestID() {
    return requestID;
  }

  /**
   * Set the request id.
   * 
   * @param requestID
   */
  public void setRequestID(int requestID) {
    this.requestID = requestID;
  }

  /**
   * Return the CCP request id.
   * 
   * @return the CCP request id
   */
  public int getCCPRequestID() {
    return CCPRequestID;
  }

  /**
   * LNS uses this method to set the ID it will use for bookkeeping about this request.
   *
   * @param CCPRequestID
   */
  public void setCCPRequestID(int CCPRequestID) {
    this.CCPRequestID = CCPRequestID;
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
   * Return the field.
   * 
   * @return the field
   */
  public String getField() {
    return field;
  }

  /**
   * Return the field value.
   * 
   * @return the fieldValue
   */
  public ResultValue getFieldValue() {
    return fieldValue;
  }
  
  /**
   * Return the source id.
   * 
   * @return
   */
  public NodeIDType getSourceId() {
    return sourceId;
  }

  // For InterfaceRequest
  @Override
  public String getServiceName() {
    return this.name;
  }

  /**
   * Return the values.
   * 
   * @return
   */
  public JSONObject getValues() {
    return values;
  }

}
