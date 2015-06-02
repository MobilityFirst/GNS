/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.newApp.packet;

import edu.umass.cs.gns.reconfiguration.InterfaceRequest;
import edu.umass.cs.gns.util.JSONUtils;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.util.Stringifiable;
import java.net.InetSocketAddress;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Set;

/**
 * This packet is sent by a local name server to a name server to add a name to GNS.
 *
 * The packet contains request IDs which are used by local name server, and the client (end-user).
 *
 * A client sending this packet sets an initial key/value pair associated with the name, and specifies
 * the TTL to be used for this name via the TTL field in this record.
 * A client must set the <code>requestID</code> field correctly to received a response.
 *
 * Once this packet reaches local name server, local name server sets the
 * <code>localNameServerID</code> and <code>LNSRequestID</code> fields before forwarding packet
 * to name server.
 *
 * When name server replies to the client after adding the record, it uses a different packet type:
 * <code>ConfirmUpdateLNSPacket</code>. But it uses fields in this packet in sending the reply.
 *
 * @param <NodeIDType>
 */
public class AddRecordPacket<NodeIDType> extends BasicPacketWithNSAndCCP<NodeIDType> implements InterfaceRequest {

  private final static String REQUESTID = "reqID";
  private final static String LNSREQID = "lnreqID";
  private final static String NAME = "name";
  private final static String RECORDKEY = "recordkey";
  private final static String VALUE = "value";
  private final static String SOURCE_ID = "sourceId";
  private final static String TIME_TO_LIVE = "ttlAddress";
  private final static String ACTIVE_NAMESERVERS = "actives";

  /**
   * Unique identifier used by the entity making the initial request to confirm
   */
  private int requestID;

  /**
   * The ID the LNS uses to for bookkeeping
   */
  private int LNSRequestID;

  /**
   * Host/domain/device name *
   */
  private final String name;

  /**
   * The key of the value key pair. *
   */
  private final String recordKey;

  /**
   * the value *
   */
  private final ResultValue value;

  /**
   * Time interval (in seconds) that the record may be cached before it should be discarded
   */
  private final int ttl;

//  /**
//   * Id of local nameserver handling this request *
//   */
//  private int localNameServerID;
//  /**
//   * ID of name server receiving the message.
//   */
//  private NodeIDType nameServerID;
  /**
   * The originator of this packet, if it is LOCAL_SOURCE_ID (ie, null) that means go back the Intercessor otherwise
   * it came from another server.
   */
  private final NodeIDType sourceId;

  /**
   * Initial set of active replicas for this name. Used by RC's to inform an active replica of the initial active
   * replica set.
   */
  private Set<NodeIDType> activeNameServers = null;

  /**
   * Constructs a new AddRecordPacket with the given name, value, and TTL.
   * This constructor does not specify one fields in this packet: <code>LNSRequestID</code>.
   * <code>LNSRequestID</code> can be set by calling <code>setLNSRequestID</code>.
   *
   * We can also change the <code>localNameServerID</code> field in this packet by calling
   * <code>setLocalNameServerID</code>.
   *
   * @param sourceId
   * @param requestID Unique identifier used by the entity making the initial request to confirm
   * @param name Host/domain/device name
   * @param recordKey The initial key that will be stored in the name record.
   * @param value The inital value of the key that is specified.
   * @param lnsAddress
   * @param ttl TTL of name record.
   */
  public AddRecordPacket(NodeIDType sourceId, int requestID, String name, String recordKey, ResultValue value, InetSocketAddress lnsAddress, int ttl) {
    super(null, lnsAddress);
    this.type = Packet.PacketType.ADD_RECORD;
    this.sourceId = sourceId != null ? sourceId : null;
    this.requestID = requestID;
    this.recordKey = recordKey;
    this.name = name;
    this.value = value;
    //this.localNameServerID = localNameServerID;
    this.ttl = ttl;
    //this.nameServerID = null;
    this.activeNameServers = null;
  }

  /**
   * Constructs a new AddRecordPacket from a JSONObject
   *
   * @param json JSONObject that represents this packet
   * @throws org.json.JSONException
   */
  public AddRecordPacket(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
    super(json.has(NAMESERVER_ID) ? unstringer.valueOf(json.getString(NAMESERVER_ID)) : null,
            json.optString(CCP_ADDRESS, null), json.optInt(CCP_PORT, INVALID_PORT));
    if (Packet.getPacketType(json) != Packet.PacketType.ADD_RECORD
            && Packet.getPacketType(json) != Packet.PacketType.ACTIVE_ADD
            && Packet.getPacketType(json) != Packet.PacketType.ACTIVE_ADD_CONFIRM) {
      throw new JSONException("AddRecordPacket: wrong packet type " + Packet.getPacketType(json));
    }
    this.type = Packet.getPacketType(json);
    this.sourceId = json.has(SOURCE_ID) ? unstringer.valueOf(json.getString(SOURCE_ID)) : null;
    this.requestID = json.getInt(REQUESTID);
    this.LNSRequestID = json.getInt(LNSREQID);
    this.recordKey = json.getString(RECORDKEY);
    this.name = json.getString(NAME);
    this.value = JSONUtils.JSONArrayToResultValue(json.getJSONArray(VALUE));
    this.ttl = json.getInt(TIME_TO_LIVE);
    this.activeNameServers = json.has(ACTIVE_NAMESERVERS)
            ? unstringer.getValuesFromJSONArray(json.getJSONArray(ACTIVE_NAMESERVERS)) : null;
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
    json.put(LNSREQID, getLNSRequestID());
    json.put(RECORDKEY, getRecordKey());
    json.put(NAME, getName());
    json.put(VALUE, new JSONArray(getValue()));
    json.put(TIME_TO_LIVE, getTTL());
    if (getActiveNameServers() != null) {
      json.put(ACTIVE_NAMESERVERS, getActiveNameServers());
    }
    return json;
  }

  public int getRequestID() {
    return requestID;
  }

  public void setRequestID(int requestID) {
    this.requestID = requestID;
  }

  public int getLNSRequestID() {
    return LNSRequestID;
  }

  /**
   * LNS uses this method to set the ID it will use for bookkeeping about this request.
   *
   * @param LNSRequestID
   */
  public void setLNSRequestID(int LNSRequestID) {
    this.LNSRequestID = LNSRequestID;
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
   * @return the value
   */
  public ResultValue getValue() {
    return value;
  }

  /**
   * @return the ttl
   */
  public int getTTL() {
    return ttl;
  }

  public NodeIDType getSourceId() {
    return sourceId;
  }

  public Set<NodeIDType> getActiveNameServers() {
    return activeNameServers;
  }

  public void setActiveNameServers(Set<NodeIDType> activeNameServers) {
    this.activeNameServers = activeNameServers;
  }

  // For InterfaceRequest
  @Override
  public String getServiceName() {
    return this.name;
  }

}
