/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.gnsApp.packet;

import edu.umass.cs.gigapaxos.InterfaceRequest;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.utils.JSONUtils;
import edu.umass.cs.nio.Stringifiable;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * This packet is sent by a CCP to a name server to add a multiple names to GNS.
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
public class AddBatchRecordPacket<NodeIDType> extends BasicPacketWithNSAndCCP<NodeIDType> implements InterfaceRequest {

  private final static String REQUESTID = "reqID";
  private final static String LNSREQID = "lnreqID";
  private final static String NAMES = "names";
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
  private final JSONArray names;

  /**
   * A map of the values per name that should be set.
   */
  private JSONObject values;

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
   * Constructs a new AddBatchRecordPacket with the given names and values.
   * This constructor does not specify one fields in this packet: <code>CCPRequestID</code>.
   * <code>CCPRequestID</code> can be set by calling <code>setCCPRequestID</code>.
   *
   * We can also change the <code>localNameServerID</code> field in this packet by calling
   * <code>setLocalNameServerID</code>.
   *
   * @param sourceId
   * @param requestID Unique identifier used by the entity making the initial request to confirm
   * @param names
   * @param values
   * @param lnsAddress
   */
  public AddBatchRecordPacket(NodeIDType sourceId, int requestID, Set<String> names, Map<String, JSONObject> values,
          InetSocketAddress lnsAddress) {
    super(null, lnsAddress);
    this.type = Packet.PacketType.ADD_BATCH_RECORD;
    this.sourceId = sourceId != null ? sourceId : null;
    this.requestID = requestID;
    this.names = new JSONArray(names);
    this.values = new JSONObject(values);
  }

  /**
   * Constructs a new AddRecordPacket from a JSONObject
   *
   * @param json JSONObject that represents this packet
   * @param unstringer
   * @throws org.json.JSONException
   */
  public AddBatchRecordPacket(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
    super(json.has(NAMESERVER_ID) ? unstringer.valueOf(json.getString(NAMESERVER_ID)) : null,
            json.optString(CCP_ADDRESS, null), json.optInt(CCP_PORT, INVALID_PORT));
    if (Packet.getPacketType(json) != Packet.PacketType.ADD_BATCH_RECORD) {
      throw new JSONException("AddBatchRecordPacket: wrong packet type " + Packet.getPacketType(json));
    }
    this.type = Packet.getPacketType(json);
    this.sourceId = json.has(SOURCE_ID) ? unstringer.valueOf(json.getString(SOURCE_ID)) : null;
    this.requestID = json.getInt(REQUESTID);
    this.CCPRequestID = json.getInt(LNSREQID);
    this.names = json.getJSONArray(NAMES);
    this.values = json.getJSONObject(VALUES);
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
    json.put(NAMES, names);
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
  public Set<String> getNames() {
    try {
      return JSONUtils.JSONArrayToSetString(names);
    } catch (JSONException e) {
      // we're basically screwed here
      GNS.getLogger().severe("Problem getting names from AddBatchRecordPacket: " + e);
      return null;
    }
  }

  /**
   * Return the values.
   *
   * @return
   */
  public JSONObject getValues() {
    return values;
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
    try {
      return names.getString(0);
    } catch (JSONException e) {
      // we're basically screwed here
      GNS.getLogger().severe("Problem getting service name from AddBatchRecordPacket: " + e);
      return null;
    }

  }

}
