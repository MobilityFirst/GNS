/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.newApp.packet;

import edu.umass.cs.gigapaxos.InterfaceRequest;
import edu.umass.cs.nio.Stringifiable;

import java.net.InetSocketAddress;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * This packet is sent by a local name server to a name server to remove a name from the GNS.
 *
 * A client must set the <code>requestID</code> field correctly to received a response.
 *
 * Once this packet reaches local name server, local name server sets the
 * <code>localNameServerID</code> and <code>CCPRequestID</code> field correctly before forwarding packet
 * to name server.
 *
 * When name server replies to the client, it uses a different packet type: <code>ConfirmUpdateLNSPacket</code>.
 * But it uses fields in this packet in sending the reply.
 *
 * @param <NodeIDType>
 */
public class RemoveRecordPacket<NodeIDType> extends BasicPacketWithNSAndCCP implements InterfaceRequest {

  private final static String REQUESTID = "reqID";
  private final static String CCPREQID = "ccpreqID";
  private final static String NAME = "name";
  private final static String SOURCE_ID = "sourceId";

  /**
   * Unique identifier used by the entity making the initial request to confirm
   */
  private int requestID;

  /**
   * The ID the CCP uses to for bookeeping
   */
  private int CCPRequestID;

  /**
   * Host/domain/device name
   */
  private final String name;

  /**
   * The originator of this packet, if it is LOCAL_SOURCE_ID (ie, null) that means go back 
   * the Intercessor otherwise it came from another server.
   */
  private final NodeIDType sourceId;

  /**
   * Constructs a new RemoveRecordPacket with the given name and value.
   *
   * @param sourceId the originator of this packet (either a server Id or null to indicate The intercessor)
   * @param requestId Unique identifier used by the entity making the initial request to confirm
   * @param name Host/domain/device name
   * @param lnsAddress
   */
  @SuppressWarnings("unchecked")
  public RemoveRecordPacket(NodeIDType sourceId, int requestId, String name, InetSocketAddress lnsAddress) {
    super(null, lnsAddress);
    this.type = Packet.PacketType.REMOVE_RECORD;
    this.sourceId = sourceId != null ? sourceId : null;
    this.requestID = requestId;
    this.name = name;
  }

  /**
   * Constructs a new RemoveRecordPacket from a JSONObject
   *
   * @param json JSONObject that represents this packet
   * @throws org.json.JSONException
   */
  @SuppressWarnings("unchecked")
  public RemoveRecordPacket(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
    super(json.has(NAMESERVER_ID) ? unstringer.valueOf(json.getString(NAMESERVER_ID)) : null,
            json.optString(CCP_ADDRESS, null), json.optInt(CCP_PORT, INVALID_PORT));
    if (Packet.getPacketType(json) != Packet.PacketType.REMOVE_RECORD && Packet.getPacketType(json) != Packet.PacketType.RC_REMOVE) {
       throw new JSONException("AddRecordPacket: wrong packet type " + Packet.getPacketType(json));
    }
    this.type = Packet.getPacketType(json);
    this.sourceId = json.has(SOURCE_ID) ? unstringer.valueOf(json.getString(SOURCE_ID)) : null;
    this.requestID = json.getInt(REQUESTID);
    this.CCPRequestID = json.getInt(CCPREQID);
    this.name = json.getString(NAME);
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
    json.put(CCPREQID, getCCPRequestID());
    json.put(NAME, getName());
    return json;
  }

  public int getRequestID() {
    return requestID;
  }

  public int getCCPRequestID() {
    return CCPRequestID;
  }

  public void setCCPRequestID(int CCPRequestID) {
    this.CCPRequestID = CCPRequestID;
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  public NodeIDType getSourceId() {
    return sourceId;
  }

//  /**
//   * This really should be documented.
//   */
//  public void changePacketTypeToRcRemove() {
//    type = Packet.PacketType.RC_REMOVE;
//  }

  public void setRequestID(int requestID) {
    this.requestID = requestID;
  }

  // For InterfaceRequest
  @Override
  public String getServiceName() {
    return this.name;
  }
}
