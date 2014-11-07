/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.nsdesign.packet;

import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;
import java.net.InetSocketAddress;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * This packet is sent by a local name server to a name server to remove a name from GNS.
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
 * @param <NodeIDType>
 */
public class RemoveRecordPacket<NodeIDType> extends BasicPacketWithNSAndLNS {

  private final static String REQUESTID = "reqID";
  private final static String LNSREQID = "lnreqID";
  private final static String NAME = "name";
  private final static String SOURCE_ID = "sourceId";
   /**
   * This is the source ID of a packet that should be returned to the intercessor of the LNS.
   * Otherwise the sourceId field contains the number of the NS who made the request.
   */
  public final static String LOCAL_SOURCE_ID = GNSNodeConfig.INVALID_NAME_SERVER_ID;

  /**
   * Unique identifier used by the entity making the initial request to confirm
   */
  private int requestID;

  /**
   * The ID the LNS uses to for bookeeping
   */
  private int LNSRequestID;

  /**
   * Host/domain/device name *
   */
  private final String name;

   /**
   * The originator of this packet, if it is LOCAL_SOURCE_ID (ie, -1) that means go back the Intercessor otherwise
   * it came from another server.
   */
  private final NodeIDType sourceId;


  /**
   * Constructs a new RemoveRecordPacket with the given name and value.
   *
   * @param sourceId the originator of this packet (either a server Id or -1 to indicate The intercessor)
   * @param requestId  Unique identifier used by the entity making the initial request to confirm
   * @param name Host/domain/device name
   * @param lnsAddress
   */
  public RemoveRecordPacket(NodeIDType sourceId, int requestId, String name, InetSocketAddress lnsAddress) {
    super(GNSNodeConfig.INVALID_NAME_SERVER_ID, lnsAddress);
    this.type = Packet.PacketType.REMOVE_RECORD;
    this.sourceId = sourceId;
    this.requestID = requestId;
    this.name = name;
  }

  /**
   * Constructs a new RemoveRecordPacket from a JSONObject
   *
   * @param json JSONObject that represents this packet
   * @throws org.json.JSONException
   */
  public RemoveRecordPacket(JSONObject json) throws JSONException {
    super((NodeIDType) json.get(NAMESERVER_ID),
            json.optString(LNS_ADDRESS, null), json.optInt(LNS_PORT, INVALID_PORT));
    if (Packet.getPacketType(json) != Packet.PacketType.REMOVE_RECORD && Packet.getPacketType(json) != Packet.PacketType.RC_REMOVE) {
      Exception e = new Exception("AddRecordPacket: wrong packet type " + Packet.getPacketType(json));
      e.printStackTrace();
    }
    this.type = Packet.getPacketType(json);
    this.sourceId = (NodeIDType) json.get(SOURCE_ID);
    this.requestID = json.getInt(REQUESTID);
    this.LNSRequestID = json.getInt(LNSREQID);
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
    json.put(LNSREQID, getLNSRequestID());
    json.put(NAME, getName());
    return json;
  }

  public int getRequestID() {
    return requestID;
  }

  public int getLNSRequestID() {
    return LNSRequestID;
  }

  public void setLNSRequestID(int LNSRequestID) {
    this.LNSRequestID = LNSRequestID;
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

  /**
   * This really should be documented.
   */
  public void changePacketTypeToRcRemove() {
    type = Packet.PacketType.RC_REMOVE;
  }

  public void setRequestID(int requestID) {
    this.requestID = requestID;
  }
}
