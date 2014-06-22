/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.nsdesign.packet;

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
 */
public class RemoveRecordPacket extends BasicPacket {

  private final static String REQUESTID = "reqID";
  private final static String LNSREQID = "lnreqID";
  private final static String NAME = "name";
  private final static String LOCALNAMESERVERID = "local";
  private final static String NAME_SERVER_ID = "nsID";
  private final static String SOURCE_ID = "sourceId";
   /**
   * This is the source ID of a packet that should be returned to the intercessor of the LNS.
   * Otherwise the sourceId field contains the number of the NS who made the request.
   */
  public final static int INTERCESSOR_SOURCE_ID = -1;

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
  private String name;

  /**
   * Id of local nameserver sending this request *
   */
  private int localNameServerID;


  /**
   * Id of name server who received this request from client
   */
  private int nameServerID;
   /**
   * The originator of this packet, if it is LOCAL_SOURCE_ID (ie, -1) that means go back the Intercessor otherwise
   * it came from another server.
   */
  private int sourceId;


  /**
   * Constructs a new RemoveRecordPacket with the given name and value.
   *
   * @param sourceId the originator of this packet (either a server Id or -1 to indicate The intercessor)
   * @param requestId  Unique identifier used by the entity making the initial request to confirm
   * @param name Host/domain/device name
   * @param localNameServerID Id of local nameserver sending this request.
   */
  public RemoveRecordPacket(int sourceId, int requestId, String name, int localNameServerID) {
    this.type = Packet.PacketType.REMOVE_RECORD;
    this.sourceId = sourceId;
    this.requestID = requestId;
    this.name = name;
    this.localNameServerID = localNameServerID;
    this.nameServerID = -1; // this field will be set by name server after it received the packet
  }

  /**
   * Constructs a new RemoveRecordPacket from a JSONObject
   *
   * @param json JSONObject that represents this packet
   * @throws org.json.JSONException
   */
  public RemoveRecordPacket(JSONObject json) throws JSONException {
    if (Packet.getPacketType(json) != Packet.PacketType.REMOVE_RECORD && Packet.getPacketType(json) != Packet.PacketType.RC_REMOVE) {
      Exception e = new Exception("AddRecordPacket: wrong packet type " + Packet.getPacketType(json));
      e.printStackTrace();
    }

    this.type = Packet.getPacketType(json);
    this.sourceId = json.getInt(SOURCE_ID);
    this.requestID = json.getInt(REQUESTID);
    this.LNSRequestID = json.getInt(LNSREQID);
    this.name = json.getString(NAME);
    this.localNameServerID = json.getInt(LOCALNAMESERVERID);
    this.nameServerID = json.getInt(NAME_SERVER_ID);
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
    json.put(SOURCE_ID, getSourceId());
    json.put(REQUESTID, getRequestID());
    json.put(LNSREQID, getLNSRequestID());
    json.put(NAME, getName());
    json.put(LOCALNAMESERVERID, getLocalNameServerID());
    json.put(NAME_SERVER_ID, getNameServerID());
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


  /**
   * @return the primaryNameserverId
   */
  public int getLocalNameServerID() {
    return localNameServerID;
  }


  public void setLocalNameServerID(int localNameServerID) {
    this.localNameServerID = localNameServerID;
  }


  public int getNameServerID() {
    return nameServerID;
  }

  public void setNameServerID(int nameServerID) {
    this.nameServerID = nameServerID;
  }

  public int getSourceId() {
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
