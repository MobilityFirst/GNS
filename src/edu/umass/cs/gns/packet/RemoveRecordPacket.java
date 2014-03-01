package edu.umass.cs.gns.packet;

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
   * ***********************************************************
   * Constructs a new RemoveRecordPacket with the given name and value.
   *
   * @param requestID  Unique identifier used by the entity making the initial request to confirm
   * @param name Host/domain/device name
   * @param localNameServerID Id of local nameserver sending this request.
   * **********************************************************
   */
  public RemoveRecordPacket(int requestID, String name, int localNameServerID) {
    this.type = Packet.PacketType.REMOVE_RECORD_LNS;
    this.requestID = requestID;
    this.name = name;
    this.localNameServerID = localNameServerID;
  }

  /**
   * ***********************************************************
   * Constructs a new RemoveRecordPacket from a JSONObject
   *
   * @param json JSONObject that represents this packet
   * @throws JSONException **********************************************************
   */
  public RemoveRecordPacket(JSONObject json) throws JSONException {
    if (Packet.getPacketType(json) != Packet.PacketType.REMOVE_RECORD_LNS
            && Packet.getPacketType(json) != Packet.PacketType.REMOVE_RECORD_NS) {
      Exception e = new Exception("AddRecordPacket: wrong packet type " + Packet.getPacketType(json));
      e.printStackTrace();
    }

    this.type = Packet.getPacketType(json);
    this.requestID = json.getInt(REQUESTID);
    this.LNSRequestID = json.getInt(LNSREQID);
    this.name = json.getString(NAME);
    this.localNameServerID = json.getInt(LOCALNAMESERVERID);
  }

  /**
   * ***********************************************************
   * Converts AddRecordPacket object to a JSONObject
   *
   * @return JSONObject that represents this packet
   * @throws JSONException **********************************************************
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    json.put(REQUESTID, getRequestID());
    json.put(LNSREQID, getLNSRequestID());
    json.put(NAME, getName());
    json.put(LOCALNAMESERVERID, getLocalNameServerID());
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
}
