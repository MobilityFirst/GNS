/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.nsdesign.packet;

//import edu.umass.cs.gns.packet.Packet.PacketType;
import edu.umass.cs.gns.util.NSResponseCode;
import edu.umass.cs.gns.util.Stringifiable;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * This class implements the packet that confirms update, add and remove transactions. The packet is transmitted from an
 * active name server to a local name server that had originally sent the address update. Also used to send
 * confirmation back to the original client ({@link edu.umass.cs.gns.clientsupport.Intercessor} is one example).
 *
 * Name server sets the <code>requestID</code> and <code>LNSRequestID</code> field based on the original request,
 * which could be update, add and remove. If this fields are not set correctly, request will not be routed back to
 * client.
 *
 * @param <NodeIDType>
 */
public class ConfirmUpdatePacket<NodeIDType> extends BasicPacket {

  private final static String REQUESTID = "reqid";
  private final static String LNSREQUESTID = "lnsreqid";
  private final static String RESPONSECODE = "code";
  private final static String RETURNTO = "returnTo";
  /**
   * Unique identifier used by the entity making the initial request to confirm
   */
  private int requestID;
  /**
   * The ID the LNS uses to for bookkeeping
   */
  private int LNSRequestID;
  /**
   * indicates success or failure of operation
   */
  private NSResponseCode responseCode;
  /**
   * Indicates if this is supposed to go back to an Intercessor at the LNS or another server.
   */
  private NodeIDType returnTo;

  /**
   * Constructs a new ConfirmUpdatePacket with the given parameters.
   *
   * @param type Type of this packet
   * @param requestID Id of local or name server
   */
  public ConfirmUpdatePacket(Packet.PacketType type, NodeIDType returnTo, int requestID, int LNSRequestID, NSResponseCode responseCode) {
    this.type = type;
    this.returnTo = returnTo;
    this.requestID = requestID;
    this.LNSRequestID = LNSRequestID;
    this.responseCode = responseCode;
  }

  /**
   * Given an <code>UpdateAddressPacket</code>, create a <code>ConfirmUpdateLNSPacket</code> indicating that
   * the update is failed.
   *
   * @param updatePacket
   * @return
   */
  public static ConfirmUpdatePacket createFailPacket(UpdatePacket updatePacket, NSResponseCode code) {
    assert code != NSResponseCode.NO_ERROR; // that would be stupid
    return new ConfirmUpdatePacket(Packet.PacketType.CONFIRM_UPDATE, updatePacket.getSourceId(),
            updatePacket.getRequestID(), updatePacket.getLNSRequestID(), code);
  }

  /**
   * Given an <code>UpdateAddressPacket</code>, create a <code>ConfirmUpdateLNSPacket</code> indicating that
   * the update is success.
   *
   * @param updatePacket  <code>UpdateAddressPacket</code> received by name server.
   * @return <code>ConfirmUpdateLNSPacket</code> indicating request failure.
   */
  public static ConfirmUpdatePacket createSuccessPacket(UpdatePacket updatePacket) {
    return new ConfirmUpdatePacket(Packet.PacketType.CONFIRM_UPDATE, updatePacket.getSourceId(),
            updatePacket.getRequestID(), updatePacket.getLNSRequestID(),
            NSResponseCode.NO_ERROR);
  }

  public ConfirmUpdatePacket(NSResponseCode code, AddRecordPacket<NodeIDType> packet) {
    this(Packet.PacketType.CONFIRM_ADD, packet.getSourceId(),
            packet.getRequestID(), packet.getLNSRequestID(), code);

  }

  public ConfirmUpdatePacket(NSResponseCode code, RemoveRecordPacket<NodeIDType> packet) {
    this(Packet.PacketType.CONFIRM_REMOVE, packet.getSourceId(),
            packet.getRequestID(), packet.getLNSRequestID(), code);
  }

  /**
   * Constructs a new ConfirmUpdatePacket from a JSONObject.
   *
   * @param json JSONObject that represents ConfirmUpdatePacket.
   * @throws org.json.JSONException
   */
  public ConfirmUpdatePacket(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
    this.type = Packet.getPacketType(json);
    this.returnTo = json.has(RETURNTO) ? unstringer.valueOf(json.getString(RETURNTO)) : null;
    this.requestID = json.getInt(REQUESTID);
    this.LNSRequestID = json.getInt(LNSREQUESTID);
    // stored as an int in the JSON to keep the byte counting folks happy
    this.responseCode = NSResponseCode.getResponseCode(json.getInt(RESPONSECODE));
  }

  /**
   * Converts a ConfirmUpdatePacket to a JSONObject
   *
   * @return JSONObject that represents ConfirmUpdatePacket
   * @throws org.json.JSONException
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    if (returnTo != null) {
      json.put(RETURNTO, returnTo);
    }
    json.put(REQUESTID, requestID);
    json.put(LNSREQUESTID, LNSRequestID);
    // store it as an int in the JSON to keep the byte counting folks happy
    json.put(RESPONSECODE, responseCode.getCodeValue());

    return json;
  }

  public NodeIDType getReturnTo() {
    return returnTo;
  }

  public int getRequestID() {
    return requestID;
  }

  public int getLNSRequestID() {
    return LNSRequestID;
  }

  public NSResponseCode getResponseCode() {
    return responseCode;
  }

  public boolean isSuccess() {
    return responseCode == NSResponseCode.NO_ERROR;
  }

  public void setRequestID(int requestID) {
    this.requestID = requestID;
  }
}
