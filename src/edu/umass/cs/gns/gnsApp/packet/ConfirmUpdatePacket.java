/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.gnsApp.packet;

import edu.umass.cs.gigapaxos.InterfaceRequest;
import edu.umass.cs.gns.gnsApp.NSResponseCode;
import edu.umass.cs.nio.Stringifiable;
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
public class ConfirmUpdatePacket<NodeIDType> extends BasicPacket implements InterfaceRequest {

  private final static String REQUESTID = "reqid";
  private final static String CCPREQUESTID = "ccpreqid";
  private final static String RESPONSECODE = "code";
  private final static String RETURNTO = "returnTo";
  /**
   * Unique identifier used by the entity making the initial request to confirm
   */
  private int requestID;
  /**
   * The ID the CCP uses to for bookkeeping
   */
  private int CCPRequestID;
  /**
   * indicates success or failure of operation
   */
  private NSResponseCode responseCode;
  /**
   * Indicates if this is supposed to go back to an Intercessor at the CCP or another server.
   */
  private NodeIDType returnTo;

  /**
   * Constructs a new ConfirmUpdatePacket<String>with the given parameters.
   *
   * @param type Type of this packet
   * @param requestID Id of local or name server
   */
  public ConfirmUpdatePacket(Packet.PacketType type, NodeIDType returnTo, int requestID, int CCPRequestID, NSResponseCode responseCode) {
    this.type = type;
    this.returnTo = returnTo;
    this.requestID = requestID;
    this.CCPRequestID = CCPRequestID;
    this.responseCode = responseCode;
  }

  /**
   * Given an <code>UpdateAddressPacket</code>, create a <code>ConfirmUpdateLNSPacket</code> indicating that
   * the update is failed.
   *
   * @param updatePacket
   * @param code
   * @return
   */
  @SuppressWarnings("unchecked")
  public static ConfirmUpdatePacket<String> createFailPacket(UpdatePacket<String>updatePacket, NSResponseCode code) {
    assert code != NSResponseCode.NO_ERROR; // that would be stupid
    return new ConfirmUpdatePacket(Packet.PacketType.UPDATE_CONFIRM, updatePacket.getSourceId(),
            updatePacket.getRequestID(), updatePacket.getCCPRequestID(), code);
  }

  /**
   * Given an <code>UpdateAddressPacket</code>, create a <code>ConfirmUpdateLNSPacket</code> indicating that
   * the update is success.
   *
   * @param updatePacket  <code>UpdateAddressPacket</code> received by name server.
   * @return <code>ConfirmUpdateLNSPacket</code> indicating request failure.
   */
  @SuppressWarnings("unchecked")
  public static ConfirmUpdatePacket<String> createSuccessPacket(UpdatePacket<String>updatePacket) {
    return new ConfirmUpdatePacket(Packet.PacketType.UPDATE_CONFIRM, updatePacket.getSourceId(),
            updatePacket.getRequestID(), updatePacket.getCCPRequestID(),
            NSResponseCode.NO_ERROR);
  }

  public ConfirmUpdatePacket(NSResponseCode code, AddRecordPacket<NodeIDType> packet) {
    this(Packet.PacketType.ADD_CONFIRM, packet.getSourceId(),
            packet.getRequestID(), packet.getCCPRequestID(), code);

  }

  public ConfirmUpdatePacket(NSResponseCode code, RemoveRecordPacket<NodeIDType> packet) {
    this(Packet.PacketType.REMOVE_CONFIRM, packet.getSourceId(),
            packet.getRequestID(), packet.getCCPRequestID(), code);
  }

  /**
   * Constructs a new ConfirmUpdatePacket<String>from a JSONObject.
   *
   * @param json JSONObject that represents ConfirmUpdatePacket.
   * @throws org.json.JSONException
   */
  public ConfirmUpdatePacket(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
    this.type = Packet.getPacketType(json);
    this.returnTo = json.has(RETURNTO) ? unstringer.valueOf(json.getString(RETURNTO)) : null;
    this.requestID = json.getInt(REQUESTID);
    this.CCPRequestID = json.getInt(CCPREQUESTID);
    // stored as an int in the JSON to keep the byte counting folks happy
    this.responseCode = NSResponseCode.getResponseCode(json.getInt(RESPONSECODE));
  }

  /**
   * Converts a ConfirmUpdatePacket<String>to a JSONObject
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
    json.put(CCPREQUESTID, CCPRequestID);
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

  public int getCCPRequestID() {
    return CCPRequestID;
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

  @Override
  public String getServiceName() {
    // FIXME:
    return "ConfirmUpdate";
  }
}
