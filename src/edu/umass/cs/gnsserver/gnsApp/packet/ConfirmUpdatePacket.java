/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Abhigyan Sharma, Westy
 *
 */
package edu.umass.cs.gnsserver.gnsApp.packet;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gnsserver.gnsApp.NSResponseCode;
import edu.umass.cs.nio.interfaces.Stringifiable;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * This class implements the packet that confirms update, add and remove transactions. The packet is transmitted from an
 * active name server to a local name server that had originally sent the address update. Also used to send
 * confirmation back to the original client ({@link edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.Intercessor} is one example).
 *
 * Name server sets the <code>requestID</code> and <code>LNSRequestID</code> field based on the original request,
 * which could be update, add and remove. If this fields are not set correctly, request will not be routed back to
 * client.
 *
 * @param <NodeIDType>
 */
public class ConfirmUpdatePacket<NodeIDType> extends BasicPacket implements Request {

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
   * Constructs a new ConfirmUpdatePacket with the given parameters.
   *
   * @param type Type of this packet
   * @param returnTo
   * @param requestID Id of local or name server
   * @param CCPRequestID
   * @param responseCode
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
   * @return a ConfirmUpdatePacket
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

  /**
   * Create a ConfirmUpdatePacket instance.
   * 
   * @param code
   * @param packet
   */
  public ConfirmUpdatePacket(NSResponseCode code, AddRecordPacket<NodeIDType> packet) {
    this(Packet.PacketType.ADD_CONFIRM, packet.getSourceId(),
            packet.getRequestID(), packet.getCCPRequestID(), code);
  }
  
  /**
   * Create a ConfirmUpdatePacket instance.
   * 
   * @param code
   * @param packet
   */
  public ConfirmUpdatePacket(NSResponseCode code, AddBatchRecordPacket<NodeIDType> packet) {
    this(Packet.PacketType.ADD_CONFIRM, packet.getSourceId(),
            packet.getRequestID(), packet.getCCPRequestID(), code);
  }

  /**
   * Create a ConfirmUpdatePacket instance.
   * 
   * @param code
   * @param packet
   */
  public ConfirmUpdatePacket(NSResponseCode code, RemoveRecordPacket<NodeIDType> packet) {
    this(Packet.PacketType.REMOVE_CONFIRM, packet.getSourceId(),
            packet.getRequestID(), packet.getCCPRequestID(), code);
  }

  /**
   * Constructs a new ConfirmUpdatePacket from a JSONObject.
   *
   * @param json JSONObject that represents ConfirmUpdatePacket.
   * @param unstringer
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
    json.put(CCPREQUESTID, CCPRequestID);
    // store it as an int in the JSON to keep the byte counting folks happy
    json.put(RESPONSECODE, responseCode.getCodeValue());

    return json;
  }

  /**
   * Return the return to.
   * 
   * @return the return to
   */
  public NodeIDType getReturnTo() {
    return returnTo;
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
   * Return the CCP request id.
   * 
   * @return the CCP request id
   */
  public int getCCPRequestID() {
    return CCPRequestID;
  }

  /**
   * Return the response code.
   * 
   * @return the response code
   */
  public NSResponseCode getResponseCode() {
    return responseCode;
  }

  /**
   * Return the success value.
   * 
   * @return true or false
   */
  public boolean isSuccess() {
    return responseCode == NSResponseCode.NO_ERROR;
  }

  /**
   * Set the request id.
   * 
   * @param requestID
   */
  public void setRequestID(int requestID) {
    this.requestID = requestID;
  }

  @Override
  public String getServiceName() {
    // FIXME:
    return "ConfirmUpdate";
  }
}
