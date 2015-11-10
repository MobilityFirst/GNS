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
import edu.umass.cs.nio.interfaces.Stringifiable;

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
public class RemoveRecordPacket<NodeIDType> extends BasicPacketWithNSAndCCP<NodeIDType> implements Request {

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
   * @param unstringer
   * @throws org.json.JSONException
   */
  @SuppressWarnings("unchecked")
  public RemoveRecordPacket(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
    super(json.has(NAMESERVER_ID) ? unstringer.valueOf(json.getString(NAMESERVER_ID)) : null,
            json.optString(CCP_ADDRESS, null), json.optInt(CCP_PORT, INVALID_PORT));
    if (Packet.getPacketType(json) != Packet.PacketType.REMOVE_RECORD 
            //&& Packet.getPacketType(json) != Packet.PacketType.RC_REMOVE
            ) {
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
   * Set the CCP request id.
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
   * Return the source id.
   * 
   * @return the node id
   */
  public NodeIDType getSourceId() {
    return sourceId;
  }

  /**
   * Set the request id.
   * 
   * @param requestID
   */
  
  public void setRequestID(int requestID) {
    this.requestID = requestID;
  }

  // For InterfaceRequest
  @Override
  public String getServiceName() {
    return this.name;
  }
}
