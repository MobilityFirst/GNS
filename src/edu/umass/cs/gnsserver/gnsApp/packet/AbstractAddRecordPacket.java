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
 * This packet is sent by a CCP to a name server to add a name to GNS.
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
public abstract class AbstractAddRecordPacket<NodeIDType> extends BasicPacketWithNSAndCCP<NodeIDType> implements Request {

  /** The request id from the client. */
  protected final static String REQUESTID = "reqID";

  /** The id maintained by the LNS. */
  protected final static String LNSREQID = "lnreqID";

  /** The source node. */
  protected final static String SOURCE_ID = "sourceId";

  /**
   * Unique identifier used by the entity making the initial request to confirm
   */
  private int requestID;

  /**
   * The ID the CCP uses to for bookkeeping
   */
  private int CCPRequestID;

  /**
   * The originator of this packet, if it is LOCAL_SOURCE_ID (ie, null) that means go back the Intercessor otherwise
   * it came from another server.
   */
  private final NodeIDType sourceId;

  /**
   * Constructs a new AbstractAddRecordPacket.
   * This constructor does not specify one fields in this packet: <code>CCPRequestID</code>.
   * <code>CCPRequestID</code> can be set by calling <code>setCCPRequestID</code>.
   *
   * We can also change the <code>localNameServerID</code> field in this packet by calling
   * <code>setLocalNameServerID</code>.
   *
   * @param sourceId
   * @param requestID Unique identifier used by the entity making the initial request to confirm
   * @param lnsAddress
   */
  public AbstractAddRecordPacket(NodeIDType sourceId, int requestID, InetSocketAddress lnsAddress) {
    super(null, lnsAddress);
    this.type = Packet.PacketType.ADD_RECORD;
    this.sourceId = sourceId != null ? sourceId : null;
    this.requestID = requestID;
  }

  

  /**
   * Constructs a new AddRecordPacket from a JSONObject
   *
   * @param json JSONObject that represents this packet
   * @param unstringer
   * @throws org.json.JSONException
   */
  public AbstractAddRecordPacket(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
    super(json.has(NAMESERVER_ID) ? unstringer.valueOf(json.getString(NAMESERVER_ID)) : null,
            json.optString(CCP_ADDRESS, null), json.optInt(CCP_PORT, INVALID_PORT));
    this.type = Packet.getPacketType(json);
    this.sourceId = json.has(SOURCE_ID) ? unstringer.valueOf(json.getString(SOURCE_ID)) : null;
    this.requestID = json.getInt(REQUESTID);
    this.CCPRequestID = json.getInt(LNSREQID);
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
   * Returns the source ID.
   * 
   * @return 
   */
  public NodeIDType getSourceId() {
    return sourceId;
  }
  
  

}
