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

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * This class implements a packet that contains a response
 * to a select statement.
 *
 * @author Westy
 * @param <NodeIDType>
 */
public class SelectResponsePacket<NodeIDType> extends BasicPacketWithNSAndCCP<NodeIDType> implements Request {

  /**
   * The possible response codes for select packets.
   */
  public enum ResponseCode {

    /**
     * NOERROR
     */
    NOERROR,

    /**
     * ERROR
     */
    ERROR
  }
  //
  private final static String ID = "id";
  private final static String RECORDS = "records";
  private final static String GUIDS = "guids";
  private final static String LNSQUERYID = "lnsQueryId";
  private final static String NSQUERYID = "nsQueryId";
  private final static String RESPONSECODE = "code";
  private final static String ERRORSTRING = "error";

  private int id;
  private int lnsQueryId;
  private int nsQueryId;
  private JSONArray records;
  private JSONArray guids;
  private ResponseCode responseCode;
  private String errorMessage;

  /**
   * Constructs a new SelectResponsePacket
   *
   * @param id
   * @param jsonObject
   */
  private SelectResponsePacket(int id, InetSocketAddress lnsAddress, int lnsQueryId, int nsQueryId,
          NodeIDType nameServerID, JSONArray records, JSONArray guids, ResponseCode responseCode,
          String errorMessage) {
    super(nameServerID, lnsAddress);
    this.type = Packet.PacketType.SELECT_RESPONSE;
    this.id = id;
    this.lnsQueryId = lnsQueryId;
    this.nsQueryId = nsQueryId;
    this.records = records;
    this.guids = guids;
    this.responseCode = responseCode;
    this.errorMessage = errorMessage;
  }

  /**
   * Used by a NameServer to a send response with full records back to the collecting NameServer
   *
   * @param id
   * @param lnsAddress
   * @param lnsQueryId
   * @param nsQueryId
   * @param nameServerID
   * @param records
   * @return a SelectResponsePacket
   */
  @SuppressWarnings("unchecked")
  public static SelectResponsePacket makeSuccessPacketForRecordsOnly(int id, InetSocketAddress lnsAddress, int lnsQueryId,
          int nsQueryId, Object nameServerID, JSONArray records) {
    return new SelectResponsePacket(id, lnsAddress, lnsQueryId, nsQueryId, nameServerID, records, null, ResponseCode.NOERROR, null);
  }

  /**
   * Used by a NameServer to a send response with only a list of guids back to the Local NameServer
   *
   * @param id
   * @param lnsAddress
   * @param lnsQueryId
   * @param nsQueryId
   * @param nameServerID
   * @param guids
   * @return a SelectResponsePacket
   */
  @SuppressWarnings("unchecked")
  public static SelectResponsePacket makeSuccessPacketForGuidsOnly(int id, InetSocketAddress lnsAddress, int lnsQueryId,
          int nsQueryId, Object nameServerID, JSONArray guids) {
    return new SelectResponsePacket(id, lnsAddress, lnsQueryId, nsQueryId, nameServerID, null, guids, ResponseCode.NOERROR, null);
  }

  /**
   * Used by a NameServer to a failure response to a NameServer or Local NameServer
   *
   * @param id
   * @param lnsAddress
   * @param lnsQueryId
   * @param nsQueryId
   * @param nameServer
   * @param errorMessage
   * @return a SelectResponsePacket
   */
  @SuppressWarnings("unchecked")
  public static SelectResponsePacket makeFailPacket(int id, InetSocketAddress lnsAddress,
          int lnsQueryId, int nsQueryId, Object nameServer, String errorMessage) {
    return new SelectResponsePacket(id, lnsAddress, lnsQueryId, nsQueryId, nameServer, null, null, ResponseCode.ERROR, errorMessage);
  }

  /**
   * Constructs new SelectResponsePacket from a JSONObject
   *
   * @param json JSONObject representing this packet
   * @param unstringer
   * @throws org.json.JSONException
   */
  public SelectResponsePacket(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
    super(json.has(NAMESERVER_ID) ? unstringer.valueOf(json.getString(NAMESERVER_ID)) : null,
            json.optString(CCP_ADDRESS, null), json.optInt(CCP_PORT, INVALID_PORT));
    if (Packet.getPacketType(json) != Packet.PacketType.SELECT_RESPONSE) {;
      throw new JSONException("StatusPacket: wrong packet type " + Packet.getPacketType(json));
    }
    this.type = Packet.getPacketType(json);
    this.id = json.getInt(ID);
    //this.lnsID = json.getInt(LNSID);
    this.lnsQueryId = json.getInt(LNSQUERYID);
    this.nsQueryId = json.getInt(NSQUERYID);
    //this.nameServer = new NodeIDType(json.getString(NAMESERVER));
    this.responseCode = ResponseCode.valueOf(json.getString(RESPONSECODE));
    // either of these could be null
    this.records = json.optJSONArray(RECORDS);
    this.guids = json.optJSONArray(GUIDS);
    this.errorMessage = json.optString(ERRORSTRING, null);

  }

  /**
   * Converts a SelectResponsePacket to a JSONObject.
   *
   * @return JSONObject representing this packet.
   * @throws org.json.JSONException
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    super.addToJSONObject(json);
    json.put(ID, id);
    //json.put(LNSID, lnsID);
    json.put(LNSQUERYID, lnsQueryId);
    json.put(NSQUERYID, nsQueryId);
    //json.put(NAMESERVER, nameServer.toString());
    json.put(RESPONSECODE, responseCode.name());
    if (records != null) {
      json.put(RECORDS, records);
    }
    if (guids != null) {
      json.put(GUIDS, guids);
    }
    if (errorMessage != null) {
      json.put(ERRORSTRING, errorMessage);
    }
    return json;
  }

  /**
   * Return the id.
   * 
   * @return the id
   */
  public int getId() {
    return id;
  }

  /**
   * Return the records.
   * 
   * @return the records
   */
    public JSONArray getRecords() {
    return records;
  }

  /**
   * Return the guids.
   * 
   * @return the guids
   */
  public JSONArray getGuids() {
    return guids;
  }

  /**
   * Return the LNS query id.
   * 
   * @return the LNS query id
   */
  public int getLnsQueryId() {
    return lnsQueryId;
  }

  /**
   * Return the NS query id.
   * 
   * @return the NS query id
   */
  public int getNsQueryId() {
    return nsQueryId;
  }

  /**
   * Return the response code.
   * 
   * @return the response code
   */
    public ResponseCode getResponseCode() {
    return responseCode;
  }

  /**
   * Return the error message.
   * 
   * @return the error message
   */
  public String getErrorMessage() {
    return errorMessage;
  }

  @Override
  public String getServiceName() {
    // FIXME:
    return "SelectResponse";
  }

}
