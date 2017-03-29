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
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsserver.gnsapp.packet;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;

import java.net.InetSocketAddress;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * This class implements a packet that contains a response
 * to a select statement.
 *
 * @author Westy
 */
@SuppressWarnings("deprecation")
public class SelectResponsePacket extends BasicPacketWithReturnAddressAndNsAddress
        implements ClientRequest {

  //
  private final static String ID = "id";
  private final static String RECORDS = "records";
  private final static String GUIDS = "guids";
  private final static String NSQUERYID = "nsQueryId";
  private final static String RESPONSECODE = "code";
  private final static String ERRORSTRING = "error";
  
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

  private long requestId;
  private int nsQueryId;
  private JSONArray records;
  private JSONArray guids;
  private ResponseCode responseCode;
  private String errorMessage;

  /*
   * Constructs a new SelectResponsePacket
   *
   * @param id
   * @param jsonObject
   */
  private SelectResponsePacket(long id, InetSocketAddress clientAddress, int nsQueryId,
          InetSocketAddress nsAddress, JSONArray records, JSONArray guids, ResponseCode responseCode,
          String errorMessage) {
    super(nsAddress, clientAddress);
    this.type = Packet.PacketType.SELECT_RESPONSE;
    this.requestId = id;
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
   * @param nsAddress
   * @param records
   * @return a SelectResponsePacket
   */
  public static SelectResponsePacket makeSuccessPacketForRecordsOnly(
          long id, InetSocketAddress lnsAddress,
          long lnsQueryId,
          int nsQueryId, InetSocketAddress nsAddress, JSONArray records) {
    return new SelectResponsePacket(id, lnsAddress, nsQueryId, nsAddress, records, null,
            ResponseCode.NOERROR, null);
  }

  /**
   * Used by a NameServer to a send response with only a list of guids back to the Local NameServer
   *
   * @param id
   * @param lnsAddress
   * @param nsQueryId
   * @param nsAddress
   * @param guids
   * @return a SelectResponsePacket
   */
  public static SelectResponsePacket makeSuccessPacketForGuidsOnly(long id,
          InetSocketAddress lnsAddress,
          int nsQueryId, InetSocketAddress nsAddress, JSONArray guids) {
    return new SelectResponsePacket(id, lnsAddress, nsQueryId, nsAddress,
            null, guids, ResponseCode.NOERROR, null);
  }

  /**
   * Used by a NameServer to a failure response to a NameServer or Local NameServer
   *
   * @param id
   * @param lnsAddress
   * @param nsQueryId
   * @param nsAddress
   * @param errorMessage
   * @return a SelectResponsePacket
   */
  public static SelectResponsePacket makeFailPacket(long id, InetSocketAddress lnsAddress,
           int nsQueryId, InetSocketAddress nsAddress, String errorMessage) {
    return new SelectResponsePacket(id, lnsAddress, nsQueryId, nsAddress,
            null, null, ResponseCode.ERROR, errorMessage);
  }

  /**
   * Constructs new SelectResponsePacket from a JSONObject
   *
   * @param json JSONObject representing this packet
   * @throws org.json.JSONException
   */
  public SelectResponsePacket(JSONObject json) throws JSONException {
    super(json);
    if (Packet.getPacketType(json) != Packet.PacketType.SELECT_RESPONSE) {
      throw new JSONException("StatusPacket: wrong packet type " + Packet.getPacketType(json));
    }
    this.type = Packet.getPacketType(json);
    this.requestId = json.getLong(ID);
    //this.lnsID = json.getInt(LNSID);
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
    json.put(ID, requestId);
    //json.put(LNSID, lnsID);
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
   * Return the requestId.
   *
   * @return the requestId
   */
  public long getId() {
    return requestId;
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
   * Return the NS query requestId.
   *
   * @return the NS query requestId
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

  /**
   *
   * @return the service name
   */
  @Override
  public String getServiceName() {
    // FIXME:
    return "SelectResponse";
  }

  /**
   *
   * @return the response
   */
  @Override
  public ClientRequest getResponse() {
    return this.response;
  }

  /**
   *
   * @return the id
   */
  @Override
  public long getRequestID() {
    return requestId;
  }
}
