/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.packet;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/*************************************************************
 * This class implements a packet that contains a response 
 * to a select statement.
 * 
 * @author Westy
 ************************************************************/
public class SelectResponsePacket extends BasicPacket {

  public enum ResponseCode {

    NOERROR,
    ERROR
  }
  //
  public final static String ID = "id";
  public final static String JSON = "json";
  public final static String LNSQUERYID = "lnsQueryId";
  public final static String NAMESERVER = "ns";
  public final static String RESPONSECODE = "code";
  public final static String ERRORSTRING = "error";
  
  private int id;
  private int lnsQueryId;
  private int nameServer;
  private JSONArray jsonArray;
  private ResponseCode responseCode;
  private String errorMessage;

  /**
   * Constructs a new QueryResponsePacket
   * @param id
   * @param jsonObject 
   */
  private SelectResponsePacket(int id, int lnsQueryId, int nameServer, JSONArray jsonArray, ResponseCode responseCode, String errorMessage) {
    this.type = Packet.PacketType.SELECT_RESPONSE;
    this.id = id;
    this.lnsQueryId = lnsQueryId;
    this.nameServer = nameServer;
    this.jsonArray = jsonArray;
    this.responseCode = responseCode;
    this.errorMessage = errorMessage;
  }

  public static SelectResponsePacket makeSuccessPacket(int id, int lnsQueryId, int nameServer, JSONArray jsonArray) {
    return new SelectResponsePacket(id, lnsQueryId, nameServer, jsonArray, ResponseCode.NOERROR, null);
  }

  public static SelectResponsePacket makeFailPacket(int id, int lnsQueryId, int nameServer, String errorMessage) {
    return new SelectResponsePacket(id, lnsQueryId, nameServer, null, ResponseCode.ERROR, errorMessage);
  }

  /**
   * Constructs new SelectResponsePacket from a JSONObject
   * @param json JSONObject representing this packet
   * @throws JSONException
   */
  public SelectResponsePacket(JSONObject json) throws JSONException {
    if (Packet.getPacketType(json) != Packet.PacketType.SELECT_RESPONSE) {
      Exception e = new Exception("StatusPacket: wrong packet type " + Packet.getPacketType(json));
      e.printStackTrace();
      return;
    }
    this.type = Packet.getPacketType(json);
    this.id = json.getInt(ID);
    this.lnsQueryId = json.getInt(LNSQUERYID);
    this.nameServer = json.getInt(NAMESERVER); 
    this.responseCode = ResponseCode.valueOf(json.getString(RESPONSECODE));
    // either of these could be null
    this.jsonArray = json.optJSONArray(JSON);
    this.errorMessage = json.optString(ERRORSTRING, null);
  }

  /**
   * Converts a SelectResponsePacket to a JSONObject.
   * 
   * @return JSONObject representing this packet.
   * @throws JSONException
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    json.put(ID, id);
    json.put(LNSQUERYID, lnsQueryId);
    json.put(NAMESERVER, nameServer);
    json.put(RESPONSECODE, responseCode.name());
    if (jsonArray != null) {
      json.put(JSON, jsonArray);
    }
    if (errorMessage != null) {
      json.put(ERRORSTRING, errorMessage);
    }

    return json;
  }

  public int getId() {
    return id;
  }

  public JSONArray getJsonArray() {
    return jsonArray;
  }

  public int getLnsQueryId() {
    return lnsQueryId;
  }

  public int getNameServer() {
    return nameServer;
  }

  public ResponseCode getResponseCode() {
    return responseCode;
  }

  public String getErrorMessage() {
    return errorMessage;
  }
  
}
