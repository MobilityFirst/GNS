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
  private final static String ID = "id";
  private final static String JSON = "json";
  private final static String LNSID = "lnsid";
  private final static String LNSQUERYID = "lnsQueryId";
  private final static String NSQUERYID = "nsQueryId";
  private final static String NAMESERVER = "ns";
  private final static String RESPONSECODE = "code";
  private final static String ERRORSTRING = "error";
  private final static String GUID = "guid"; // for auto group guid this is the guid to be maintained
  
  private int id;
  private int lnsID; // the local name server handling this request
  private int lnsQueryId;
  private int nsQueryId;
  private int nameServer;
  private JSONArray jsonArray;
  private ResponseCode responseCode;
  private String errorMessage;
  private String guid; // the group GUID we are maintaning or null for simple select

  /**
   * Constructs a new SelectResponsePacket
   * @param id
   * @param jsonObject 
   */
  private SelectResponsePacket(int id, int lns, int lnsQueryId, int nsQueryId, int nameServer, JSONArray jsonArray, ResponseCode responseCode,
          String errorMessage, String guid) {
    this.type = Packet.PacketType.SELECT_RESPONSE;
    this.id = id;
    this.lnsID = lns;
    this.lnsQueryId = lnsQueryId;
    this.nsQueryId = nsQueryId;
    this.nameServer = nameServer;
    this.jsonArray = jsonArray;
    this.responseCode = responseCode;
    this.errorMessage = errorMessage;
    this.guid = guid;
  }

  public static SelectResponsePacket makeSuccessPacket(int id, int lns, int lnsQueryId, int nsQueryId, int nameServer, JSONArray jsonArray) {
    return new SelectResponsePacket(id, lns, lnsQueryId, nsQueryId, nameServer, jsonArray, ResponseCode.NOERROR, null, null);
  }

  public static SelectResponsePacket makeFailPacket(int id, int lns, int lnsQueryId, int nsQueryId, int nameServer, String errorMessage) {
    return new SelectResponsePacket(id, lns, lnsQueryId, nsQueryId, nameServer, null, ResponseCode.ERROR, errorMessage, null);
  }
  
  public static SelectResponsePacket makeGuidSuccessResponse(int id, int lns, int lnsQueryId, int nsQueryId, int nameServer, JSONArray jsonArray, String guid) {
    return new SelectResponsePacket(id, lns, lnsQueryId, nsQueryId, nameServer, jsonArray, ResponseCode.NOERROR, null, guid);
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
    this.lnsID = json.getInt(LNSID);
    this.lnsQueryId = json.getInt(LNSQUERYID);
    this.nsQueryId = json.getInt(NSQUERYID);
    this.nameServer = json.getInt(NAMESERVER); 
    this.responseCode = ResponseCode.valueOf(json.getString(RESPONSECODE));
    // either of these could be null
    this.jsonArray = json.optJSONArray(JSON);
    this.errorMessage = json.optString(ERRORSTRING, null);
    this.guid = json.optString(GUID, null);
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
    json.put(LNSID, lnsID);
    json.put(LNSQUERYID, lnsQueryId);
    json.put(NSQUERYID, nsQueryId);
    json.put(NAMESERVER, nameServer);
    json.put(RESPONSECODE, responseCode.name());
    if (jsonArray != null) {
      json.put(JSON, jsonArray);
    }
    if (errorMessage != null) {
      json.put(ERRORSTRING, errorMessage);
    }
    if (guid != null) {
      json.put(GUID, guid);
    }
    return json;
  }

  public int getId() {
    return id;
  }

  public int getLnsID() {
    return lnsID;
  }

  public JSONArray getJsonArray() {
    return jsonArray;
  }

  public int getLnsQueryId() {
    return lnsQueryId;
  }

  public int getNsQueryId() {
    return nsQueryId;
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

  public String getGuid() {
    return guid;
  }
  
}
