/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.packet;

import java.text.ParseException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/*************************************************************
 * This class implements a packet that contains a response 
 * to a complex query statement.
 * 
 * @author Westy
 ************************************************************/
public class QueryResponsePacket extends BasicPacket {

  public final static String ID = "id";
  public final static String JSON = "json";

  private int id;
  private JSONArray jsonArray;

  /**
   * Constructs a new QueryResponsePacket with the given JSONArray
   * @param id
   * @param jsonObject 
   */
  public QueryResponsePacket(int id, JSONArray jsonArray) {
    this.type = Packet.PacketType.QUERY_RESPONSE;
    this.id = id;
    this.jsonArray = jsonArray;
  }

  /**
   * Constructs a new empty QueryResponsePacket packet
   * 
   * @param id 
   */
  public QueryResponsePacket(int id) {
    this(id, new JSONArray());
  }

  /*************************************************************
   * Constructs new QueryResponsePacket from a JSONObject
   * @param json JSONObject representing this packet
   * @throws JSONException
   ************************************************************/
  public QueryResponsePacket(JSONObject json) throws JSONException, ParseException {
    if (Packet.getPacketType(json) != Packet.PacketType.QUERY_RESPONSE) {
      Exception e = new Exception("StatusPacket: wrong packet type " + Packet.getPacketType(json));
      e.printStackTrace();
      return;
    }
    this.type = Packet.getPacketType(json);
    this.id = json.getInt(ID);
    this.jsonArray = json.getJSONArray(JSON);
  }

  /*************************************************************
   * Converts a ActiveNSUpdatePacket to a JSONObject.
   * @return JSONObject representing this packet.
   * @throws JSONException
   ************************************************************/
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    json.put(ID, id);
    json.put(JSON, jsonArray);

    return json;
  }
  
  public int getId() {
    return id;
  }
  
   public JSONArray getJsonArray() {
    return jsonArray;
  }
}
