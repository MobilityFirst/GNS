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
public class SelectResponsePacket extends BasicPacket {

  public final static String ID = "id";
  public final static String JSON = "json";
  public final static String LNSQUERYID = "lnsQueryId";
  public final static String NAMESERVER = "ns";
  private int id;
  private int lnsQueryId;
  private int nameServer;
  private JSONArray jsonArray;

  /**
   * Constructs a new QueryResponsePacket
   * @param id
   * @param jsonObject 
   */
  public SelectResponsePacket(int id, int lnsQueryId, int nameServer, JSONArray jsonArray) {
    this.type = Packet.PacketType.QUERY_RESPONSE;
    this.id = id;
    this.lnsQueryId = lnsQueryId;
    this.nameServer = nameServer;
    this.jsonArray = jsonArray;
  }

  /*************************************************************
   * Constructs new QueryResponsePacket from a JSONObject
   * @param json JSONObject representing this packet
   * @throws JSONException
   ************************************************************/
  public SelectResponsePacket(JSONObject json) throws JSONException {
    if (Packet.getPacketType(json) != Packet.PacketType.QUERY_RESPONSE) {
      Exception e = new Exception("StatusPacket: wrong packet type " + Packet.getPacketType(json));
      e.printStackTrace();
      return;
    }
    this.type = Packet.getPacketType(json);
    this.id = json.getInt(ID);
    this.lnsQueryId = json.getInt(LNSQUERYID);
    this.nameServer = json.getInt(NAMESERVER);
    this.jsonArray = json.getJSONArray(JSON);
  }

  /*************************************************************
   * Converts a QueryResponsePacket to a JSONObject.
   * @return JSONObject representing this packet.
   * @throws JSONException
   ************************************************************/
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    json.put(ID, id);
    json.put(LNSQUERYID, lnsQueryId);
    json.put(NAMESERVER, nameServer);
    json.put(JSON, jsonArray);

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
}
