/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.newApp.packet.admin;

import edu.umass.cs.gns.newApp.packet.BasicPacket;
import edu.umass.cs.gns.newApp.packet.Packet;
import edu.umass.cs.gns.util.Format;
import org.json.JSONException;
import org.json.JSONObject;

import java.text.ParseException;
import java.util.Date;

/**
 * This class implements a packet that contains general
 * response information for admin purposes.
 * 
 * @author Westy
 */
public class AdminResponsePacket extends BasicPacket {

  public final static String ID = "id";
  public final static String JSON = "json";
  public final static String TIME = "time";
  private int id;
  private Date time;
  /** JOSNObject where the results are kept **/
  private JSONObject jsonObject;

  /**
   * Constructs a new AdminResponsePacket with the given JSONObject
   * @param id
   * @param jsonObject
   */
  public AdminResponsePacket(int id, JSONObject jsonObject) {
    this.type = Packet.PacketType.ADMIN_RESPONSE;
    this.id = id;
    this.time = new Date();
    this.jsonObject = jsonObject;
  }

  /**
   * Constructs a new empty status packet
   *
   * @param id
   */
  public AdminResponsePacket(int id) {
    this(id, new JSONObject());
  }

  /**
   * Constructs new AdminResponsePacket from a JSONObject
   * @param json JSONObject representing this packet
   * @throws org.json.JSONException
   */
  public AdminResponsePacket(JSONObject json) throws JSONException, ParseException {
    if (Packet.getPacketType(json) != Packet.PacketType.ADMIN_RESPONSE) {
      Exception e = new Exception("AdminResponsePacket: wrong packet type " + Packet.getPacketType(json));
      e.printStackTrace();
      return;
    }

    this.type = Packet.getPacketType(json);
    this.id = json.getInt(ID);
    this.time = Format.parseDateTimeOnlyMilleUTC(json.getString(TIME));
    this.jsonObject = json.getJSONObject(JSON);
  }

  public int getId() {
    return id;
  }

  public Date getTime() {
    return time;
  }

  public JSONObject getJsonObject() {
    return jsonObject;
  }

  /**
   * Converts a AdminResponsePacket to a JSONObject.
   * @return JSONObject representing this packet.
   * @throws org.json.JSONException
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    json.put(ID, id);
    json.put(TIME, Format.formatDateTimeOnlyMilleUTC(time));
    json.put(JSON, jsonObject);

    return json;
  }
}
