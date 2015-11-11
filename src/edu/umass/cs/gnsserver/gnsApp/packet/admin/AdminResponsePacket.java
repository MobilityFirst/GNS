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
package edu.umass.cs.gnsserver.gnsApp.packet.admin;

import edu.umass.cs.gnsserver.gnsApp.packet.BasicPacket;
import edu.umass.cs.gnsserver.gnsApp.packet.Packet;
import edu.umass.cs.gnscommon.utils.Format;
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

  /** The id */
  public final static String ID = "id";

  /** The JSON */
  public final static String JSON = "json";

  /** The time */
  public final static String TIME = "time";
  private int id;
  private Date time;
  /** JOSNObject where the results are kept **/
  private JSONObject jsonObject;

  /**
   * Constructs a new AdminResponsePacket with the given JSONObject.
   * 
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
   * Constructs a new empty status packet.
   *
   * @param id
   */
  public AdminResponsePacket(int id) {
    this(id, new JSONObject());
  }

  /**
   * Constructs new AdminResponsePacket from a JSONObject.
   * 
   * @param json JSONObject representing this packet
   * @throws org.json.JSONException
   * @throws java.text.ParseException
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

  /**
   * Return the id.
   * 
   * @return the id
   */
  public int getId() {
    return id;
  }

  /**
   * Return the time.
   * 
   * @return the time
   */
  public Date getTime() {
    return time;
  }

  /**
   * Return the json.
   * 
   * @return the json
   */
  public JSONObject getJsonObject() {
    return jsonObject;
  }

  /**
   * Converts a AdminResponsePacket to a JSONObject.
   * 
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
