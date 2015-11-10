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
import edu.umass.cs.gnsserver.utils.Format;
import org.json.JSONException;
import org.json.JSONObject;

import java.text.ParseException;
import java.util.Date;

/**
 * This class implements a packet that contains status information
 * 
 * @author Westy
 * @param <NodeIDType>
 */
public class StatusPacket<NodeIDType> extends BasicPacket {

  /** id */
  public final static String ID = "id";

  /** json */
  public final static String JSON = "json";

  /** time */
  public final static String TIME = "time";
  private NodeIDType id;
  private Date time;
  /** JOSNObject where the results are kept **/
  private JSONObject jsonObject;

  /**
   * Constructs a new status packet with the given JSONObject.
   * 
   * @param id
   * @param jsonObject
   */
  public StatusPacket(NodeIDType id, JSONObject jsonObject) {
    this.type = Packet.PacketType.STATUS;
    this.id = id;
    this.time = new Date();
    this.jsonObject = jsonObject;
  }

  /**
   * Constructs a new empty status packet.
   *
   * @param id
   */
  public StatusPacket(NodeIDType id) {
    this(id, new JSONObject());
  }

  /**
   * Constructs new StatusPacket from a JSONObject.
   * 
   * @param json JSONObject representing this packet
   * @throws org.json.JSONException
   * @throws java.text.ParseException
   */
  @SuppressWarnings("unchecked")
  public StatusPacket(JSONObject json) throws JSONException, ParseException {
    if (Packet.getPacketType(json) != Packet.PacketType.STATUS) {
      Exception e = new Exception("StatusPacket: wrong packet type " + Packet.getPacketType(json));
      e.printStackTrace();
      return;
    }

    this.type = Packet.getPacketType(json);
    this.id = (NodeIDType) json.get(ID);
    this.time = Format.parseDateTimeOnlyMilleUTC(json.getString(TIME));
    this.jsonObject = json.getJSONObject(JSON);
  }

  /**
   * Return the id.
   * 
   * @return the id
   */
  public NodeIDType getId() {
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
   * Return the json object.
   * 
   * @return the json object
   */
  public JSONObject getJsonObject() {
    return jsonObject;
  }

  /**
   * Converts a StatusPacket to a JSONObject.
   * @return JSONObject representing this packet.
   * @throws org.json.JSONException
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    json.put(ID, id.toString());
    json.put(TIME, Format.formatDateTimeOnlyMilleUTC(time));
    json.put(JSON, jsonObject);

    return json;
  }
}
