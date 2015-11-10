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
import org.json.JSONException;
import org.json.JSONObject;

/**
 * This class implements the packet used to mark the end of a dump request.
 *
 * @author Westy
 *
 */
public class SentinalPacket extends BasicPacket {

  /**
   * id
   */
  public final static String ID = "id";
  private int id;

  /**
   * Constructs new SentinalPacket.
   *
   * @param id
   *
   */
  public SentinalPacket(int id) {
    this.type = Packet.PacketType.SENTINAL;
    this.id = id;
  }

  /**
   * Constructs new SentinalPacket from a JSONObject
   *
   * @param json JSONObject representing this packet
   * @throws org.json.JSONException
   *
   */
  public SentinalPacket(JSONObject json) throws JSONException {
    if (Packet.getPacketType(json) != Packet.PacketType.SENTINAL) {
      Exception e = new Exception("SentinalPacket: wrong packet type " + Packet.getPacketType(json));
      e.printStackTrace();
      return;
    }

    this.id = json.getInt(ID);
    this.type = Packet.getPacketType(json);
  }

  /**
   * Converts a SentinalPacket to a JSONObject.
   *
   * @return JSONObject representing this packet.
   * @throws org.json.JSONException
   *
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    json.put(ID, id);
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
}
