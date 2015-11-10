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
 * This class implements a packet that tells the name server
 * to initial the status system.
 * 
 * @author Westy
 */
public class StatusInitPacket extends BasicPacket {

  /**
   * Constructs a new status init packet
   */
  public StatusInitPacket() {
    this.type = Packet.PacketType.STATUS_INIT;
  }

  /**
   * Constructs new StatusInitPacket from a JSONObject.
   * 
   * @param json JSONObject representing this packet
   * @throws org.json.JSONException
   */
  public StatusInitPacket(JSONObject json) throws JSONException {
    if (Packet.getPacketType(json) != Packet.PacketType.STATUS_INIT) {
      Exception e = new Exception("StatusInitPacket: wrong packet type " + Packet.getPacketType(json));
      e.printStackTrace();
      return;
    }

    this.type = Packet.getPacketType(json);
  }

  /**
   * Converts a StatusInitPacket to a JSONObject.
   * 
   * @return JSONObject representing this packet.
   * @throws org.json.JSONException
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());

    return json;
  }
}
