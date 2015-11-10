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
package edu.umass.cs.gnsserver.gnsApp.packet;

import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * This class implements a packet that contains no information.
 * 
 * @author Westy
 */
public class NoopPacket extends BasicPacket implements ReconfigurableRequest {

  /**
   * Constructs a new NoopPacket.
   */
  public NoopPacket() {
    this.type = Packet.PacketType.NOOP;
  }

  /**
   * Constructs new StatusPacket from a JSONObject
   * @param json JSONObject representing this packet
   * @throws org.json.JSONException
   */
  public NoopPacket(JSONObject json) throws JSONException {
    if (Packet.getPacketType(json) != Packet.PacketType.NOOP) {
      throw new JSONException("NOOP: wrong packet type " + Packet.getPacketType(json));
    }
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
    return json;
  }

  @Override
  public int getEpochNumber() {
    return -1;
  }

  @Override
  public boolean isStop() {
    return true;
  }

  @Override
  public String getServiceName() {
    return "Noop";
  }
}
