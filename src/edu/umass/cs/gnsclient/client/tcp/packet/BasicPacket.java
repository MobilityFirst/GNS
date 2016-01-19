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
 *  Initial developer(s): Westy, Emmanuel Cecchet
 *
 */
package edu.umass.cs.gnsclient.client.tcp.packet;

import edu.umass.cs.gnsserver.gnsApp.packet.ExtensiblePacketInterface;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import org.json.JSONException;

/**
 * Provides the basics for Packets including a type field.
 *
 * @author westy
 */
public abstract class BasicPacket implements PacketInterface, ExtensiblePacketInterface {

  /**
   * The packet type *
   */
  protected Packet.PacketType type;

  /**
   * @return the type
   */
  public Packet.PacketType getType() {
    return type;
  }
  
  // For InterfaceRequest
  /**
   * Return the request type.
   *
   * @return the request type
   */
  public IntegerPacketType getRequestType() {
    return type;
  }

  /**
   * @param type the type to set
   */
  public void setType(Packet.PacketType type) {
    this.type = type;
  }
  
  @Override
  public String toString() {
    try {
      return this.toJSONObject().toString();
    } catch (JSONException e) {
      GNS.getLogger().severe("Problem converting packet to string:" + e);
      return "BasicPacket{" + "type=" + getType() + '}';
    }
  }

  public static boolean shortenLongToString = false; // shorten is unused right now

  public String toString(boolean shorten) {
    if (shortenLongToString && shorten) {
      return toReasonableString();
    } else {
      return toString();
    }
  }

  public String toReasonableString() {
    try {
      return this.toJSONObject().toReasonableString();
    } catch (JSONException e) {
      GNS.getLogger().severe("Problem converting packet to string:" + e);
      return "BasicPacket{" + "type=" + getType() + '}';
    }
  }


}
