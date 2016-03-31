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
package edu.umass.cs.gnsserver.gnsapp.packet;

import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.utils.Util;
import java.net.InetSocketAddress;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Provides the basics for Packets including a type field.
 * Includes support for maintaining a clientAddress field
 * using {@link MessageNIOTransport.getSenderAddress}.
 *
 * @author westy
 */
public abstract class BasicPacketWithClientAddress extends BasicPacket {

  private final static String CLIENT_ADDRESS = "clientAddress";

  private InetSocketAddress clientAddress = null;

  /**
   * Creates a BasicPacket.
   */
  public BasicPacketWithClientAddress() {
    // will be filled in when it is next read by receiver after getting sent
    this.clientAddress = null;
  }

  /**
   * Creates a BasicPacket from a JSONObject.
   * 
   * @param json
   * @throws JSONException 
   */
  public BasicPacketWithClientAddress(JSONObject json) throws JSONException {
    this.clientAddress = json.has(CLIENT_ADDRESS)
            ? Util.getInetSocketAddressFromString(json.getString(CLIENT_ADDRESS))
            : MessageNIOTransport.getSenderAddress(json);
  }

  @Override
  public void addToJSONObject(JSONObject json) throws JSONException {
    if (clientAddress != null) {
      json.put(CLIENT_ADDRESS, clientAddress.toString());
    }
  }

  /**
   * Returns the address from which this packet originated.
   * 
   * @return 
   */
  public InetSocketAddress getClientAddress() {
    return clientAddress;
  }

}
