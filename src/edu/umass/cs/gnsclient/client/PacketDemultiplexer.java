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
package edu.umass.cs.gnsclient.client;

import org.json.JSONObject;
import edu.umass.cs.gnsclient.client.tcp.packet.Packet;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import org.json.JSONException;

/**
 * 
 * @author westy
 */
public class PacketDemultiplexer extends AbstractJSONPacketDemultiplexer{
  
  BasicUniversalTcpClient client;

  public PacketDemultiplexer(BasicUniversalTcpClient client) {
    this.client = client;
    this.register(Packet.PacketType.COMMAND);
    this.register(Packet.PacketType.COMMAND_RETURN_VALUE);
  }
  
@Override
  public boolean handleMessage(JSONObject jsonObject) {
    long receivedTime = System.currentTimeMillis();
    try {
      switch (Packet.getPacketType(jsonObject)) {
        case COMMAND:
          break;
        case COMMAND_RETURN_VALUE:
          client.handleCommandValueReturnPacket(jsonObject, receivedTime);
          break;
        default:
          return false;
      }
    } catch (JSONException e) {
      return false;
    }
    return true;
  }
  
}
