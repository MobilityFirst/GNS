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

import java.util.logging.Level;

import org.json.JSONObject;

import edu.umass.cs.gnsserver.gnsApp.packet.Packet;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ActiveReplicaError;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;

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
    this.register(ReconfigurationPacket.PacketType.ACTIVE_REPLICA_ERROR);
  }
  
  public String toString() {
	  return this.getClass().getSimpleName();
  }
  
	@Override
	public boolean handleMessage(JSONObject jsonObject) {
		GNS.getLogger().log(Level.FINE, "{0} received {1}", new Object[]{this, jsonObject});
		long receivedTime = System.currentTimeMillis();
		try {
			Packet.PacketType type = Packet.getPacketType(jsonObject);
			if (type != null) {
				switch (type) {
				case COMMAND:
					break;
				case COMMAND_RETURN_VALUE:
					client.handleCommandValueReturnPacket(jsonObject,
							receivedTime);
					break;
				default:
					return false;
				}
			}
			else if(ReconfigurationPacket.getReconfigurationPacketType(jsonObject)==ReconfigurationPacket.PacketType.ACTIVE_REPLICA_ERROR) {
				client.handleCommandValueReturnPacket(new ActiveReplicaError(jsonObject), receivedTime);
			}
			else assert(false);
		} catch (JSONException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
  
}
