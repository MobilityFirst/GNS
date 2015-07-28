/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.gigapaxos.paxosutil;

import edu.umass.cs.gigapaxos.PaxosManager;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.nio.JSONPacket;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * @author V. Arun
 *         <p>
 *         Used to get NIO to send paxos packets to PaxosManager. This class has
 *         been merged into PaxosManager now and will be soon deprecated.
 */
@SuppressWarnings("javadoc")
public class PaxosPacketDemultiplexer<NodeIDType> extends
		AbstractJSONPacketDemultiplexer {

	// private final PaxosManager<NodeIDType> paxosManager;

	public PaxosPacketDemultiplexer(PaxosManager<NodeIDType> pm) {
		// paxosManager = pm;
		this.register(PaxosPacket.PaxosPacketType.PAXOS_PACKET);
	}

	public boolean handleMessage(JSONObject jsonMsg) {
		boolean isPacketTypeFound = false;
		try {
			PaxosPacket.PaxosPacketType type = PaxosPacket.PaxosPacketType
					.getPaxosPacketType(JSONPacket.getPacketType(jsonMsg));
			if (type == null
					|| !type.equals(PaxosPacket.PaxosPacketType.PAXOS_PACKET))
				return false;
			throw new RuntimeException("This class should no longer be used");
			// paxosManager.handleIncomingPacket(jsonMsg);
			// return true;
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return isPacketTypeFound;
	}
}
