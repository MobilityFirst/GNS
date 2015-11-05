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
package edu.umass.cs.reconfiguration.reconfigurationpackets;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;

/**
@author V. Arun
 * @param <NodeIDType> 
 */
public class AckStartEpoch<NodeIDType> extends BasicReconfigurationPacket<NodeIDType> implements ReplicableRequest{

	private boolean coordType = false;
	/**
	 * @param initiator
	 * @param serviceName
	 * @param epochNumber
	 * @param sender
	 */
	public AckStartEpoch(NodeIDType initiator, String serviceName,
			int epochNumber, NodeIDType sender) {
		super(initiator, ReconfigurationPacket.PacketType.ACK_START_EPOCH, serviceName, epochNumber);
		this.setSender(sender);
	}
	/**
	 * @param json
	 * @param unstringer
	 * @throws JSONException
	 */
	public AckStartEpoch(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
		super(json, unstringer);
	}
	@Override
	public boolean needsCoordination() {
		return coordType;
	}
	@Override
	public void setNeedsCoordination(boolean b) {
		coordType = b;
	}
	@Override
	public IntegerPacketType getRequestType() {
		return this.getType();
	}
}
