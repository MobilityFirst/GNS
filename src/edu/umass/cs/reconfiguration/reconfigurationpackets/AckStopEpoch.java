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

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Stringifiable;

/**
 * @author V. Arun
 * @param <NodeIDType> 
 */
public class AckStopEpoch<NodeIDType> extends
		BasicReconfigurationPacket<NodeIDType> implements Request {

	private static enum Keys {
		FINAL_STATE
	}

	private final String finalState;

	/**
	 * @param sender
	 * @param stopEpoch
	 * @param finalState
	 */
	public AckStopEpoch(NodeIDType sender, StopEpoch<NodeIDType> stopEpoch,
			String finalState) {
		super(stopEpoch.getInitiator(),
				ReconfigurationPacket.PacketType.ACK_STOP_EPOCH, stopEpoch
						.getServiceName(), stopEpoch.getEpochNumber());
		this.setKey(stopEpoch.getKey());
		this.setSender(sender);
		this.finalState = finalState;
	}

	/**
	 * @param sender
	 * @param stopEpoch
	 */
	public AckStopEpoch(NodeIDType sender, StopEpoch<NodeIDType> stopEpoch) {
		this(sender, stopEpoch, null);
	}

	/**
	 * @param json
	 * @param unstringer
	 * @throws JSONException
	 */
	public AckStopEpoch(JSONObject json, Stringifiable<NodeIDType> unstringer)
			throws JSONException {
		super(json, unstringer);
		this.finalState = json.has(Keys.FINAL_STATE.toString()) ? json.getString(Keys.FINAL_STATE.toString()) : null;
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.FINAL_STATE.toString(), this.finalState);
		return json;
	}

	@Override
	public IntegerPacketType getRequestType() {
		return this.getType();
	}
	
	/**
	 * @return Final state of the epoch whose stoppage is being acknowledged.
	 */
	public String getFinalState() {
		return this.finalState;
	}
}
