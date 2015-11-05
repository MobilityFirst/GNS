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

import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;

/**
 * @author V. Arun
 * @param <NodeIDType> 
 */
public class StopEpoch<NodeIDType> extends
		BasicReconfigurationPacket<NodeIDType> implements
		ReconfigurableRequest, ReplicableRequest {

	private static enum Keys {
		GET_FINALSTATE
	};

	private final boolean getFinalState;

	/**
	 * @param initiator
	 * @param name
	 * @param epochNumber
	 * @param getFinalState
	 */
	public StopEpoch(NodeIDType initiator, String name, int epochNumber,
			boolean getFinalState) {
		super(initiator, ReconfigurationPacket.PacketType.STOP_EPOCH, name,
				epochNumber);
		this.getFinalState = getFinalState;
	}

	/**
	 * @param initiator
	 * @param name
	 * @param epochNumber
	 */
	public StopEpoch(NodeIDType initiator, String name, int epochNumber) {
		this(initiator, name, epochNumber, false);
	}

	/**
	 * @param json
	 * @param unstringer
	 * @throws JSONException
	 */
	public StopEpoch(JSONObject json, Stringifiable<NodeIDType> unstringer)
			throws JSONException {
		super(json, unstringer);
		this.getFinalState = json.optBoolean(Keys.GET_FINALSTATE.toString());
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.GET_FINALSTATE.toString(), this.getFinalState);
		return json;
	}

	@Override
	public boolean isStop() {
		return true;
	}

	@Override
	public boolean needsCoordination() {
		return true;
	}

	@Override
	public void setNeedsCoordination(boolean b) {
		// do nothing
	}
	
	/**
	 * @return True if the epoch final state should be sent with the
	 *         AckStopEpoch packet.
	 */
	public boolean shouldGetFinalState() {	
		return this.getFinalState;
	}
}
