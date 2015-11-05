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

/**
 * @author V. Arun
 * @param <NodeIDType>
 */
public class DropEpochFinalState<NodeIDType> extends
		BasicReconfigurationPacket<NodeIDType> {

	private static enum Keys {
		DELETE_NAME
	};

	private final boolean deleteName; // will completely delete the name as
										// opposed to a specific epoch

	/**
	 * @param initiator
	 * @param name
	 * @param epochNumber
	 * @param deleteName
	 */
	public DropEpochFinalState(NodeIDType initiator, String name,
			int epochNumber, boolean deleteName) {
		super(initiator,
				ReconfigurationPacket.PacketType.DROP_EPOCH_FINAL_STATE, name,
				epochNumber);
		this.deleteName = deleteName;
	}

	/**
	 * @param json
	 * @param unstringer
	 * @throws JSONException
	 */
	public DropEpochFinalState(JSONObject json,
			Stringifiable<NodeIDType> unstringer) throws JSONException {
		super(json, unstringer);
		this.deleteName = (json.has(Keys.DELETE_NAME.toString()) ? json
				.getBoolean(Keys.DELETE_NAME.toString()) : false);
	}

	/**
	 * @return Whether this name is being deleted (as opposed to being
	 *         reconfigured).
	 */
	public boolean isDeleteName() {
		return this.deleteName;
	}
}
