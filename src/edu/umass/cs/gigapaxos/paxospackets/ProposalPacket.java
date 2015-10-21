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
package edu.umass.cs.gigapaxos.paxospackets;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * @author arun
 *
 */
@SuppressWarnings("javadoc")
public class ProposalPacket extends RequestPacket {
	/**
	 * Slot number assigned to the request. A proposal is a (slot, request)
	 * two-tuple.
	 */
	public final int slot;

	public ProposalPacket(int slot, RequestPacket req) {
		super(req);
		this.slot = slot;
		this.packetType = PaxosPacketType.PROPOSAL;
	}

	protected ProposalPacket(ProposalPacket prop) {
		super(prop);
		this.slot = prop.slot;
		this.packetType = PaxosPacketType.PROPOSAL;
	}

	public ProposalPacket(JSONObject json) throws JSONException {
		super(json);
		this.packetType = PaxosPacketType.PROPOSAL;
		this.slot = json.getInt(PaxosPacket.Keys.S.toString());
	}

	public ProposalPacket(net.minidev.json.JSONObject json) throws JSONException {
		super(json);
		this.packetType = PaxosPacketType.PROPOSAL;
		this.slot = (Integer)json.get(PaxosPacket.Keys.S.toString());
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		json.put(PaxosPacket.Keys.S.toString(), slot);
		return json;
	}
	@Override
	public net.minidev.json.JSONObject toJSONSmartImpl() throws JSONException {
		net.minidev.json.JSONObject json = super.toJSONSmartImpl();
		json.put(PaxosPacket.Keys.S.toString(), slot);
		return json;
	}


	@Override
	protected String getSummaryString() {
		return slot + ", " + super.getSummaryString();
	}
}
