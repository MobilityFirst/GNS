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

import edu.umass.cs.gigapaxos.paxosutil.Ballot;

/**
 * @author arun
 *
 */
@SuppressWarnings("javadoc")
public final class PreparePacket extends PaxosPacket {

	/**
	 * Ballot in the PREPARE message.
	 */
	public final Ballot ballot;
	/**
	 * First undecided slot at coordinator. Only used as an optimization, not
	 * for safety.
	 */
	public final int firstUndecidedSlot;

	// non-final because AbstractPaxosLogger.rollForward needs to set it
	private boolean recovery;

	public PreparePacket(Ballot b) {
		this(b, -1);
	}

	public PreparePacket(Ballot b, int firstUndecidedSlot) {
		super((PaxosPacket) null);
		this.ballot = b;
		this.packetType = PaxosPacketType.PREPARE;
		this.firstUndecidedSlot = firstUndecidedSlot;
		this.recovery = false;
	}

	public PreparePacket(int coordinatorID, int receiverID, Ballot ballot,
			int slotNumber) {
		super((PaxosPacket) null);
		this.ballot = ballot;
		this.firstUndecidedSlot = slotNumber;
		this.packetType = PaxosPacketType.PREPARE;
		this.recovery = false;
	}

	public PreparePacket(JSONObject json) throws JSONException {
		super(json);
		assert (PaxosPacket.getPaxosPacketType(json) == PaxosPacketType.PREPARE);
		this.packetType = PaxosPacket.getPaxosPacketType(json);
		this.ballot = new Ballot(json.getString(PaxosPacket.NodeIDKeys.B
				.toString()));
		this.firstUndecidedSlot = json
				.getInt(PaxosPacket.Keys.PREP_MIN.toString());
		this.recovery = json.optBoolean(PaxosPacket.Keys.RCVRY.toString());
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(PaxosPacket.NodeIDKeys.B.toString(), ballot.toString());
		json.put(PaxosPacket.Keys.PREP_MIN.toString(),
				firstUndecidedSlot);
		if(this.recovery) json.put(PaxosPacket.Keys.RCVRY.toString(), recovery);

		return json;
	}

	public boolean isRecovery() {
		return this.recovery;
	}

	public void setRecovery() {
		this.recovery = true;
	}

	@Override
	protected String getSummaryString() {
		return ballot + "(" +this.firstUndecidedSlot+")";
	}
}