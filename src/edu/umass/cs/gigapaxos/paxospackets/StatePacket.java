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

import edu.umass.cs.gigapaxos.paxosutil.Ballot;
import edu.umass.cs.gigapaxos.paxosutil.SlotBallotState;
import edu.umass.cs.utils.Util;

import org.json.JSONException;
import org.json.JSONObject;


/**
 * @author arun
 *
 */
@SuppressWarnings("javadoc")
public final class StatePacket extends PaxosPacket{

	/**
	 * Ballot in which the request at the checkpointed slot was committed.
	 */
	public final Ballot ballot;
	/**
	 * Slot number of the request immediately after executing which the checkpoint was taken.
	 */
	public final int slotNumber;
	/**
	 * The checkpoint state encoded as a string.
	 */
	public final String state;
	/*
	 * Whether the checkpoint is large. If so, checkpoint/restore or remote
	 * checkpoint transfers will use the file system instead of memory.
	 */
	public final boolean isLargeCheckpoint;

	public StatePacket(Ballot b, int slotNumber, String state) {
		this(b, slotNumber, state, false);
	}
	private StatePacket(Ballot b, int slotNumber, String state, boolean isLargeCheckpoint) {
		super((PaxosPacket)null);
		this.ballot = b;
		this.slotNumber = slotNumber;
		this.state = state;
		this.packetType = PaxosPacketType.CHECKPOINT_STATE;
		this.isLargeCheckpoint = isLargeCheckpoint;		
	}

	public StatePacket(JSONObject json) throws JSONException{
		super(json);
		assert(PaxosPacket.getPaxosPacketType(json)==PaxosPacketType.CHECKPOINT_STATE); 
		this.packetType = PaxosPacketType.CHECKPOINT_STATE;
		this.slotNumber = json.getInt(PaxosPacket.Keys.S.toString());
		this.ballot = new Ballot(json.getString(PaxosPacket.NodeIDKeys.B.toString()));
		this.state = json.getString(PaxosPacket.Keys.STATE.toString());
		this.isLargeCheckpoint = json.optBoolean(PaxosPacket.Keys.BIG_CP.toString());
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(PaxosPacket.Keys.S.toString(), this.slotNumber);
		json.put(PaxosPacket.NodeIDKeys.B.toString(), this.ballot.ballotNumber+":"+this.ballot.coordinatorID);
		json.put(PaxosPacket.Keys.STATE.toString(), this.state);
		json.put(PaxosPacket.Keys.BIG_CP.toString(), this.isLargeCheckpoint);
		return json;
	}
	
	public static StatePacket getStatePacket(SlotBallotState sbs) {
		return new StatePacket(new Ballot(sbs.ballotnum, sbs.coordinator), sbs.slot, sbs.state);
	}

	@Override
	protected String getSummaryString() {
		return ballot + ", " + slotNumber + ", ["
				+ Util.truncate(state, 16, 16) + "]";
	}
}
