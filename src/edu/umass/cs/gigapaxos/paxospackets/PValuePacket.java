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
 * A <slot, ballot, request> three-tuple. The slot and request are within the
 * proposal here. A pvalue is used internally by paxos acceptors and
 * coordinators. It also acts as a DECISION packet as every committed request
 * must be associated with some slot, and ballot.
 */

@SuppressWarnings("javadoc")
public class PValuePacket extends ProposalPacket {

	/**
	 * A ballot is a {@code <ballotNumber:ballotCoordinator>} two-tuple. A
	 * PValuePacket is a <ballot, proposal> two-tuple, or equivalently a
	 * <ballot, slot, request> three-tuple, or equivalently a <ballotNumber:
	 * balltoCoordinator, slot, request> four-tuple.
	 */
	public final Ballot ballot;

	/**
	 * Whether this is a decision being replayed under recovery.
	 */
	private boolean recovery;
	/**
	 * For garbage collection, similar to that in AcceptPacket. This is the slot
	 * up to which the median node (or above) has checkpointed when nodes in the
	 * paxos replica group are ordered by their macCheckpointedSlot.
	 */
	private int medianCheckpointedSlot;
	
	private boolean noCoalesce = false;
	
	private final int batchSize;

	public PValuePacket(Ballot b, ProposalPacket p) {
		super(p);
		this.ballot = b;
		this.medianCheckpointedSlot = -1;
		this.recovery = false; // true only when created from json
		this.batchSize = p.batchSize();
		// packet type needs to be explicitly assigned later
	}

	// Super-called by inheritors and by PaxosInstanceStateMachine
	public PValuePacket(PValuePacket pvalue) {
		super(pvalue);
		this.ballot = pvalue.ballot;
		this.medianCheckpointedSlot = pvalue.medianCheckpointedSlot;
		this.packetType = pvalue.getType();
		this.recovery = false; // true only when created from json
		this.batchSize = pvalue.batchSize;
	}

	public PValuePacket(JSONObject json) throws JSONException {
		super(json);
		this.ballot = new Ballot(json.getString(PaxosPacket.NodeIDKeys.B
				.toString()));
		this.medianCheckpointedSlot = json
				.getInt(PaxosPacket.Keys.GC_S.toString());
		this.recovery = json.optBoolean(PaxosPacket.Keys.RCVRY.toString());
		this.packetType = PaxosPacket.getPaxosPacketType(json);
		this.noCoalesce = json.optBoolean(PaxosPacket.Keys.NO_COALESCE.toString());
		this.batchSize = json.getInt(RequestPacket.Keys.BS.toString());
	}

	public PValuePacket(net.minidev.json.JSONObject json) throws JSONException {
		super(json);
		this.ballot = new Ballot((String)json.get(PaxosPacket.NodeIDKeys.B
				.toString()));
		this.medianCheckpointedSlot = (Integer)json
				.get(PaxosPacket.Keys.GC_S.toString());
		this.recovery = json.containsKey(PaxosPacket.Keys.RCVRY.toString()) ? (Boolean)json.get(PaxosPacket.Keys.RCVRY.toString()) : false;
		this.packetType = PaxosPacket.getPaxosPacketType(json);
		this.noCoalesce = json.containsKey(PaxosPacket.Keys.NO_COALESCE.toString()) ? (Boolean)json.get(PaxosPacket.Keys.NO_COALESCE.toString()) : false;
		this.batchSize = (Integer)json.get(RequestPacket.Keys.BS.toString());
	}

	public PValuePacket makeDecision(int mcSlot) {
		this.packetType = PaxosPacketType.DECISION;
		this.medianCheckpointedSlot = mcSlot;
		this.setStringifiedSelf(null);
		/*
		 * Only prepares, accepts, and decisions are replyed, so we should never
		 * be making a decision out of a recovery packet, hence the assert.
		 */
		assert (!recovery);
		return (this);
	}

	public PValuePacket preempt() {
		// Note: preemption does not change final fields
		this.packetType = PaxosPacketType.PREEMPTED;
		return this;
	}
	
	public boolean isCoalescable() {
		return !this.noCoalesce
				&& this.getType().equals(PaxosPacket.PaxosPacketType.DECISION);
	}

	public int getMedianCheckpointedSlot() {
		return this.medianCheckpointedSlot;
	}

	protected void setMedianCheckpointedSlot(int slot) {
		this.medianCheckpointedSlot = slot;
	}

	public boolean isRecovery() {
		return this.recovery;
	}

	public PValuePacket setRecovery() {
		return this.setRecovery(true);
	}
	public PValuePacket setRecovery(boolean b) {
		this.recovery = b;
		return this;
	}
	
	@Override
	protected String getSummaryString() {
		return ballot + ", " + super.getSummaryString()
				+ (this.isRecovery() ? "(recovery)" : "");
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		json.put(PaxosPacket.NodeIDKeys.B.toString(), ballot.toString());
		json.put(PaxosPacket.Keys.GC_S.toString(),
				this.medianCheckpointedSlot);
		if(this.recovery) json.put(PaxosPacket.Keys.RCVRY.toString(), this.recovery);
		if(this.noCoalesce) json.put(PaxosPacket.Keys.NO_COALESCE.toString(), this.noCoalesce);
		json.put(RequestPacket.Keys.BS.toString(), this.batchSize);
		return json;
	}
	
	@Override
	public net.minidev.json.JSONObject toJSONSmartImpl() throws JSONException {
		net.minidev.json.JSONObject json = super.toJSONSmartImpl();
		json.put(PaxosPacket.NodeIDKeys.B.toString(), ballot.toString());
		json.put(PaxosPacket.Keys.GC_S.toString(),
				this.medianCheckpointedSlot);
		if(this.recovery) json.put(PaxosPacket.Keys.RCVRY.toString(), this.recovery);
		if(this.noCoalesce) json.put(PaxosPacket.Keys.NO_COALESCE.toString(), this.noCoalesce);
		json.put(RequestPacket.Keys.BS.toString(), this.batchSize);
		return json;
	}

	public PValuePacket setNoCoalesce() {
		this.noCoalesce = true;
		return this;
	}

	public PValuePacket getMetaDecision() {
		PValuePacket meta = new PValuePacket(this.ballot, new ProposalPacket(this.slot,
				new RequestPacket(this.requestID,
						null, this.stop, this).getFirstOnly()));
		meta.packetType = PaxosPacketType.DECISION;
		return meta;
	}
}
