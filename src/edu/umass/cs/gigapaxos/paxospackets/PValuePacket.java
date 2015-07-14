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

	public PValuePacket(Ballot b, ProposalPacket p) {
		super(p);
		this.ballot = b;
		this.medianCheckpointedSlot = -1;
		this.recovery = false; // true only when created from json
	}

	// Meant for super calling by inheritors
	protected PValuePacket(PValuePacket pvalue) {
		super(pvalue);
		this.ballot = pvalue.ballot;
		this.medianCheckpointedSlot = pvalue.medianCheckpointedSlot;
		this.packetType = pvalue.getType();
		this.recovery = false; // true only when created from json
	}

	public PValuePacket(JSONObject json) throws JSONException {
		super(json);
		this.ballot = new Ballot(json.getString(PaxosPacket.NodeIDKeys.BALLOT
				.toString()));
		this.medianCheckpointedSlot = json
				.getInt(PaxosPacket.Keys.MEDIAN_CHECKPOINTED_SLOT.toString());
		this.recovery = json.getBoolean(PaxosPacket.Keys.RECOVERY.toString());
		this.packetType = PaxosPacket.getPaxosPacketType(json);
	}

	public PValuePacket makeDecision(int mcSlot) {
		this.packetType = PaxosPacketType.DECISION;
		this.medianCheckpointedSlot = mcSlot;
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

	public int getMedianCheckpointedSlot() {
		return this.medianCheckpointedSlot;
	}

	protected void setMedianCheckpointedSlot(int slot) {
		this.medianCheckpointedSlot = slot;
	}

	public boolean isRecovery() {
		return this.recovery;
	}

	public void setRecovery() {
		this.recovery = true;
	}
	
	@Override
	protected String getSummaryString() {
		return ballot + ", " + super.getSummaryString();
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		json.put(PaxosPacket.NodeIDKeys.BALLOT.toString(), ballot.toString());
		json.put(PaxosPacket.Keys.MEDIAN_CHECKPOINTED_SLOT.toString(),
				this.medianCheckpointedSlot);
		json.put(PaxosPacket.Keys.RECOVERY.toString(), this.recovery);
		return json;
	}
}
