package edu.umass.cs.gns.gigapaxos.multipaxospacket;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.gigapaxos.paxosutil.Ballot;

/* A <slot, ballot, request> three-tuple. The slot and request are
 * within the proposal here. A pvalue is used internally by paxos
 * acceptors and coordinators. It also acts as a DECISION packet
 * as every committed request must be associated with some slot,
 * and ballot.
 */

public class PValuePacket extends ProposalPacket {

	public final Ballot ballot;
	
	private boolean recovery;
	private int medianCheckpointedSlot; // for garbage collection, similar to that in AcceptPacket
	
	public PValuePacket(Ballot b, ProposalPacket p) {
    	super(p);
		this.ballot = b;
		this.medianCheckpointedSlot=-1;
		// packetType inherited, not assigned until DECISION or PREEMPTED
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

	public PValuePacket(JSONObject json) throws JSONException{
		super(json);
		this.ballot = new Ballot(json.getString(PaxosPacket.NodeIDKeys.BALLOT.toString()));
		this.medianCheckpointedSlot = json.getInt(PaxosPacket.Keys.MEDIAN_CHECKPOINTED_SLOT.toString());
		this.recovery = json.getBoolean(PaxosPacket.Keys.RECOVERY.toString());
		this.packetType = PaxosPacket.getPaxosPacketType(json);
	}
	
	public PValuePacket makeDecision(int mcSlot, int issuer) {
		this.packetType = PaxosPacketType.DECISION;
		this.medianCheckpointedSlot = mcSlot;
		return new PValuePacket(this); // can't modify recovery, so new
	}
	public PValuePacket preempt() {
		this.packetType = PaxosPacketType.PREEMPTED; // preemption does not change final fields, unlike getDecisionPacket
		return this;
	}
	public int getMedianCheckpointedSlot() {return this.medianCheckpointedSlot;}
	protected void setMedianCheckpointedSlot(int slot) {this.medianCheckpointedSlot = slot;}
	public boolean isRecovery() {
		return this.recovery;
	}
	public void setRecovery() {
		this.recovery = true;
	}
	/* A convenience method for when we really need a RequestPacket, not a deeper
	 * inherited PaxosPacket, e.g., when forwarding a preempted pvalue to the
	 * current, new coordinator to re-propose. Else, we would need to explicitly
	 * handle the PREEMPTED type in PaxosInstanceStateMachine.handlePaxosMessage.
	 */
	public RequestPacket getRequestPacket() {return new RequestPacket(this);}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		json.put(PaxosPacket.NodeIDKeys.BALLOT.toString(), ballot.toString());
		json.put(PaxosPacket.Keys.MEDIAN_CHECKPOINTED_SLOT.toString(), this.medianCheckpointedSlot);
		json.put(PaxosPacket.Keys.RECOVERY.toString(), this.recovery);
		//json.put(PaxosPacket.NodeIDKeys.DECISION_ISSUER.toString(), this.decisionIssuer);
		return json;
	}
}

