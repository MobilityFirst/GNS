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
	private int majorityCommittedSlot; // for garbage collection, similar to that in AcceptPacket
	//private int decisionIssuer=-1; // issuer of the decision that in general may be different from the ballot coordinator
	
	public PValuePacket(Ballot b, ProposalPacket p) {
    	super(p);
		this.ballot = b;
		this.majorityCommittedSlot=-1;
		// packetType inherited, not assigned until DECISION or PREEMPTED
		//this.decisionIssuer = b.coordinatorID;
		this.recovery = false; // true only when created from json
	}
	// Meant for super calling by inheritors
	protected PValuePacket(PValuePacket pvalue) {
		super(pvalue);
		this.ballot = pvalue.ballot;
		this.majorityCommittedSlot = pvalue.majorityCommittedSlot;
		this.packetType = pvalue.getType();
		this.recovery = false; // true only when created from json
		//this.decisionIssuer = pvalue.decisionIssuer;
	}

	public PValuePacket(JSONObject json) throws JSONException{
		super(json);
		this.ballot = new Ballot(json.getString(PaxosPacket.NodeIDKeys.BALLOT.toString()));
		this.majorityCommittedSlot = json.getInt(PaxosPacket.Keys.MEDIAN_COMMITTED_SLOT.toString());
		this.recovery = json.getBoolean(PaxosPacket.Keys.RECOVERY.toString());
		this.packetType = PaxosPacket.getPaxosPacketType(json);
		//this.decisionIssuer = json.getInt(PaxosPacket.NodeIDKeys.DECISION_ISSUER.toString());
	}
	
	public PValuePacket makeDecision(int mcSlot, int issuer) {
		this.packetType = PaxosPacketType.DECISION;
		this.majorityCommittedSlot = mcSlot;
		//this.decisionIssuer = issuer;
		return new PValuePacket(this); // can't modify recovery, so new
	}
	//public int getDecisionIssuer() {return this.decisionIssuer;}
	public PValuePacket preempt() {
		this.packetType = PaxosPacketType.PREEMPTED; // preemption does not change final fields, unlike getDecisionPacket
		return this;
	}
	public int getMajorityCommittedSlot() {return this.majorityCommittedSlot;}
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
		json.put(PaxosPacket.Keys.MEDIAN_COMMITTED_SLOT.toString(), this.majorityCommittedSlot);
		json.put(PaxosPacket.Keys.RECOVERY.toString(), this.recovery);
		//json.put(PaxosPacket.NodeIDKeys.DECISION_ISSUER.toString(), this.decisionIssuer);
		return json;
	}
}

