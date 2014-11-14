package edu.umass.cs.gns.gigapaxos.multipaxospacket;

import org.json.JSONException;
import org.json.JSONObject;


public final class AcceptPacket extends PValuePacket {
	protected static enum Keys {SENDER_NODE, MAJORITY_COMMITTED_SLOT};
	
	public final int nodeID; // sender nodeID
	public final int majorityCommittedSlot; 	// slot number up to which a majority have committed

	//protected static final String NODE = "node";
	//private static final String SLOT = "slotCommitted@Majority";

	public AcceptPacket(int nodeID, PValuePacket pValue, int slotNumber) {
		super(pValue);
		this.packetType = PaxosPacketType.ACCEPT;
		this.nodeID = nodeID;
		this.majorityCommittedSlot = slotNumber;
	}

	public AcceptPacket(JSONObject json) throws JSONException {
		super(json);
		assert(PaxosPacket.getPaxosPacketType(json) == PaxosPacketType.ACCEPT); // coz class is final
		this.packetType = PaxosPacketType.ACCEPT;
		this.nodeID = json.getInt(Keys.SENDER_NODE.toString());
		this.majorityCommittedSlot = json.getInt(Keys.MAJORITY_COMMITTED_SLOT.toString());
		this.paxosID = json.getString(PaxosPacket.PAXOS_ID);
	}


	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.SENDER_NODE.toString(), nodeID);
		json.put(Keys.MAJORITY_COMMITTED_SLOT.toString(), majorityCommittedSlot);
		json.put(RECOVERY, this.recovery);
		return json;
	}
}
