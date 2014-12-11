package edu.umass.cs.gns.gigapaxos.multipaxospacket;

import org.json.JSONException;
import org.json.JSONObject;


public final class AcceptPacket extends PValuePacket {
	protected static enum Keys {SENDER_NODE, MAJORITY_COMMITTED_SLOT};
	
	public final int sender; // sender nodeID
	public final int majorityExecutedSlot; 	// slot number up to which a majority have committed

	public AcceptPacket(int nodeID, PValuePacket pValue, int slotNumber) {
		super(pValue);
		this.packetType = PaxosPacketType.ACCEPT;
		this.sender = nodeID;
		this.majorityExecutedSlot = slotNumber;
	}

	public AcceptPacket(JSONObject json) throws JSONException {
		super(json);
		assert(PaxosPacket.getPaxosPacketType(json) == PaxosPacketType.ACCEPT); // coz class is final
		this.packetType = PaxosPacketType.ACCEPT;
		this.sender = json.getInt(Keys.SENDER_NODE.toString());
		this.majorityExecutedSlot = json.getInt(Keys.MAJORITY_COMMITTED_SLOT.toString());
		this.paxosID = json.getString(PaxosPacket.PAXOS_ID);
	}


	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.SENDER_NODE.toString(), sender);
		json.put(Keys.MAJORITY_COMMITTED_SLOT.toString(), majorityExecutedSlot);
		return json;
	}
}
