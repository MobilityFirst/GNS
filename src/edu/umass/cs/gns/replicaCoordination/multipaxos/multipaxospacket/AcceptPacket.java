package edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket;

import org.json.JSONException;
import org.json.JSONObject;


public final class AcceptPacket extends PValuePacket {
	public final int nodeID; // sender nodeID
	public final int majorityCommittedSlot; 	// slot number up to which a majority have committed
	//public final boolean recovery;

	protected static final String NODE = "node";
	private static final String SLOT = "slotCommitted@Majority";

	public AcceptPacket(int nodeID, PValuePacket pValue, int slotNumber) {
		super(pValue);
		this.packetType = PaxosPacketType.ACCEPT;
		this.nodeID = nodeID;
		this.majorityCommittedSlot = slotNumber;
		//this.recovery = false;
	}

	public AcceptPacket(JSONObject json) throws JSONException {
		super(json);
		assert(PaxosPacket.getPaxosPacketType(json) == PaxosPacketType.ACCEPT); // coz class is final
		this.packetType = PaxosPacketType.ACCEPT;
		this.nodeID = json.getInt(NODE);
		this.majorityCommittedSlot = json.getInt(SLOT);
		this.paxosID = json.getString(PaxosPacket.PAXOS_ID);
		//this.recovery = json.getBoolean(RECOVERY);
	}


	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		json.put(NODE, nodeID);
		json.put(SLOT, majorityCommittedSlot);
		json.put(RECOVERY, this.recovery);
		return json;
	}
}
