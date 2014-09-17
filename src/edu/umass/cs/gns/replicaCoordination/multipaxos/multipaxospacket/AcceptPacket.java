package edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket;

import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;
import org.json.JSONException;
import org.json.JSONObject;


public final class AcceptPacket extends PValuePacket {
	public final NodeId<String> nodeID; // sender nodeID
	public final int majorityCommittedSlot; 	// slot number up to which a majority have committed
	//public final boolean recovery;

	protected static final String NODE = "node";
	private static final String SLOT = "slotCommitted@Majority";

	public AcceptPacket(NodeId<String> nodeID, PValuePacket pValue, int slotNumber) {
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
		this.nodeID = new NodeId<String>(json.getString(NODE));
		this.majorityCommittedSlot = json.getInt(SLOT);
		this.paxosID = json.getString(PaxosPacket.PAXOS_ID);
		//this.recovery = json.getBoolean(RECOVERY);
	}


	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		json.put(NODE, nodeID.get());
		json.put(SLOT, majorityCommittedSlot);
		json.put(RECOVERY, this.recovery);
		return json;
	}
}
