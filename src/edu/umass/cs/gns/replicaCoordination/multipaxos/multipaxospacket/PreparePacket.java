package edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket;

import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.Ballot;


public final class PreparePacket extends PaxosPacket {
	protected final static String COORDINATOR="coordinatorID";

	public final NodeId<String>  coordinatorID;
	public final Ballot ballot;
	public final int firstUndecidedSlot;
	public final NodeId<String> receiverID;
	public final boolean recovery;

	public PreparePacket(NodeId<String> coordinatorID, NodeId<String> receiverID, Ballot b) {
		super((PaxosPacket)null);
		this.coordinatorID = coordinatorID;
		this.receiverID = receiverID;
		this.ballot = b;
		this.packetType = PaxosPacketType.PREPARE;
		this.firstUndecidedSlot = -1;
		this.recovery = false;
	}

	public PreparePacket(NodeId<String> coordinatorID, NodeId<String> receiverID, Ballot ballot, int slotNumber) {
		super((PaxosPacket)null);
		this.coordinatorID = coordinatorID;
		this.receiverID = receiverID;
		this.ballot = ballot;
		this.firstUndecidedSlot = slotNumber;
		this.packetType = PaxosPacketType.PREPARE;
		this.recovery = false;
	}
	
	private PreparePacket(NodeId<String> rcvrID, PreparePacket prepare) {
		super(prepare);
		this.coordinatorID = prepare.coordinatorID;
		this.receiverID = rcvrID;
		this.ballot = prepare.ballot;
		this.firstUndecidedSlot = prepare.firstUndecidedSlot;
		this.packetType = PaxosPacketType.PREPARE;
		this.recovery = prepare.recovery;
	}

	public PreparePacket fixPreparePacketReceiver(NodeId<String> rcvrID) {
		PreparePacket prepare = (!this.receiverID.equals(rcvrID) ? new PreparePacket(rcvrID, this) : this);
		//prepare.putPaxosID(this.getPaxosID(), this.version);
		return prepare;
	}

	public PreparePacket(JSONObject json) throws JSONException{
		super(json);
		assert(PaxosPacket.getPaxosPacketType(json)==PaxosPacketType.PREPARE); // coz class is final
		this.packetType = PaxosPacket.getPaxosPacketType(json);
		this.coordinatorID = new NodeId<String>(json.getString(COORDINATOR));
		this.receiverID = new NodeId<String>(json.getString("receiverID"));
		this.ballot = new Ballot(json.getString("ballot"));
		this.firstUndecidedSlot = json.getInt("slotNumber");
		this.recovery = json.getBoolean(RECOVERY);
	}


	@Override
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = new JSONObject();
		json.put(COORDINATOR, coordinatorID.get());
		json.put("receiverID", receiverID.get());
		json.put("ballot", ballot.toString());
		json.put("slotNumber", firstUndecidedSlot);
		json.put(RECOVERY, recovery);

		return json;
	}
}
