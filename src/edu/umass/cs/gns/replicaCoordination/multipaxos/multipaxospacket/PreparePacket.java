package edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.Ballot;


public final class PreparePacket extends PaxosPacket {
	protected final static String COORDINATOR="coordinatorID";

	public final int coordinatorID;
	public final Ballot ballot;
	public  final int firstUndecidedSlot;
	public final int receiverID;
	public final boolean recovery;

	public PreparePacket(int coordinatorID, int receiverID, Ballot b) {
		super((PaxosPacket)null);
		this.coordinatorID = coordinatorID;
		this.receiverID = receiverID;
		this.ballot = b;
		this.packetType = PaxosPacketType.PREPARE;
		this.firstUndecidedSlot = -1;
		this.recovery = false;
	}

	public PreparePacket(int coordinatorID, int receiverID, Ballot ballot, int slotNumber) {
		super((PaxosPacket)null);
		this.coordinatorID = coordinatorID;
		this.receiverID = receiverID;
		this.ballot = ballot;
		this.firstUndecidedSlot = slotNumber;
		this.packetType = PaxosPacketType.PREPARE;
		this.recovery = false;
	}
	
	private PreparePacket(int rcvrID, PreparePacket prepare) {
		super(prepare);
		this.coordinatorID = prepare.coordinatorID;
		this.receiverID = rcvrID;
		this.ballot = prepare.ballot;
		this.firstUndecidedSlot = prepare.firstUndecidedSlot;
		this.packetType = PaxosPacketType.PREPARE;
		this.recovery = prepare.recovery;
	}

	public PreparePacket fixPreparePacketReceiver(int rcvrID) {
		PreparePacket prepare = (this.receiverID!=rcvrID ? new PreparePacket(rcvrID, this) : this);
		//prepare.putPaxosID(this.getPaxosID(), this.version);
		return prepare;
	}

	public PreparePacket(JSONObject json) throws JSONException{
		super(json);
		assert(PaxosPacket.getPaxosPacketType(json)==PaxosPacketType.PREPARE); // coz class is final
		this.packetType = PaxosPacket.getPaxosPacketType(json);
		this.coordinatorID = json.getInt(COORDINATOR);
		this.receiverID = json.getInt("receiverID");
		this.ballot = new Ballot(json.getString("ballot"));
		this.firstUndecidedSlot = json.getInt("slotNumber");
		this.recovery = json.getBoolean(RECOVERY);
	}


	@Override
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = new JSONObject();
		json.put(COORDINATOR, coordinatorID);
		json.put("receiverID", receiverID);
		json.put("ballot", ballot.toString());
		json.put("slotNumber", firstUndecidedSlot);
		json.put(RECOVERY, recovery);

		return json;
	}
}
