package edu.umass.cs.gns.gigapaxos.multipaxospacket;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.gigapaxos.paxosutil.Ballot;


public final class PreparePacket extends PaxosPacket {
	//protected final static String COORDINATOR="coordinatorID";

	public final int coordinatorID;
	public final Ballot ballot;
	public  final int firstUndecidedSlot;
	public final int receiverID;
	
	private boolean recovery;

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
		return prepare;
	}

	public PreparePacket(JSONObject json) throws JSONException{
		super(json);
		assert(PaxosPacket.getPaxosPacketType(json)==PaxosPacketType.PREPARE); // coz class is final
		this.packetType = PaxosPacket.getPaxosPacketType(json);
		this.coordinatorID = json.getInt(PaxosPacket.NodeIDKeys.COORDINATOR.toString());
		this.receiverID = json.getInt(PaxosPacket.NodeIDKeys.RECEIVER.toString());
		this.ballot = new Ballot(json.getString(PaxosPacket.NodeIDKeys.BALLOT.toString()));
		this.firstUndecidedSlot = json.getInt(PaxosPacket.Keys.FIRST_UNDECIDED_SLOT.toString());
		this.recovery = json.getBoolean(PaxosPacket.Keys.RECOVERY.toString());
	}


	@Override
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = new JSONObject();
		json.put(PaxosPacket.NodeIDKeys.COORDINATOR.toString(), coordinatorID);
		json.put(PaxosPacket.NodeIDKeys.RECEIVER.toString(), receiverID);
		json.put(PaxosPacket.NodeIDKeys.BALLOT.toString(), ballot.toString());
		json.put(PaxosPacket.Keys.FIRST_UNDECIDED_SLOT.toString(), firstUndecidedSlot);
		json.put(PaxosPacket.Keys.RECOVERY.toString(), recovery);

		return json;
	}
	
	public boolean isRecovery() {
		return this.recovery;
	}
	public void setRecovery() {
		this.recovery = true;
	}
}
