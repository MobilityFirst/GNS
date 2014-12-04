package edu.umass.cs.gns.gigapaxos.multipaxospacket;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.gigapaxos.paxosutil.Ballot;


public final class PreparePacket extends PaxosPacket {

	public final Ballot ballot;
	public  final int firstUndecidedSlot; // not really necessary
	
	private boolean recovery; // non-final because AbstractPaxosLogger.rollForward needs to set it

	public PreparePacket(Ballot b) {
		super((PaxosPacket)null);
		this.ballot = b;
		this.packetType = PaxosPacketType.PREPARE;
		this.firstUndecidedSlot = -1;
		this.recovery = false;
	}

	public PreparePacket(int coordinatorID, int receiverID, Ballot ballot, int slotNumber) {
		super((PaxosPacket)null);
		this.ballot = ballot;
		this.firstUndecidedSlot = slotNumber;
		this.packetType = PaxosPacketType.PREPARE;
		this.recovery = false;
	}
	
	public PreparePacket(JSONObject json) throws JSONException{
		super(json);
		assert(PaxosPacket.getPaxosPacketType(json)==PaxosPacketType.PREPARE); // coz class is final
		this.packetType = PaxosPacket.getPaxosPacketType(json);
		this.ballot = new Ballot(json.getString(PaxosPacket.NodeIDKeys.BALLOT.toString()));
		this.firstUndecidedSlot = json.getInt(PaxosPacket.Keys.FIRST_UNDECIDED_SLOT.toString());
		this.recovery = json.getBoolean(PaxosPacket.Keys.RECOVERY.toString());
	}


	@Override
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = new JSONObject();
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