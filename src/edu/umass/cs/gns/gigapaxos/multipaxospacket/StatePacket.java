package edu.umass.cs.gns.gigapaxos.multipaxospacket;

import edu.umass.cs.gns.gigapaxos.paxosutil.Ballot;

import org.json.JSONException;
import org.json.JSONObject;


public final class StatePacket extends PaxosPacket{

	public final Ballot ballot;
	public final int slotNumber;
	public final String state;

	public StatePacket(Ballot b, int slotNumber, String state) {
		super((PaxosPacket)null);
		this.ballot = b;
		this.slotNumber = slotNumber;
		this.state = state;
		this.packetType = PaxosPacketType.CHECKPOINT_STATE;
	}


	public StatePacket(JSONObject json) throws JSONException{
		super(json);
		assert(PaxosPacket.getPaxosPacketType(json)==PaxosPacketType.CHECKPOINT_STATE); // coz class is final
		this.packetType = PaxosPacketType.CHECKPOINT_STATE;
		this.slotNumber = json.getInt(PaxosPacket.Keys.SLOT.toString());
		this.ballot = new Ballot(json.getString(PaxosPacket.NodeIDKeys.BALLOT.toString()));
		this.state = json.getString(PaxosPacket.Keys.STATE.toString());
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(PaxosPacket.Keys.SLOT.toString(), this.slotNumber);
		json.put(PaxosPacket.NodeIDKeys.BALLOT.toString(), this.ballot.ballotNumber+":"+this.ballot.coordinatorID);
		json.put(PaxosPacket.Keys.STATE.toString(), this.state);
		return json;
	}

}
