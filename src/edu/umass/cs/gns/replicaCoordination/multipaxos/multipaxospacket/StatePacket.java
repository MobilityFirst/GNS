package edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket;

import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.Ballot;

import org.json.JSONException;
import org.json.JSONObject;


public final class StatePacket extends PaxosPacket{
	private final static String SLOT="SLOT";
	private final static String BALLOT="BALLOT";
	private final static String STATE="STATE";

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
		this.slotNumber = json.getInt(SLOT);
		this.ballot = new Ballot(json.getString(BALLOT));
		this.state = json.getString(STATE);
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(SLOT, this.slotNumber);
		json.put(BALLOT, this.ballot.ballotNumber+":"+this.ballot.coordinatorID);
		json.put(STATE, this.state);
		return json;
	}

}
