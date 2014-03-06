package edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.packet.Packet;
import edu.umass.cs.gns.packet.PaxosPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.Ballot;

public class PValuePacket extends PaxosPacket {


	public Ballot ballot;
	public ProposalPacket proposal;

	public PValuePacket(Ballot b, ProposalPacket p) {
		this.ballot = b;
		this.proposal = p;
	}

	static String BALLOT = "ballot";

	public PValuePacket(JSONObject json) throws JSONException{
		this.proposal = new ProposalPacket(json);
		//        try{
		//
		//        } catch (Exception e) {
		//            this.proposal = null;
		//        }
		//		this.packetType = json.getInt(PaxosPacketType.ptype);
		this.ballot = new Ballot(json.getString(BALLOT));
	}
	public int getType() {
		return this.packetType;
	}
	public void setType(int type) {
		this.packetType = type;
	}

	@Override
	public JSONObject toJSONObject() throws JSONException {
		JSONObject json = this.proposal.toJSONObject();
	    Packet.putPacketType(json, PacketType.PAXOS_PACKET); json.put(PaxosPacket.paxosIDKey, this.paxosID);

		json.put(BALLOT, ballot.toString());
		json.put(PaxosPacketType.ptype, packetType);
		return json;
	}

}

