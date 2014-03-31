package edu.umass.cs.gns.paxos.paxospacket;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.paxos.Ballot;

import java.io.Serializable;

public class PValuePacket extends Packet implements Serializable {

	
	public Ballot ballot;
	public ProposalPacket proposal;
	
	public PValuePacket(Ballot b, ProposalPacket p) {
		this.ballot = b;
		this.proposal = p;
	}

    static String BALLOT = "b1";

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
	
	@Override
	public JSONObject toJSONObject() throws JSONException {
        JSONObject json = this.proposal.toJSONObject();
		json.put(BALLOT, ballot.toString());
		json.put(PaxosPacketType.ptype, packetType);
		return json;
	}
	
}

