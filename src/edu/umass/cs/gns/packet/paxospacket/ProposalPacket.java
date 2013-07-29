package edu.umass.cs.gns.packet.paxospacket;

import org.json.JSONException;
import org.json.JSONObject;

public class ProposalPacket extends Packet{
	
	public int slot;
	
	public RequestPacket req;

	public  static String SLOT = "s1";

	public ProposalPacket(int slot, RequestPacket req, int packetType) {
		this.slot = slot;
		this.req = req;
		this.packetType = packetType;
	}
	
	public ProposalPacket(JSONObject json) throws JSONException {
        try {
		    this.req = new RequestPacket(json);
        } catch (JSONException e) {
            this.req = null;
        }
		this.packetType = json.getInt(PaxosPacketType.ptype);
		this.slot = json.getInt(SLOT);
	}
	
	public ProposalPacket getDecisionPacket() {
		return new ProposalPacket(slot, req, PaxosPacketType.DECISION);
	}

    public void makeDecisionPacket() {
        this.packetType = PaxosPacketType.DECISION;
//        this.req = null;

    }
	
	@Override
	public JSONObject toJSONObject() throws JSONException {
        JSONObject json = this.req.toJSONObject();
		json.put(SLOT, slot);
		json.put(PaxosPacketType.ptype, packetType);
		return json;
	}

}
