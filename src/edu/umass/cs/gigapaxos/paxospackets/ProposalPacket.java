package edu.umass.cs.gigapaxos.paxospackets;

import org.json.JSONException;
import org.json.JSONObject;

public class ProposalPacket extends RequestPacket {	
	public final int slot;

    public ProposalPacket(int slot, RequestPacket req) {
    	super(req);
    	this.slot = slot;
    	this.packetType = PaxosPacketType.PROPOSAL;
    }
    protected ProposalPacket(ProposalPacket prop) {
    	super(prop);
    	this.slot = prop.slot;
    	this.packetType = PaxosPacketType.PROPOSAL;
    }

    public ProposalPacket(JSONObject json) throws JSONException {
    	super(json);
		this.packetType = PaxosPacketType.PROPOSAL; 
		this.slot = json.getInt(PaxosPacket.Keys.SLOT.toString());
	}
    
	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		json.put(PaxosPacket.Keys.SLOT.toString(), slot);
		return json;
	}
}
