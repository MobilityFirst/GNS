package edu.umass.cs.gns.packet.paxospacket;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.Serializable;

public class ProposalPacket extends Packet implements Serializable {
	
	public int slot;
	
	public RequestPacket req;

	public  static String SLOT = "s1";
    public  static String GC_SLOT = "s2";

    public  int gcSlot = 0;

//	public ProposalPacket(int slot, RequestPacket req, int packetType) {
//		this.slot = slot;
//		this.req = req;
//		this.packetType = packetType;
//        this.gcSlot = 0;
//	}

    public ProposalPacket(int slot, RequestPacket req, int packetType, int gcSlot) {
        this.slot = slot;
        this.req = req;
        this.packetType = packetType;
        this.gcSlot = gcSlot;
    }

    public ProposalPacket(JSONObject json) throws JSONException {
        try {
		    this.req = new RequestPacket(json);
        } catch (JSONException e) {
            this.req = null;
        }
		this.packetType = json.getInt(PaxosPacketType.ptype);
		this.slot = json.getInt(SLOT);
        if (json.has(GC_SLOT)) gcSlot = json.getInt(GC_SLOT);
	}
	
	public ProposalPacket getDecisionPacket() {
		return new ProposalPacket(slot, req, PaxosPacketType.DECISION, gcSlot);
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
        if (gcSlot > 0) json.put(GC_SLOT, gcSlot);
		return json;
	}

}
