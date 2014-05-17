package edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket;

import org.json.JSONException;
import org.json.JSONObject;


public class FailureDetectionPacket extends PaxosPacket{

	public final int senderNodeID; 
	private final int responderNodeID; // destination
	private final boolean status; // up status of destination, currently not used.

	public FailureDetectionPacket(int senderNodeID, int responderNodeID, boolean status) {
    	super((PaxosPacket)null);
		this.senderNodeID = senderNodeID;
		this.responderNodeID = responderNodeID;
		this.packetType = PaxosPacketType.FAILURE_DETECT;
		this.status = status;
	}

	public FailureDetectionPacket(JSONObject json) throws JSONException {
		super(json);
		this.senderNodeID = json.getInt("sender");
		this.responderNodeID = json.getInt("responder");
		assert(PaxosPacket.getPaxosPacketType(json)==PaxosPacketType.FAILURE_DETECT);
		this.packetType = PaxosPacket.getPaxosPacketType(json);
		this.status = json.getBoolean("status");
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = new JSONObject();
		json.put("status", status);
		json.put("sender", senderNodeID);
		json.put("responder", responderNodeID);
		return json;
	}

}
