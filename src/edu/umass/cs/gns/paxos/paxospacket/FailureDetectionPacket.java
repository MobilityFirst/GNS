package edu.umass.cs.gns.paxos.paxospacket;

import org.json.JSONException;
import org.json.JSONObject;

public class FailureDetectionPacket extends Packet{
	
	/**
	 * ID of node that sent the packet.
	 */
	public int senderNodeID;
	
	/**
	 * ID of destination node. 
	 */
	public int responderNodeID;
	
	/**
	 * Status of responder node. true = node is up, false = node is down.
	 */
	public boolean status;
	
	
	public FailureDetectionPacket(int senderNodeID, int responderNodeID, 
		boolean status, int packetType) {
		this.senderNodeID = senderNodeID;
		this.responderNodeID = responderNodeID;
		this.packetType = packetType;
		this.status = status;
	}

	public FailureDetectionPacket(JSONObject json) throws JSONException {
		this.senderNodeID = json.getInt("sender");
		this.responderNodeID = json.getInt("responder");
		this.packetType = json.getInt(PaxosPacketType.ptype);
		this.status = json.getBoolean("status");
	}
	
	public FailureDetectionPacket getFailureDetectionResponse() {
		return new FailureDetectionPacket(this.senderNodeID, 
				this.responderNodeID, true, PaxosPacketType.FAILURE_RESPONSE); 
	}
	
	@Override
	public JSONObject toJSONObject() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(PaxosPacketType.ptype, packetType);
		json.put("status", status);
		json.put("sender", senderNodeID);
		json.put("responder", responderNodeID);
		return json;
	}
	
}
