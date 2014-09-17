package edu.umass.cs.gns.paxos.paxospacket;

import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;
import org.json.JSONException;
import org.json.JSONObject;

public class FailureDetectionPacket extends PaxosPacket{
	
	/**
	 * ID of node that sent the packet.
	 */
	public NodeId<String> senderNodeID;
	
	/**
	 * ID of destination node. 
	 */
	public NodeId<String> responderNodeID;
	
	/**
	 * Status of responder node. true = node is up, false = node is down.
	 */
	public boolean status;
	
	
	public FailureDetectionPacket(NodeId<String> senderNodeID, NodeId<String> responderNodeID, 
		boolean status, PaxosPacketType packetType) {
		this.senderNodeID = senderNodeID;
		this.responderNodeID = responderNodeID;
		this.packetType = packetType.getInt();
		this.status = status;
	}

	public FailureDetectionPacket(JSONObject json) throws JSONException {
		this.senderNodeID = new NodeId<String>(json.getString("sender"));
		this.responderNodeID = new NodeId<String>(json.getInt("responder"));
		this.packetType = json.getInt(PaxosPacket.PACKET_TYPE_FIELD_NAME);
		this.status = json.getBoolean("status");
	}
	
	public FailureDetectionPacket getFailureDetectionResponse() {
		return new FailureDetectionPacket(this.senderNodeID, 
				this.responderNodeID, true, PaxosPacketType.FAILURE_RESPONSE); 
	}
	
	@Override
	public JSONObject toJSONObject() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(PaxosPacket.PACKET_TYPE_FIELD_NAME, packetType);
		json.put("status", status);
		json.put("sender", senderNodeID.get());
		json.put("responder", responderNodeID.get());
		return json;
	}
	
}
