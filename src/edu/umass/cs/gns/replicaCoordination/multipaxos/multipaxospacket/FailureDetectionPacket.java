package edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.nsdesign.packet.PaxosPacket;
import edu.umass.cs.gns.nsdesign.packet.Packet.PacketType;

public class FailureDetectionPacket extends PaxosPacket{

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
	public int getType() {
		return this.packetType;
	}

	@Override
	public JSONObject toJSONObject() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(PaxosPacketType.ptype, packetType);
	    Packet.putPacketType(json, PacketType.PAXOS_PACKET); json.put(PaxosPacket.paxosIDKey, this.paxosID);

		json.put("status", status);
		json.put("sender", senderNodeID);
		json.put("responder", responderNodeID);
		return json;
	}

}
