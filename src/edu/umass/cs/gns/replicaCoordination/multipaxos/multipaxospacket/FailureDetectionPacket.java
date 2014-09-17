package edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket;

import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;
import org.json.JSONException;
import org.json.JSONObject;


public class FailureDetectionPacket extends PaxosPacket{

	public final NodeId<String> senderNodeID; 
	private final NodeId<String> responderNodeID; // destination
	private final boolean status; // up status of destination, currently not used.

	public FailureDetectionPacket(NodeId<String> senderNodeID, NodeId<String> responderNodeID, boolean status) {
    	super((PaxosPacket)null);
		this.senderNodeID = senderNodeID;
		this.responderNodeID = responderNodeID;
		this.packetType = PaxosPacketType.FAILURE_DETECT;
		this.status = status;
	}

	public FailureDetectionPacket(JSONObject json) throws JSONException {
		super(json);
		this.senderNodeID = new NodeId<String>(json.getString("sender"));
		this.responderNodeID = new NodeId<String>(json.getString("responder"));
		assert(PaxosPacket.getPaxosPacketType(json)==PaxosPacketType.FAILURE_DETECT);
		this.packetType = PaxosPacket.getPaxosPacketType(json);
		this.status = json.getBoolean("status");
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = new JSONObject();
		json.put("status", status);
		json.put("sender", senderNodeID.get());
		json.put("responder", responderNodeID.get());
		return json;
	}

}
