package edu.umass.cs.gns.gigapaxos.multipaxospacket;

import edu.umass.cs.gns.nio.Stringifiable;

import org.json.JSONException;
import org.json.JSONObject;


public class FailureDetectionPacket<NodeIDType> extends PaxosPacket{

	private static enum Keys {SENDER, RESPONDER, STATUS};
	
	public final NodeIDType senderNodeID; 
	private final NodeIDType responderNodeID; // destination
	private final boolean status; // up status of destination, currently not used.

	public FailureDetectionPacket(NodeIDType senderNodeID, NodeIDType responderNodeID, boolean status) {
    	super((PaxosPacket)null);
		this.senderNodeID = senderNodeID;
		this.responderNodeID = responderNodeID;
		this.packetType = PaxosPacketType.FAILURE_DETECT;
		this.status = status;
	}

	public FailureDetectionPacket(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
		super(json);
		this.senderNodeID = unstringer.valueOf(json.getString(Keys.SENDER.toString()));
		this.responderNodeID = unstringer.valueOf(json.getString(Keys.RESPONDER.toString()));
		assert(PaxosPacket.getPaxosPacketType(json)==PaxosPacketType.FAILURE_DETECT);
		this.packetType = PaxosPacket.getPaxosPacketType(json);
		this.status = json.getBoolean(Keys.STATUS.toString());
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(Keys.STATUS.toString(), status);
		json.put(Keys.SENDER.toString(), senderNodeID);
		json.put(Keys.RESPONDER.toString(), responderNodeID);
		return json;
	}

}
