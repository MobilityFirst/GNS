package edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket;

import edu.umass.cs.gns.nsdesign.packet.PaxosPacket;
import org.json.JSONException;
import org.json.JSONObject;

/* A sync packet is simply a request by the sender
 * (nodeID) to the receiver asking which commits are
 * missing at the receiver. The receiver of a sync
 * packet is expected to respond with missing commits.
 */

public final class SynchronizePacket extends PaxosPacket{
	private final static String NODE = "nodeID";

	public final int nodeID;

	public SynchronizePacket(int nodeID) {
		super((PaxosPacket)null);
		this.packetType = PaxosPacketType.SYNC_REQUEST;
		this.nodeID = nodeID;

	}

	public  SynchronizePacket(JSONObject jsonObject) throws JSONException{
		super(jsonObject);
		assert(PaxosPacket.getPaxosPacketType(jsonObject)==PaxosPacketType.SYNC_REQUEST);
		this.packetType = PaxosPacketType.SYNC_REQUEST;
		this.nodeID = jsonObject.getInt(NODE);
		this.paxosID = jsonObject.getString(PaxosPacket.PAXOS_ID);
	}


	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(NODE, nodeID);
		return json;
	}
}
