package edu.umass.cs.gigapaxos.paxospackets;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * @author arun
 *
 */
@SuppressWarnings("javadoc")
public final class AcceptPacket extends PValuePacket {
	protected static enum Keys {
		SENDER_NODE, MAJORITY_EXECUTED_SLOT
	};

	/**
	 * Sender node ID. FIXME: should just be the same as the ballot coordinator.
	 */
	public final int sender;

	public AcceptPacket(int nodeID, PValuePacket pValue, int slotNumber) {
		super(pValue);
		this.packetType = PaxosPacketType.ACCEPT;
		this.sender = nodeID;
		this.setMedianCheckpointedSlot(slotNumber);
	}

	public AcceptPacket(JSONObject json) throws JSONException {
		super(json);
		assert (PaxosPacket.getPaxosPacketType(json) == PaxosPacketType.ACCEPT); 
		this.packetType = PaxosPacketType.ACCEPT;
		this.sender = json.getInt(Keys.SENDER_NODE.toString());
		this.paxosID = json.getString(PaxosPacket.PAXOS_ID);
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.SENDER_NODE.toString(), sender);
		return json;
	}

	@Override
	protected String getSummaryString() {
		return super.getSummaryString();
	}
}
