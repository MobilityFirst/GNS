package edu.umass.cs.gigapaxos.paxospackets;

import edu.umass.cs.nio.Stringifiable;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * 
 * @author arun
 * @param <NodeIDType>
 * 
 *            This "failure detection" packet is really a keep-alive, i.e., its
 *            purpose is to tell the recipient that the sender is up.
 */

@SuppressWarnings("javadoc")
public class FailureDetectionPacket<NodeIDType> extends PaxosPacket {

	private static enum Keys {
		SENDER, RESPONDER, STATUS
	};

	/**
	 * Node ID of sender sending this keep-alive packet.
	 */
	public final NodeIDType senderNodeID;
	/**
	 * Destination to which the keep-alive being sent.
	 */
	private final NodeIDType responderNodeID;
	/**
	 * A status flag that is currently not used for anything.
	 */
	private final boolean status;

	public FailureDetectionPacket(NodeIDType senderNodeID,
			NodeIDType responderNodeID, boolean status) {
		super((PaxosPacket) null);
		this.senderNodeID = senderNodeID;
		this.responderNodeID = responderNodeID;
		this.packetType = PaxosPacketType.FAILURE_DETECT;
		this.status = status;
	}

	public FailureDetectionPacket(JSONObject json,
			Stringifiable<NodeIDType> unstringer) throws JSONException {
		super(json);
		this.senderNodeID = unstringer.valueOf(json.getString(Keys.SENDER
				.toString()));
		this.responderNodeID = unstringer.valueOf(json.getString(Keys.RESPONDER
				.toString()));
		assert (PaxosPacket.getPaxosPacketType(json) == PaxosPacketType.FAILURE_DETECT);
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

	@Override
	protected String getSummaryString() {
		return "";
	}

}
