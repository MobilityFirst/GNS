package edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nio.IntegerPacketType;
import edu.umass.cs.gns.reconfiguration.InterfaceRequest;
import edu.umass.cs.gns.reconfiguration.RequestParseException;
import edu.umass.cs.gns.util.Stringifiable;

/**
 * @author V. Arun
 */
public class AckStopEpoch<NodeIDType> extends
		BasicReconfigurationPacket<NodeIDType> implements InterfaceRequest {

	private static enum Keys {
		FINAL_STATE
	}

	private final String finalState;

	public AckStopEpoch(NodeIDType sender, StopEpoch<NodeIDType> stopEpoch,
			String finalState) {
		super(stopEpoch.getInitiator(),
				ReconfigurationPacket.PacketType.ACK_STOP_EPOCH, stopEpoch
						.getServiceName(), stopEpoch.getEpochNumber());
		this.setKey(stopEpoch.getKey());
		this.setSender(sender);
		this.finalState = finalState;
	}

	public AckStopEpoch(NodeIDType sender, StopEpoch<NodeIDType> stopEpoch) {
		this(sender, stopEpoch, null);
	}

	public AckStopEpoch(JSONObject json, Stringifiable<NodeIDType> unstringer)
			throws JSONException {
		super(json, unstringer);
		this.finalState = json.optString(Keys.FINAL_STATE.toString());
	}

	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.FINAL_STATE.toString(), this.finalState);
		return json;
	}

	@Override
	public IntegerPacketType getRequestType() throws RequestParseException {
		return this.getType();
	}
	
	public String getFinalState() {
		return this.finalState;
	}
}
