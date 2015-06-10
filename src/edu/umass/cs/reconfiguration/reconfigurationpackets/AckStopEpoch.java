package edu.umass.cs.reconfiguration.reconfigurationpackets;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.InterfaceRequest;
import edu.umass.cs.nio.IntegerPacketType;
import edu.umass.cs.nio.Stringifiable;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

/**
 * @author V. Arun
 * @param <NodeIDType> 
 */
public class AckStopEpoch<NodeIDType> extends
		BasicReconfigurationPacket<NodeIDType> implements InterfaceRequest {

	private static enum Keys {
		FINAL_STATE
	}

	private final String finalState;

	/**
	 * @param sender
	 * @param stopEpoch
	 * @param finalState
	 */
	public AckStopEpoch(NodeIDType sender, StopEpoch<NodeIDType> stopEpoch,
			String finalState) {
		super(stopEpoch.getInitiator(),
				ReconfigurationPacket.PacketType.ACK_STOP_EPOCH, stopEpoch
						.getServiceName(), stopEpoch.getEpochNumber());
		this.setKey(stopEpoch.getKey());
		this.setSender(sender);
		this.finalState = finalState;
	}

	/**
	 * @param sender
	 * @param stopEpoch
	 */
	public AckStopEpoch(NodeIDType sender, StopEpoch<NodeIDType> stopEpoch) {
		this(sender, stopEpoch, null);
	}

	/**
	 * @param json
	 * @param unstringer
	 * @throws JSONException
	 */
	public AckStopEpoch(JSONObject json, Stringifiable<NodeIDType> unstringer)
			throws JSONException {
		super(json, unstringer);
		this.finalState = json.has(Keys.FINAL_STATE.toString()) ? json.getString(Keys.FINAL_STATE.toString()) : null;
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.FINAL_STATE.toString(), this.finalState);
		return json;
	}

	@Override
	public IntegerPacketType getRequestType() throws RequestParseException {
		return this.getType();
	}
	
	/**
	 * @return Final state of the epoch whose stoppage is being acknowledged.
	 */
	public String getFinalState() {
		return this.finalState;
	}
}
