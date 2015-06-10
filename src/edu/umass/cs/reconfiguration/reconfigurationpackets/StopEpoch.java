package edu.umass.cs.reconfiguration.reconfigurationpackets;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.nio.Stringifiable;
import edu.umass.cs.reconfiguration.InterfaceReconfigurableRequest;
import edu.umass.cs.reconfiguration.InterfaceReplicableRequest;

/**
 * @author V. Arun
 * @param <NodeIDType> 
 */
public class StopEpoch<NodeIDType> extends
		BasicReconfigurationPacket<NodeIDType> implements
		InterfaceReconfigurableRequest, InterfaceReplicableRequest {

	private static enum Keys {
		GET_FINALSTATE
	};

	private final boolean getFinalState;

	/**
	 * @param initiator
	 * @param name
	 * @param epochNumber
	 * @param getFinalState
	 */
	public StopEpoch(NodeIDType initiator, String name, int epochNumber,
			boolean getFinalState) {
		super(initiator, ReconfigurationPacket.PacketType.STOP_EPOCH, name,
				epochNumber);
		this.getFinalState = getFinalState;
	}

	/**
	 * @param initiator
	 * @param name
	 * @param epochNumber
	 */
	public StopEpoch(NodeIDType initiator, String name, int epochNumber) {
		this(initiator, name, epochNumber, false);
	}

	/**
	 * @param json
	 * @param unstringer
	 * @throws JSONException
	 */
	public StopEpoch(JSONObject json, Stringifiable<NodeIDType> unstringer)
			throws JSONException {
		super(json, unstringer);
		this.getFinalState = json.optBoolean(Keys.GET_FINALSTATE.toString());
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.GET_FINALSTATE.toString(), this.getFinalState);
		return json;
	}

	@Override
	public boolean isStop() {
		return true;
	}

	@Override
	public boolean needsCoordination() {
		return true;
	}

	@Override
	public void setNeedsCoordination(boolean b) {
		// do nothing
	}
	
	/**
	 * @return True if the epoch final state should be sent with the
	 *         AckStopEpoch packet.
	 */
	public boolean shouldGetFinalState() {	
		return this.getFinalState;
	}
}
