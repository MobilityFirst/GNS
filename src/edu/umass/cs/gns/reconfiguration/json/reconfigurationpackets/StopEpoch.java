package edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.reconfiguration.InterfaceReconfigurableRequest;
import edu.umass.cs.gns.reconfiguration.InterfaceReplicableRequest;
import edu.umass.cs.gns.util.Stringifiable;

/**
 * @author V. Arun
 */
public class StopEpoch<NodeIDType> extends
		BasicReconfigurationPacket<NodeIDType> implements
		InterfaceReconfigurableRequest, InterfaceReplicableRequest {

	private static enum Keys {
		GET_FINALSTATE
	};

	private final boolean getFinalState;

	public StopEpoch(NodeIDType initiator, String name, int epochNumber,
			boolean getFinalState) {
		super(initiator, ReconfigurationPacket.PacketType.STOP_EPOCH, name,
				epochNumber);
		this.getFinalState = getFinalState;
	}

	public StopEpoch(NodeIDType initiator, String name, int epochNumber) {
		this(initiator, name, epochNumber, false);
	}

	public StopEpoch(JSONObject json, Stringifiable<NodeIDType> unstringer)
			throws JSONException {
		super(json, unstringer);
		this.getFinalState = json.optBoolean(Keys.GET_FINALSTATE.toString());
	}

	public JSONObject toJSONObjectimpl() throws JSONException {
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
		// TODO Auto-generated method stub
	}
	
	public boolean shouldGetFinalState() {	
		return this.getFinalState;
	}
}
