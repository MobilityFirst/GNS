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

	public StopEpoch(NodeIDType initiator, String name, int epochNumber) {
		super(initiator, ReconfigurationPacket.PacketType.STOP_EPOCH, name,
				epochNumber);
	}

	public StopEpoch(JSONObject json, Stringifiable<NodeIDType> unstringer)
			throws JSONException {
		super(json, unstringer);
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
}
