package edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets;


import org.json.JSONException;
import org.json.JSONObject;

/**
@author V. Arun
 */
public class StopEpoch<NodeIDType> extends BasicReconfigurationPacket<NodeIDType> {

	public StopEpoch(
			NodeIDType initiator,
			String name, int epochNumber) {
		super(initiator, ReconfigurationPacket.PacketType.STOP_EPOCH, name, epochNumber);
	}
	public StopEpoch(JSONObject json) throws JSONException {
		super(json);
	}
}
