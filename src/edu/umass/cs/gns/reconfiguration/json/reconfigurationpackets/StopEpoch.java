package edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets;


import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.util.Stringifiable;

/**
@author V. Arun
 */
public class StopEpoch<NodeIDType> extends BasicReconfigurationPacket<NodeIDType> {

	public StopEpoch(
			NodeIDType initiator,
			String name, int epochNumber) {
		super(initiator, ReconfigurationPacket.PacketType.STOP_EPOCH, name, epochNumber);
	}
	public StopEpoch(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
		super(json, unstringer);
	}
}
