package edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.util.Stringifiable;

/**
@author V. Arun
 */
public class RequestEpochFinalState<NodeIDType> extends BasicReconfigurationPacket<NodeIDType> {

	public RequestEpochFinalState(NodeIDType initiator, String name, int epochNumber) {
		super(initiator, ReconfigurationPacket.PacketType.REQUEST_EPOCH_FINAL_STATE, name, epochNumber);
	}
	public RequestEpochFinalState(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
		super(json, unstringer);
	}
}
