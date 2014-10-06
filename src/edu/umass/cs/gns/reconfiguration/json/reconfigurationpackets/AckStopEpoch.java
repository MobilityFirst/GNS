package edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets;

import org.json.JSONException;
import org.json.JSONObject;

/**
@author V. Arun
 */
public class AckStopEpoch<NodeIDType> extends BasicReconfigurationPacket<NodeIDType> {

	public AckStopEpoch(NodeIDType sender, StopEpoch<NodeIDType> stopEpoch) {
		super(stopEpoch.getInitiator(), ReconfigurationPacket.PacketType.ACK_STOP_EPOCH, 
			stopEpoch.getServiceName(), stopEpoch.getEpochNumber());
		this.setKey(stopEpoch.getKey());
		this.setSender(sender);
	}
	public AckStopEpoch(JSONObject json) throws JSONException {
		super(json);
	}
}
