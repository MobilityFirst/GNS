package edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets;

import org.json.JSONException;
import org.json.JSONObject;

/**
@author V. Arun
 */
public class AckDropEpochFinalState<NodeIDType> extends BasicReconfigurationPacket<NodeIDType> {

	public AckDropEpochFinalState(NodeIDType sender, DropEpochFinalState<NodeIDType> dropEpoch) {
		super(dropEpoch.getInitiator(), ReconfigurationPacket.PacketType.ACK_DROP_EPOCH_FINAL_STATE, 
			dropEpoch.getServiceName(), dropEpoch.getEpochNumber());
		this.setKey(dropEpoch.getKey());
		this.setSender(sender);
	}
	public AckDropEpochFinalState(JSONObject json) throws JSONException {
		super(json);
	}
}
