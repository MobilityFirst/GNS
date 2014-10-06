package edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets;

import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;

/**
@author V. Arun
 */
public class AckStartEpoch<NodeIDType> extends BasicReconfigurationPacket<NodeIDType> {

	public AckStartEpoch(NodeIDType initiator, String serviceName,
			int epochNumber, NodeIDType sender) {
		super(initiator, ReconfigurationPacket.PacketType.ACK_START_EPOCH, serviceName, epochNumber);
		this.setSender(sender);
	}
	public AckStartEpoch(JSONObject json) throws JSONException {
		super(json);
	}
}
