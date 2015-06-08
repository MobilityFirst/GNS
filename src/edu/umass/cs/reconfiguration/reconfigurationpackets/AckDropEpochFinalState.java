package edu.umass.cs.reconfiguration.reconfigurationpackets;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.InterfaceRequest;
import edu.umass.cs.nio.IntegerPacketType;
import edu.umass.cs.nio.Stringifiable;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

/**
@author V. Arun
 * @param <NodeIDType> 
 */
public class AckDropEpochFinalState<NodeIDType> extends BasicReconfigurationPacket<NodeIDType> implements InterfaceRequest{

	public AckDropEpochFinalState(NodeIDType sender, DropEpochFinalState<NodeIDType> dropEpoch) {
		super(dropEpoch.getInitiator(), ReconfigurationPacket.PacketType.ACK_DROP_EPOCH_FINAL_STATE, 
			dropEpoch.getServiceName(), dropEpoch.getEpochNumber());
		this.setKey(dropEpoch.getKey());
		this.setSender(sender);
	}
	public AckDropEpochFinalState(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
		super(json, unstringer);
	}
	@Override
	public IntegerPacketType getRequestType() throws RequestParseException {
		return this.getType();
	}
}
