package edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nio.IntegerPacketType;
import edu.umass.cs.gns.reconfiguration.InterfaceRequest;
import edu.umass.cs.gns.reconfiguration.RequestParseException;
import edu.umass.cs.gns.util.Stringifiable;

/**
@author V. Arun
 */
public class AckStopEpoch<NodeIDType> extends BasicReconfigurationPacket<NodeIDType> implements InterfaceRequest {

	public AckStopEpoch(NodeIDType sender, StopEpoch<NodeIDType> stopEpoch) {
		super(stopEpoch.getInitiator(), ReconfigurationPacket.PacketType.ACK_STOP_EPOCH, 
			stopEpoch.getServiceName(), stopEpoch.getEpochNumber());
		this.setKey(stopEpoch.getKey());
		this.setSender(sender);
	}
	public AckStopEpoch(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
		super(json, unstringer);
	}
	@Override
	public IntegerPacketType getRequestType() throws RequestParseException {
		return this.getType();
	}
}
