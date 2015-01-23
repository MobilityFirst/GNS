package edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets;


import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nio.IntegerPacketType;
import edu.umass.cs.gns.nio.JSONNIOTransport;
import edu.umass.cs.gns.reconfiguration.InterfaceReplicableRequest;
import edu.umass.cs.gns.reconfiguration.RequestParseException;
import edu.umass.cs.gns.util.Stringifiable;
import edu.umass.cs.gns.util.StringifiableDefault;

/**
@author V. Arun
 */
public class DeleteServiceName extends BasicReconfigurationPacket<InetSocketAddress> implements InterfaceReplicableRequest {

	public static final Stringifiable<InetSocketAddress> unstringer = new StringifiableDefault<InetSocketAddress>(
			new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));

	//private boolean coordType = false;
	
	public DeleteServiceName(
			InetSocketAddress initiator,
			String name, int epochNumber) {
		super(initiator, ReconfigurationPacket.PacketType.DELETE_SERVICE_NAME, name, epochNumber);
	}
	public DeleteServiceName(JSONObject json, Stringifiable<?> unstringer) throws JSONException {
		super(json, DeleteServiceName.unstringer); // ignores argument unstringer
		this.setSender(JSONNIOTransport.getSenderAddress(json));
	}
	public DeleteServiceName(JSONObject json) throws JSONException {
		this(json, unstringer);
	}
	@Override
	public IntegerPacketType getRequestType() throws RequestParseException {
		return ReconfigurationPacket.PacketType.DELETE_SERVICE_NAME;
	}
	@Override
	public boolean needsCoordination() {
		return false; //coordType;
	}
	@Override
	public void setNeedsCoordination(boolean b) {
		//coordType = b;
	} 
}
