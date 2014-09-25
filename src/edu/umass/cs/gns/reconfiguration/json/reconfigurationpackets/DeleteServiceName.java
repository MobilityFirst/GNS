package edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets;


import java.net.InetSocketAddress;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nio.JSONNIOTransport;

/**
@author V. Arun
 */
public class DeleteServiceName extends BasicReconfigurationPacket<String> {

	public DeleteServiceName(
			String initiator,
			String name, int epochNumber) {
		super(initiator, ReconfigurationPacket.PacketType.DELETE_SERVICE_NAME, name, epochNumber);
	}
	public DeleteServiceName(JSONObject json) throws JSONException {
		super(json);
		InetSocketAddress isa = JSONNIOTransport.getSenderAddress(json);
		this.setSender(isa.getAddress().toString()+":"+isa.getPort());
	}
}
