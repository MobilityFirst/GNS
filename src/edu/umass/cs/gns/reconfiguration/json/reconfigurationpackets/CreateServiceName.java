package edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets;


import java.net.InetSocketAddress;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nio.JSONNIOTransport;

/**
@author V. Arun
 */
public class CreateServiceName extends BasicReconfigurationPacket<String> {

	public static enum Keys {INITIAL_STATE};
	
	public CreateServiceName(
			String initiator,
			String name, int epochNumber, String state) {
		super(initiator, ReconfigurationPacket.PacketType.CREATE_SERVICE_NAME, name, epochNumber);
	}
	public CreateServiceName(JSONObject json) throws JSONException {
		super(json);
		InetSocketAddress isa = JSONNIOTransport.getSenderAddress(json);
		this.setSender(isa.getAddress().toString()+":"+isa.getPort());
	}
}
