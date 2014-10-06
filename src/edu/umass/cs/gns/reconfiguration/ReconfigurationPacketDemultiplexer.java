package edu.umass.cs.gns.reconfiguration;


import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.gns.nio.InterfacePacketDemultiplexer;
import edu.umass.cs.gns.nio.JSONPacket;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket;

/**
@author V. Arun
 */
public class ReconfigurationPacketDemultiplexer extends
AbstractPacketDemultiplexer {
	
	private final InterfacePacketDemultiplexer packetDemux;

	public ReconfigurationPacketDemultiplexer(InterfacePacketDemultiplexer pd) {
		assert(pd!=null);
		this.packetDemux = pd;
	}

	@Override
	public boolean handleJSONObject(JSONObject json) {
		ReconfigurationPacket.PacketType type = null;
		try {
			type = ReconfigurationPacket.PacketType.intToType.get(
				JSONPacket.getPacketType(json));
		} catch(JSONException je) {
			je.printStackTrace();
		}
		if(type==null || this.packetDemux==null) return false;
		
		return this.packetDemux.handleJSONObject(json);
	}

}
