package edu.umass.cs.reconfiguration;


import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.nio.InterfacePacketDemultiplexer;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.utils.Util;

/**
@author V. Arun
 */
public class ReconfigurationPacketDemultiplexer extends
AbstractPacketDemultiplexer {
	
	private final InterfacePacketDemultiplexer packetDemux;

	public ReconfigurationPacketDemultiplexer() {
		this.packetDemux = null;
	}
	public ReconfigurationPacketDemultiplexer(InterfacePacketDemultiplexer pd) {
		assert(pd!=null);
		this.packetDemux = pd;
	}

	@Override
	public boolean handleJSONObject(JSONObject json) {
		assert(false);

		ReconfigurationPacket.PacketType type = null;
		try {
			type = ReconfigurationPacket.PacketType.intToType.get(
				JSONPacket.getPacketType(json));
			if(type==null || this.packetDemux==null) return false;
			Util.assertAssertionsEnabled();
			assert(false);
			return (this.packetDemux!= null ? this.packetDemux.handleJSONObject(json) :
				this.handleJSONObjectSuper(json));
		} catch(JSONException je) {
			je.printStackTrace();
		}
		return false;
	}

}
