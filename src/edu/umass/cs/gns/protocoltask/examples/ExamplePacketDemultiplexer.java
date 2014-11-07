package edu.umass.cs.gns.protocoltask.examples;

import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.gns.nio.NIOTransport;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.protocoltask.ProtocolExecutor;

/**
 * @author V. Arun
 */
public class ExamplePacketDemultiplexer extends AbstractPacketDemultiplexer {
	private static final boolean DEBUG = ProtocolExecutor.DEBUG;
	private final ExampleNode node;
	private Logger log =
			NIOTransport.LOCAL_LOGGER ? Logger.getLogger(NIOTransport.class.getName())
					: GNS.getLogger();

	ExamplePacketDemultiplexer(ExampleNode n) {
		this.node = n;
		this.register(Packet.PacketType.TEST_PING);
		this.register(Packet.PacketType.TEST_PONG);
	}

	@Override
	public boolean handleJSONObject(JSONObject json) {
		try {
			if (DEBUG)
				log.finest("PD " + this.node.getMyID() + " received " + json);
			switch (Packet.getPacketType(json)) {
			case TEST_PING:
			case TEST_PONG:
				this.node.handleIncoming(json);
				break;
			default:
				return false;
			}
		} catch (JSONException je) {
			je.printStackTrace();
		}
		return true;
	}

}
