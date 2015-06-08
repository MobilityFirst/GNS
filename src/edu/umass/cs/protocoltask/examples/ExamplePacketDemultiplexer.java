package edu.umass.cs.protocoltask.examples;

import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.protocoltask.ProtocolExecutor;

/**
 * @author V. Arun
 */
public class ExamplePacketDemultiplexer extends AbstractJSONPacketDemultiplexer {
	private static final boolean DEBUG = ProtocolExecutor.DEBUG;
	private final ExampleNode node;
	private Logger log =
			ProtocolExecutor.getLogger();

	ExamplePacketDemultiplexer(ExampleNode n) {
		this.node = n;
		this.register(PingPongPacket.PacketType.TEST_PING);
		this.register(PingPongPacket.PacketType.TEST_PONG);
	}

	@Override
	public boolean handleMessage(JSONObject json) {
		try {
			if (DEBUG)
				log.finest("PD " + this.node.getMyID() + " received " + json);
			switch (PingPongPacket.PacketType.intToType.get(JSONPacket.getPacketType(json))) {
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
