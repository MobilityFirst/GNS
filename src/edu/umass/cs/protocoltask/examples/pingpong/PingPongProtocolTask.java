package edu.umass.cs.protocoltask.examples.pingpong;

import java.util.Set;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.nioutils.MessagingTask;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.protocoltask.examples.PingPongPacket;
import edu.umass.cs.protocoltask.examples.PingPongServer;
import edu.umass.cs.protocoltask.json.ProtocolPacket;
import edu.umass.cs.utils.Util;

/**
 * @author V. Arun
 */

/*
 * This example waits for numPings ping-pongs form any node in a
 * specified set of nodes. Note that it does not wait for numPings
 * ping-pongs from *all* nodes in the set.
 */
public class PingPongProtocolTask extends PingPongServer {
	public static final int MAX_PINGS = 10;

	private String key = null;

	private final int[] nodes;
	private final int numPings;

	private Logger log = ProtocolExecutor.getLogger();

	public PingPongProtocolTask(int id, Set<Integer> nodes, int numPings) {
		super(id);
		this.nodes = Util.setToIntArray(nodes);
		this.numPings = numPings;
		log.info("Node" + myID + " constructing protocol task with nodeIDs " +
				nodes);
	}

	/*************************** Start of overridden methods *****************************************/
	@Override
	public String getKey() {
		return this.key;
	}

	@Override
	public String refreshKey() {
		return (this.key =
				(this.myID.toString() + (int) (Math.random() * Integer.MAX_VALUE)));
	}

	@Override
	public MessagingTask[] handleEvent(
			ProtocolEvent<PingPongPacket.PacketType, String> event,
			ProtocolTask<Integer, PingPongPacket.PacketType, String>[] ptasks) {

		JSONObject msg = null;
		try {
			msg = ((ProtocolPacket<?,?>) event.getMessage()).toJSONObject();
		} catch (JSONException je) {
			je.printStackTrace();
			return null;
		}
		MessagingTask mtask = null;
		try {
			switch (PingPongPacket.PacketType.intToType.get(JSONPacket.getPacketType(msg))) {
			case TEST_PONG:
				mtask = handlePingPong(new PingPongPacket(msg));
				break;
			default:
				throw new RuntimeException("Unrecognizable message type: " +
						JSONPacket.getPacketType(msg));
			}
		} catch (JSONException je) {
			je.printStackTrace();
		}
		return mtask != null ? mtask.toArray() : null;
	}

	@Override
	public MessagingTask[] start() {
		PingPongPacket ppp =
				new PingPongPacket(this.myID,
						PingPongPacket.PacketType.TEST_PING);
		log.info("Node" + myID + " starting protocol task with nodeIDs " +
				Util.arrayOfIntToString(nodes));
		return new MessagingTask(nodes, ppp).toArray();
	}

	/*************************** End of overridden methods *****************************************/

	/*************************** Private or testing methods below *********************************/
	private MessagingTask handlePingPong(PingPongPacket ppp) {
		assert (Integer.valueOf(ppp.getInitiator()) == this.myID);
		return handlePong(ppp);
	}

	private MessagingTask handlePong(PingPongPacket pong) {
		pong.incrCounter();
		int sender = Integer.valueOf(pong.flip(this.myID));
		log.info("Node" + myID + " protocol task received pong from " + sender +
				": " + pong);
		if (pong.getCounter() >= (this.numPings - 1))
			ProtocolExecutor.cancel(this); // throws exception
		return new MessagingTask(sender, pong);
	}

	public static void main(String[] args) {
		System.out.println("No unit test. Run ExampleNode instead.");
	}
}
