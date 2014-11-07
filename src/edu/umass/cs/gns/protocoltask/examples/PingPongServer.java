package edu.umass.cs.gns.protocoltask.examples;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.MessagingTask;
import edu.umass.cs.gns.nio.NIOTransport;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.protocoltask.ProtocolEvent;
import edu.umass.cs.gns.protocoltask.ProtocolTask;
import edu.umass.cs.gns.protocoltask.TESTProtocolTaskConfig;

/**
 * @author V. Arun
 */
public class PingPongServer implements ProtocolTask<Integer, Packet.PacketType, String> {

	private final String key = null;

	protected final Integer myID;
	private static Packet.PacketType[] types = {Packet.PacketType.TEST_PING};

	private Logger log =
			NIOTransport.LOCAL_LOGGER ? Logger.getLogger(getClass().getName())
					: GNS.getLogger();

	public PingPongServer(int id) {
		this.myID = id;
	}

	/*************************** Start of overridden methods *****************************************/
	@Override
	public String getKey() {
		return this.key;
	}

	@Override
	public String refreshKey() {
		return (this.myID.toString() +
				(int) (Math.random() * Integer.MAX_VALUE));
	}

	@Override
	public MessagingTask[] handleEvent(
			ProtocolEvent<Packet.PacketType, String> event,
			ProtocolTask<Integer, Packet.PacketType, String>[] ptasks) {

		PingPongPacket ppp = ((PingPongPacket) event.getMessage());
		MessagingTask mtask = null;
		switch (ppp.getType()) {
		case TEST_PING:
			mtask = handlePingPong(ppp);
			break;
		default:
			throw new RuntimeException("Unrecognizable message");
		}
		return mtask != null ? mtask.toArray() : null;
	}

	@Override
	public MessagingTask[] start() {
		return null;
	}

	/*************************** End of overridden methods *****************************************/

	/*************************** Private or testing methods below *********************************/
	private MessagingTask handlePingPong(PingPongPacket ppp) {
		return handlePing(ppp);
	}

	private MessagingTask handlePing(PingPongPacket ping) {
		int sender = ping.flip(this.myID);
		if (TESTProtocolTaskConfig.shouldDrop(ping.getCounter()))
			return null;
		log.info("Node" + myID + " pingpong server ponging to " + sender +
				": " + ping);
		return new MessagingTask(Integer.valueOf(sender), ping);
	}

	/************************************ Testing methods below **********************************/

	public static void main(String[] args) {
		// Not much to test here as this is an example of how to use protocol task
	}

	@Override
	public Set<Packet.PacketType> getEventTypes() {
		return new HashSet<Packet.PacketType>(Arrays.asList(types)); //types;
	}
}
