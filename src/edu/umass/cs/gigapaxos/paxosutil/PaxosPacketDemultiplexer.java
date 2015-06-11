package edu.umass.cs.gigapaxos.paxosutil;

import edu.umass.cs.gigapaxos.PaxosManager;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.nio.JSONPacket;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * @author V. Arun
 *         <p>
 *         Used to get NIO to send paxos packets to PaxosManager. This class has
 *         been merged into PaxosManager now and will be soon deprecated.
 */
@SuppressWarnings("javadoc")
public class PaxosPacketDemultiplexer<NodeIDType> extends
		AbstractJSONPacketDemultiplexer {

	// private final PaxosManager<NodeIDType> paxosManager;

	public PaxosPacketDemultiplexer(PaxosManager<NodeIDType> pm) {
		// paxosManager = pm;
		this.register(PaxosPacket.PaxosPacketType.PAXOS_PACKET);
	}

	public boolean handleMessage(JSONObject jsonMsg) {
		boolean isPacketTypeFound = false;
		try {
			PaxosPacket.PaxosPacketType type = PaxosPacket.PaxosPacketType
					.getPaxosPacketType(JSONPacket.getPacketType(jsonMsg));
			if (type == null
					|| !type.equals(PaxosPacket.PaxosPacketType.PAXOS_PACKET))
				return false;
			throw new RuntimeException("This class should no longer be used");
			// paxosManager.handleIncomingPacket(jsonMsg);
			// return true;
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return isPacketTypeFound;
	}
}
