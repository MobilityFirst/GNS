package edu.umass.cs.gns.protocoltask.examples.thresholdfetch;

import java.util.Set;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.GenericMessagingTask;
import edu.umass.cs.gns.nio.MessagingTask;
import edu.umass.cs.gns.nio.NIOTransport;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.nsdesign.packet.Packet.PacketType;
import edu.umass.cs.gns.protocoltask.ProtocolEvent;
import edu.umass.cs.gns.protocoltask.ProtocolExecutor;
import edu.umass.cs.gns.protocoltask.ProtocolTask;
import edu.umass.cs.gns.protocoltask.examples.PingPongPacket;
import edu.umass.cs.gns.protocoltask.json.ProtocolPacket;
import edu.umass.cs.gns.protocoltask.json.ThresholdProtocolTask;
import edu.umass.cs.gns.util.Util;

/**
@author V. Arun
 */

/* This protocol task waits for responses from a majority of nodes in 
 * the specified set. It instantiates ThresholdProtocolTask that 
 * requires implementing a "boolean handleEvent(event)" method that 
 * determines whether the response is valid or not. If it is valid,
 * ThresholdProtocolTask automatically marks the corresponding
 * sender as having responded and does not retry ("restart") the 
 * request to that node anymore.
 */
public class MajorityFetchProtocolTask extends ThresholdProtocolTask<Integer, Packet.PacketType, Long> {

	private Long key = null;

	private final int[] nodes;
	private final int myID;

	private Logger log =  NIOTransport.LOCAL_LOGGER ? Logger.getLogger(getClass().getName()) : GNS.getLogger();

	public MajorityFetchProtocolTask(int id, Set<Integer> nodes) {
		super(nodes, nodes.size()%2==0 ? nodes.size() : nodes.size()+1);
		this.nodes = Util.setToIntArray(nodes);
		this.myID = id;
	}

	/***************************Start of overridden methods *****************************************/
	@Override
	public Long getKey() {return this.key;}

	@Override
	public Long refreshKey() {
		return (this.key = ((long)this.myID)<<32 + (int)(Math.random()*Integer.MAX_VALUE));
	}

	@Override
	public boolean handleEvent(ProtocolEvent<Packet.PacketType,Long> event) {

		JSONObject msg = null;
		try {
			msg = ((ProtocolPacket)event.getMessage()).toJSONObject();
		} catch(JSONException je) {
			je.printStackTrace();
			return false;
		}
		boolean responded = false;
		try {
			switch(Packet.getPacketType(msg)) {
			case TEST_PONG:
				responded = handlePingPong(new PingPongPacket(msg));
				break;
			default:
				throw new RuntimeException("Unrecognizable message type: " + Packet.getPacketType(msg));
			}
		} catch(JSONException je) {
			je.printStackTrace();
		}
		return responded;
	}

	@Override
	public MessagingTask[] start() {
		PingPongPacket ppp = new PingPongPacket(this.myID, this.myID, Packet.PacketType.TEST_PING);
		log.info("Node"+myID+" starting protocol task with nodeIDs " + Util.arrayToString(nodes));
		return new MessagingTask(nodes, ppp).toArray();
	}
	
	@Override
	public GenericMessagingTask<Integer,?>[] handleThresholdEvent(
			ProtocolTask<Integer, PacketType, Long>[] ptasks) {
		ProtocolExecutor.cancel(this);
		return null;
	}

	@Override
	public MessagingTask[] restart() {
		return start();
	}
	/***************************End of overridden methods *****************************************/

	/***************************Private or testing methods below *********************************/
	private boolean handlePingPong(PingPongPacket ppp) {
		return handlePong(ppp);
	}
	private boolean handlePong(PingPongPacket pong) {
		assert(pong.getInitiator()==this.myID);
		pong.incrCounter();
		int sender = pong.flip(this.myID);
		log.info("Node"+myID+" protocol task received pong from " + sender + ": " + pong);
		return true;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("Not unit-testable. Run ExampleNode instead.");
	}
}
