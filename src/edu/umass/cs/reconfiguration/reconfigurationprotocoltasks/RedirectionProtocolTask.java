package edu.umass.cs.reconfiguration.reconfigurationprotocoltasks;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.protocoltask.ThresholdProtocolTask;
import edu.umass.cs.reconfiguration.Reconfigurator;
import edu.umass.cs.reconfiguration.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ClientReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket.PacketType;

/**
 * @author arun
 *
 * @param <NodeIDType>
 * 
 *            This protocol task is used to redirect a client request to the
 *            appropriate reconfigurator if a client happens to send the request
 *            to an incorrect reconfigurator.
 */
public class RedirectionProtocolTask<NodeIDType>
		extends
		ThresholdProtocolTask<NodeIDType, BasicReconfigurationPacket.PacketType, String> {

	private static final int MAX_RESTARTS = 0;
	private static final long RESTART_PERIOD = 4000;

	private final ClientReconfigurationPacket request;
	private final Set<NodeIDType> nodes;
	private final NodeIDType myID;
	int numRestarts = 0;
	private ClientReconfigurationPacket response = null;

	private static final Logger log = Reconfigurator.getLogger();

	/**
	 * @param request
	 * @param nodes
	 * @param myID
	 */
	public RedirectionProtocolTask(ClientReconfigurationPacket request,
			Set<NodeIDType> nodes, NodeIDType myID) {
		super(nodes, 1);
		this.request = request;
		this.nodes = nodes;
		this.myID = myID;
		log.info(this + " spawned for request " + request);
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] restart() {
		if (this.numRestarts++ < MAX_RESTARTS)
			return start();
		else
			ProtocolExecutor.cancel(this);
		return null;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] start() {
		NodeIDType nextNode = getRandomNode();
		log.log(Level.INFO, "{0} sending {1} to {2}", new Object[] { this,
				this.request.getSummary(), nextNode });
		return new GenericMessagingTask<NodeIDType, ClientReconfigurationPacket>(
				nextNode, this.request).toArray();
	}

	@SuppressWarnings("unchecked")
	private NodeIDType getRandomNode() {
		return (NodeIDType) this.nodes.toArray()[(int) (Math.random() * this.nodes
				.size())];
	}

	ReconfigurationPacket.PacketType[] types = {
			ReconfigurationPacket.PacketType.CREATE_SERVICE_NAME,
			ReconfigurationPacket.PacketType.DELETE_SERVICE_NAME,
			ReconfigurationPacket.PacketType.REQUEST_ACTIVE_REPLICAS, };

	@Override
	public Set<PacketType> getEventTypes() {
		return new HashSet<PacketType>(Arrays.asList(types));
	}

	@Override
	public String getKey() {
		return this.getClass().getSimpleName() + myID + this.request.getSummary();
	}
	
	public String toString() {
		return this.getKey();
	}

	@Override
	public boolean handleEvent(ProtocolEvent<PacketType, String> event) {
		// event must be client reconfiguration packet with same name and type.
		if (!(event instanceof ClientReconfigurationPacket)
				|| !event.getType().equals(this.request.getType())
				|| !((ClientReconfigurationPacket) event).getServiceName()
						.equals(this.request.getServiceName()))
			return false;
		this.response = (ClientReconfigurationPacket) event;
		return true;
	}

	public long getPeriod() {
		return RESTART_PERIOD;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleThresholdEvent(
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) {
		// send to self so that reconfigurator can send to client
		return new GenericMessagingTask<NodeIDType, ClientReconfigurationPacket>(
				myID, this.response).toArray();

	}
}
