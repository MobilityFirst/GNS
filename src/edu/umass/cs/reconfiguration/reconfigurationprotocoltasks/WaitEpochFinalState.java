package edu.umass.cs.reconfiguration.reconfigurationprotocoltasks;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.protocoltask.ThresholdProtocolTask;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.Reconfigurator;
import edu.umass.cs.reconfiguration.reconfigurationpackets.AckStartEpoch;
import edu.umass.cs.reconfiguration.reconfigurationpackets.EpochFinalState;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RequestEpochFinalState;
import edu.umass.cs.reconfiguration.reconfigurationpackets.StartEpoch;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket.PacketType;
import edu.umass.cs.utils.MyLogger;

/**
 * @author V. Arun
 * @param <NodeIDType> 
 */
public class WaitEpochFinalState<NodeIDType>
		extends
		ThresholdProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String> {

	private static final long RESTART_PERIOD = 4000;

	private final StartEpoch<NodeIDType> startEpoch;
	private final AbstractReplicaCoordinator<NodeIDType> appCoordinator;
	private final RequestEpochFinalState<NodeIDType> reqState;
	private final Map<NodeIDType, String> notifiees = new ConcurrentHashMap<NodeIDType, String>();

	private Iterator<NodeIDType> prevGroupIterator;
	private boolean first = true;

	private final String key;

	private static final Logger log = (Reconfigurator.getLogger());

	/**
	 * @param myID
	 * @param startEpoch
	 * @param appCoordinator
	 */
	public WaitEpochFinalState(NodeIDType myID,
			StartEpoch<NodeIDType> startEpoch,
			AbstractReplicaCoordinator<NodeIDType> appCoordinator) {
		super(startEpoch.getPrevEpochGroup(), 1);
		this.startEpoch = startEpoch;
		this.appCoordinator = appCoordinator;
		this.prevGroupIterator = this.startEpoch.getPrevEpochGroup().iterator();
		this.reqState = new RequestEpochFinalState<NodeIDType>(myID,
				startEpoch.getPrevGroupName(),
				(startEpoch.getPrevEpochNumber()));
		this.key = this.refreshKey();
		this.setPeriod(RESTART_PERIOD);
		this.notifiees.put(this.startEpoch.getInitiator(),
				this.startEpoch.getKey());
	}

	// simply calls start() but only if state not yet received
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] restart() {
		if (this.amObviated()) {
			ProtocolExecutor.cancel(this);
			return null;
		}
		if (!this.prevGroupIterator.hasNext())
			this.prevGroupIterator = this.startEpoch.getPrevEpochGroup()
					.iterator();
		GenericMessagingTask<NodeIDType, ?>[] mtasks = start();
		if (mtasks != null)
			log.log(Level.INFO, MyLogger.FORMAT[2], new Object[] { getKey()
					, " resending request to " , mtasks[0].recipients[0] });
		return mtasks;
	}

	// Will try once from each prev node and then give up
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] start() {
		if (!this.prevGroupIterator.hasNext())
			return null;
		// Try myself first if I am in both old and new groups
		NodeIDType target = this.positionIterator();
		GenericMessagingTask<NodeIDType, ?> mtask = new GenericMessagingTask<NodeIDType, Object>(
				target, this.reqState);
		return mtask.toArray();
	}

	private NodeIDType positionIterator() {
		NodeIDType myID = this.appCoordinator.getMyID();
		// if contains me or not first time
		if (!this.startEpoch.getPrevEpochGroup().contains(myID) || !this.first
				|| (this.first = false))
			return this.prevGroupIterator.next();
		// else if contains me and first time
		while (this.prevGroupIterator.hasNext()
				&& !this.prevGroupIterator.next().equals(myID))
			;
		return myID; // leave iterator at self
	}

	private boolean amObviated() {
		Integer curEpoch = this.appCoordinator.getEpoch(this.startEpoch
				.getServiceName());
		if (curEpoch != null
				&& curEpoch - this.startEpoch.getEpochNumber() >= 0)
			return true;
		return false;
	}

	@Override
	public String refreshKey() {
		return (Reconfigurator.getTaskKey(getClass(), reqState,
				this.appCoordinator.getMyID().toString()) +
		// need different key for split/merge operations
		(!reqState.getServiceName().equals(this.startEpoch.getServiceName()) ? ":"
				+ this.startEpoch.getServiceName()
				+ ":"
				+ this.startEpoch.getEpochNumber()
				: ""));
	}

	protected static final ReconfigurationPacket.PacketType[] types = { ReconfigurationPacket.PacketType.EPOCH_FINAL_STATE };

	@Override
	public Set<PacketType> getEventTypes() {
		return new HashSet<ReconfigurationPacket.PacketType>(
				Arrays.asList(types));
	}

	@Override
	public String getKey() {
		return this.key;
	}

	@Override
	public boolean handleEvent(ProtocolEvent<PacketType, String> event) {
		ReconfigurationPacket.PacketType type = event.getType();
		if (type == null)
			return false;
		boolean handled = false;
		/*
		 * handleEvent returns true only if replica group creation succeeds.
		 * Note that replica group creation can fail because either
		 */
		switch (type) {
		case EPOCH_FINAL_STATE:
			@SuppressWarnings("unchecked")
			EpochFinalState<NodeIDType> state = (EpochFinalState<NodeIDType>) event;
			if (!checkEpochFinalState(event))
				break;
			log.log(Level.INFO, MyLogger.FORMAT[4],
					new Object[] { this, "received", state.getSummary(),
							"state=", state.getState() });
			handled = this.appCoordinator.createReplicaGroup(
					this.startEpoch.getServiceName(),
					this.startEpoch.getEpochNumber(), state.getState(),
					this.startEpoch.getCurEpochGroup());
			/*
			 * createReplicaGroup should always return true to indicate that it
			 * either succeeded in creating the group with the specified epoch
			 * number or higher and the supplied state. This should always be
			 * possible, if necessary as a blocking operation. So we invoke
			 */
			assert (handled);

		default:
			break;
		}
		return handled;
	}

	public String toString() {
		return this.getKey();
	}

	private boolean checkEpochFinalState(ProtocolEvent<PacketType, String> event) {
		// FIXME: What is there to check here other than the type?
		return true;
	}

	/**
	 * @param node
	 * @param key
	 * @return Node to be notified with AckStartEpoch when epoch final
	 *         state becomes ready.
	 */
	public synchronized String addNotifiee(NodeIDType node, String key) {
		return this.notifiees.put(node, key);
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleThresholdEvent(
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) {

		return this.getAckStarts();
	}

	/**
	 * @return Messaging tasks to be performed to send acks to notifiees.
	 */
	public GenericMessagingTask<NodeIDType, ?>[] getAckStarts() {
		Set<GenericMessagingTask<NodeIDType, AckStartEpoch<NodeIDType>>> mtasks = new HashSet<GenericMessagingTask<NodeIDType, AckStartEpoch<NodeIDType>>>();
		for (NodeIDType node : new HashSet<NodeIDType>(this.notifiees.keySet())) {

			AckStartEpoch<NodeIDType> ackStartEpoch = new AckStartEpoch<NodeIDType>(
					node, startEpoch.getServiceName(),
					startEpoch.getEpochNumber(), this.appCoordinator.getMyID());
			// need to explicitly set key as ackStart is going to different task
			ackStartEpoch.setKey(this.notifiees.get(node));
			mtasks.add(new GenericMessagingTask<NodeIDType, AckStartEpoch<NodeIDType>>(
					node, ackStartEpoch));
			log.log(Level.INFO, MyLogger.FORMAT[5], new Object[] { this,
					"sending", ackStartEpoch.getSummary(), "to RC" + node,
					"with key", this.notifiees.get(node) });
		}
		return mtasks.toArray(mtasks.iterator().next().toArray());
	}

}
