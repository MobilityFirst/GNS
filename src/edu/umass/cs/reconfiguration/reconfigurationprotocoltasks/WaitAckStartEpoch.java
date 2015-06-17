package edu.umass.cs.reconfiguration.reconfigurationprotocoltasks;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.protocoltask.ThresholdProtocolTask;
import edu.umass.cs.reconfiguration.Reconfigurator;
import edu.umass.cs.reconfiguration.RepliconfigurableReconfiguratorDB;
import edu.umass.cs.reconfiguration.reconfigurationpackets.AckStartEpoch;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RCRecordRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.StartEpoch;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket.PacketType;
import edu.umass.cs.utils.MyLogger;

/**
 * @author V. Arun
 * @param <NodeIDType>
 */
/*
 * This protocol task is initiated at a reconfigurator to await a majority of
 * acknowledgments for StartEpoch messages from active replicas.
 */
public class WaitAckStartEpoch<NodeIDType>
		extends
		ThresholdProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String> {

	private static final long RESTART_PERIOD = 8000;

	private final StartEpoch<NodeIDType> startEpoch;
	private final RepliconfigurableReconfiguratorDB<NodeIDType> DB;

	private boolean done = false;
	private final String key;

	private static final Logger log = Reconfigurator.getLogger();

	/**
	 * @param startEpoch
	 * @param DB
	 */
	public WaitAckStartEpoch(StartEpoch<NodeIDType> startEpoch,
			RepliconfigurableReconfiguratorDB<NodeIDType> DB) {
		super(
				startEpoch.getCurEpochGroup(),
				(!startEpoch.isMerge() ? startEpoch.getCurEpochGroup().size() / 2 + 1
						: 1));
		// need to recreate start epoch just to set initiator to self
		this.startEpoch = new StartEpoch<NodeIDType>(DB.getMyID(),
				startEpoch.getServiceName(), startEpoch.getEpochNumber(),
				startEpoch.curEpochGroup, startEpoch.prevEpochGroup,
				startEpoch.prevGroupName, startEpoch.isMerge,
				startEpoch.prevEpoch, startEpoch.creator,
				startEpoch.initialState, startEpoch.newlyAddedNodes);
		this.DB = DB;
		this.key = this.refreshKey();
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] restart() {
		log.log(Level.INFO, MyLogger.FORMAT[2],
				new Object[] { this.refreshKey(), " re-starting ",
						this.startEpoch.getSummary() });
		return start();
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] start() {
		/*
		 * Send StartEpoch to all new actives and await a majority. But if the
		 * startEpoch is a merge request, we only need to sequentially send
		 * startEpoch requests and it suffices to get an ack from one.
		 * 
		 * Should send startEpoch only to self in case of merge operations.
		 */
		assert (!this.startEpoch.isMerge() || this.startEpoch.curEpochGroup
				.contains(this.DB.getMyID()));
		GenericMessagingTask<NodeIDType, StartEpoch<NodeIDType>> mtask = !this.startEpoch
				.isMerge() ? new GenericMessagingTask<NodeIDType, StartEpoch<NodeIDType>>(
				this.startEpoch.getCurEpochGroup().toArray(), this.startEpoch)
				: new GenericMessagingTask<NodeIDType, StartEpoch<NodeIDType>>(
						this.DB.getMyID(), this.startEpoch);
		return mtask.toArray();
	}

	/**
	 * @return The refreshed key.
	 */
	public String refreshKey() {
		return Reconfigurator.getTaskKey(getClass(), startEpoch, this.DB
				.getMyID().toString());
	}

	/**
	 * Packet types handled.
	 */
	public static final ReconfigurationPacket.PacketType[] types = { ReconfigurationPacket.PacketType.ACK_START_EPOCH, };

	@Override
	public Set<PacketType> getEventTypes() {
		return new HashSet<ReconfigurationPacket.PacketType>(
				Arrays.asList(types));
	}

	@Override
	public String getKey() {
		return this.key;
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean handleEvent(ProtocolEvent<PacketType, String> event) {
		assert (event.getType().equals(types[0]));
		AckStartEpoch<NodeIDType> ackStart = ((AckStartEpoch<NodeIDType>) event);
		if (isDone())
			log.log(Level.INFO, MyLogger.FORMAT[3], new Object[] { this,
					"successfully processed", event.getType(),
					"after being done" });
		else
			log.log(Level.INFO, MyLogger.FORMAT[2],
					new Object[] { this, "received", ackStart.getSummary(),
							"from", ackStart.getSender() });

		return !isDone();
	}

	/*
	 * FIXME: shoud maybe spawn WaitAckDropEoch only after all new actives have
	 * sent acked start epoch as opposed to just a majority. An alternative is
	 * to delay WaitAckDropEoch a bit to allow all new actives to have acked
	 * starting the new epoch.
	 */
	// Send dropEpoch when startEpoch is acked by majority
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleThresholdEvent(
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {

		log.log(Level.INFO, MyLogger.FORMAT[3], new Object[] {
				this,
				" received MAJORITY ACKs for ",
				startEpoch.getSummary(),
				(this.startEpoch.creator != null ? "; sending ack to client "
						+ this.startEpoch.creator : "") });

		this.setDone(true);
		// multicast start epoch confirmation message
		RCRecordRequest<NodeIDType> rcRecReq = new RCRecordRequest<NodeIDType>(
				this.DB.getMyID(), this.startEpoch,
				RCRecordRequest.RequestTypes.RECONFIGURATION_COMPLETE);
		GenericMessagingTask<NodeIDType, ?> epochStartCommit = new GenericMessagingTask(
				this.DB.getMyID(), rcRecReq);

		if (this.startEpoch.hasPrevEpochGroup()) { // need to drop prev epoch
			ptasks[0] = new WaitAckDropEpoch<NodeIDType>(this.startEpoch,
					this.DB);
		} else if (this.startEpoch.creator != null) { // creation epoch
			/*
			 * We used to send creation confirmation to client here earlier, but
			 * it is best to do it using
			 * Reconfigurator.sendCreateConfirmationToClient as the client
			 * facing messenger may in general be different.
			 */
		} else
			assert (false);

		// propagate start epoch confirmation to all
		return epochStartCommit.toArray();
		// default action will do timed cancel
	}

	private synchronized boolean isDone() {
		return done;
	}

	private void setDone(boolean b) {
		this.done = b;
	}

	public String toString() {
		return this.getKey();
	}

	public long getPeriod() {
		return RESTART_PERIOD;
	}
}
