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
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.Reconfigurator;
import edu.umass.cs.reconfiguration.RepliconfigurableReconfiguratorDB;
import edu.umass.cs.reconfiguration.reconfigurationpackets.AckDropEpochFinalState;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DropEpochFinalState;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RCRecordRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.StartEpoch;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket.PacketType;
import edu.umass.cs.utils.ML;

/**
 * @author V. Arun
 * @param <NodeIDType>
 */
/*
 * This protocol task is initiated at a reconfigurator to await a majority of
 * acknowledgments from active replicas for StopEpoch messages.
 */
public class WaitAckDropEpoch<NodeIDType>
		extends
		ThresholdProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String> {

	/*
	 * We use different restart times for service names and RC group names as RC
	 * group names may have much more variance in checkpoint transfer times.
	 * Setting these values too low or too high will not impact safety. However,
	 * too low values may result in inefficient, remote checkpoint transfers
	 * while too high values may result in a large number of pending tasks
	 * consuming memory.
	 */
	private static final long RESTART_PERIOD_SERVICE_NAMES = 4*WaitAckStopEpoch.RESTART_PERIOD;
	private static final long RESTART_PERIOD_RC_GROUP_NAMES = 64*WaitAckStopEpoch.RESTART_PERIOD;
	private static final int MAX_RESTARTS = 5;

	private final NodeIDType myID;
	private final DropEpochFinalState<NodeIDType> dropEpoch;
	private final StartEpoch<NodeIDType> startEpoch;
	private int numRestarts = 0;
	private final long creationTime = System.currentTimeMillis();

	private final String key;

	private static final Logger log = Reconfigurator.getLogger();

	/**
	 * @param startEpoch
	 * @param DB
	 *            The getAllActive() call is a hack to be able to finish deletes
	 *            quicker. This has safety violation risks upon name re-creation
	 *            and is meant only for instrumentation.
	 *            <p>
	 *            
	 *            FIXME: We should not drop the previous epoch final state for
	 *            split/merge RC group reconfigurations.
	 */
	public WaitAckDropEpoch(StartEpoch<NodeIDType> startEpoch,
			RepliconfigurableReconfiguratorDB<NodeIDType> DB) {
		super(startEpoch.noCurEpochGroup()
				&& ReconfigurationConfig.aggressiveDeletionsAllowed() ? DB
				.getAllActives() : startEpoch.getPrevEpochGroup());
		/*
		 * FIXME: dropEpoch service name should be previous group name for merge
		 * and not be done at all for non-merge RC group change operations.
		 */
		this.dropEpoch = new DropEpochFinalState<NodeIDType>(DB.getMyID(),
				(startEpoch.isMerge() ? startEpoch.getPrevGroupName()
						: startEpoch.getServiceName()),
				startEpoch.isMerge() ? startEpoch.getPrevEpochNumber()
						: startEpoch.getEpochNumber() - 1, false);

		this.startEpoch = startEpoch;

		this.myID = DB.getMyID();
		this.key = this.refreshKey();
		this.setPeriod(DB.isRCGroupName(startEpoch.getServiceName()) ? RESTART_PERIOD_RC_GROUP_NAMES
				: RESTART_PERIOD_SERVICE_NAMES);
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] restart() {
		// send DropEpoch to all new actives and await acks from all?
		log.log(Level.INFO, ML.F[2], new Object[] {
				this.refreshKey(),
				" (re-)sending[" + this.numRestarts + " of " + MAX_RESTARTS
						+ "] ", this.dropEpoch.getSummary() });
		if (// have something to drop in the first place
		this.startEpoch.hasPrevEpochGroup()
		// count limit on restarts for good garbage collection
				&& this.numRestarts++ < MAX_RESTARTS
				// time limit on restarts to prevent ghost final state deletion
				&& System.currentTimeMillis() - this.creationTime < ReconfigurationConfig
						.getMaxFinalStateAge())
			return (new GenericMessagingTask<NodeIDType, DropEpochFinalState<NodeIDType>>(
					this.startEpoch.getPrevEpochGroup().toArray(),
					this.dropEpoch)).toArray();
		// else
		ProtocolExecutor.cancel(this);
		return null;
	}

	/**
	 * We skip the first period before sending out drop epochs. It is good to
	 * delay it a bit as some replicas may still not be done acquiring the
	 * previous epoch final state and it is good for them to have the
	 * opportunity to do so from a previous epoch replica rather than a paxos
	 * peer replica in the new epoch. This delaying is not needed for safety as
	 * a majority of replicas must have already started the new epoch by the
	 * time a drop epoch task is spawned, so this (minority) replica can always
	 * get the current state through a checkpoint transfer from one of the
	 * majority peer replica in the new epoch in case all previous epoch final
	 * state has been garbage collected before it could start the new epoch.
	 */
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] start() {
		return null;
	}

	/**
	 * @return The refreshed key.
	 */
	public String refreshKey() {
		return Reconfigurator.getTaskKey(getClass(), dropEpoch,
				this.myID.toString());
	}

	/**
	 * Packet types handled.
	 */
	public static final ReconfigurationPacket.PacketType[] types = { ReconfigurationPacket.PacketType.ACK_DROP_EPOCH_FINAL_STATE };

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
		assert (event.getType()
				.equals(ReconfigurationPacket.PacketType.ACK_DROP_EPOCH_FINAL_STATE));
		log.log(Level.FINE, ML.F[2],
				new Object[] { this.refreshKey(), "received",
						((AckDropEpochFinalState<String>) event).getSummary() });
		return true;
	}

	/**
	 * We should delete the record currently pending deletion.
	 */
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleThresholdEvent(
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		log.log(Level.INFO,
				"{0} received all ACKs for {1}{2}",
				new Object[] {
						this.refreshKey(),
						dropEpoch.getSummary(),
						(this.startEpoch.noCurEpochGroup()
								|| this.startEpoch.isMerge() ? "; initiating DELETE_COMPLETE"
								: "") });
		// not safe to complete delete before MAX_FINAL_STATE_AGE
		if (this.startEpoch.noCurEpochGroup()
				&& !ReconfigurationConfig.aggressiveDeletionsAllowed())
			return null;
		// delete RC record if no current group
		RCRecordRequest<NodeIDType> rcRecReq = new RCRecordRequest<NodeIDType>(
				this.myID, this.startEpoch,
				RCRecordRequest.RequestTypes.RECONFIGURATION_PREV_DROPPED);
		return (new GenericMessagingTask<NodeIDType, Object>(this.myID,
				rcRecReq)).toArray();
	}
}
