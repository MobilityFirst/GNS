package edu.umass.cs.reconfiguration.deprecated;

import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.protocoltask.SchedulableProtocolTask;
import edu.umass.cs.reconfiguration.Reconfigurator;
import edu.umass.cs.reconfiguration.RepliconfigurableReconfiguratorDB;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RCRecordRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket.PacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.ReconfigurationRecord;
import edu.umass.cs.reconfiguration.reconfigurationutils.ReconfigurationRecord.RCStates;
import edu.umass.cs.utils.ML;

/**
 * @author V. Arun
 * @param <NodeIDType>
 * 
 *            This protocol task is initiated at a reconfigurator in order to
 *            commit the completion of the reconfiguration, i.e., to change the
 *            state of the reconfiguration record to READY or to execute the
 *            actual deletion of the record. We need a task for this because
 *            simply invoking handleIncoming (that in turn calls paxos
 *            propose(.)) does not suffice to ensure that the command will be
 *            committed.
 */

public class WaitCoordinatedCommit<NodeIDType>
		implements
		SchedulableProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String> {

	/**
	 * FIXME: WaitCoordinatedCommit is a poor candidate for a protocol task as
	 * it has no messaging and should really not be even scheduled periodically.
	 * Instead, it needs an event-driven wait/notify scheme to check if it has
	 * been obviated and if not, after a timeout, re-issue the request. This
	 * could still be all done inside the start() method in protocoltask, but at
	 * that point using a protocoltask is pointless. Using periodic polling
	 * (with or without protocoltask) is a poor, slower implementation. It is
	 * also better to use a common request "key" across nodes here so that we
	 * can know via the executed() callback that the request has been executed
	 * irrespective of which node issued it. All issued RCRecordRequest requests
	 * must get executed at least once in order for a node to make progress; the
	 * only exception is PREV_DROPPED that is not essential to make progress.
	 */

	private final long RESTART_PERIOD = 1000;

	private final RCRecordRequest<NodeIDType> rcRecReq;
	private final RepliconfigurableReconfiguratorDB<NodeIDType> DB;

	private final String key;

	private static final Logger log = (Reconfigurator.getLogger());

	/**
	 * @param rcRecReq
	 * @param DB
	 */
	public WaitCoordinatedCommit(RCRecordRequest<NodeIDType> rcRecReq,
			RepliconfigurableReconfiguratorDB<NodeIDType> DB) {
		this.rcRecReq = rcRecReq;
		this.DB = DB;
		this.key = this.refreshKey();
		if (this.DB.isRCGroupName(rcRecReq.getServiceName()))
			this.DB.addRCTask(getKey());
	}

	// will keep calling start until obviated
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] restart() {
		if (amObviated())
			return killMyself();
		// else coordinate RC record request again
		log.log(Level.INFO,
				ML.F[3],
				new Object[] {
						this.refreshKey(),
						" re-proposing ",
						rcRecReq.getSummary(),
						(rcRecReq.isReconfigurationMerge() ? "; merging state = "
								+ rcRecReq.startEpoch.initialState
								: "") });
		this.DB.coordinateRequestSuppressExceptions(rcRecReq);
		return null;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] start() {
		/*
		 * Better to supply the paxos group explicitly here, otherwise it is
		 * possible for RepliconfigurableReconfiguratorDB to hash it to a
		 * different reconfigurator group if it is not aware of this node's
		 * existence at this point in time.
		 */

		if (this.isStartable())
			log.log(Level.INFO, "{0} ready to start coordinating {1}", new Object[] { this,
					this.rcRecReq.getSummary() });
		this.DB.coordinateRequestSuppressExceptions(rcRecReq);
		return null;
	}

	private GenericMessagingTask<NodeIDType, ?>[] killMyself() {
		this.DB.removeRCTask(getKey());
		ProtocolExecutor.cancel(this);
		return null;
	}

	/**
	 * @return The refreshed key.
	 */
	public String refreshKey() {
		return getCommitTaskKey(this.rcRecReq, this.DB.getMyID()
				.toString());
	}

	/*
	 * This method is relevant only for merges or splits, so it is relevant only
	 * for RC group changes.
	 */
	private boolean isStartable() {
		switch (rcRecReq.getRCRequestType()) {
		case RECONFIGURATION_MERGE:
			/*
			 * We need the merger group to be ready before the mergee can be
			 * merged.
			 */
			this.DB.waitReady(rcRecReq.getServiceName(),
					rcRecReq.getEpochNumber(), RESTART_PERIOD);
			break;
		case RECONFIGURATION_COMPLETE:
			if (this.rcRecReq.startEpoch.isSplit())
				/*
				 * Ideally we want to be notified right when the splittee group
				 * is stopped (via WaitAckStop), but there is no easy way to
				 * check the DB and know if a paxos group has been stopped
				 * without inelegant hacks.
				 */
				this.DB.waitReady(rcRecReq.startEpoch.getPrevGroupName(),
						rcRecReq.startEpoch.getPrevEpochNumber()+1, RESTART_PERIOD);
			break;
		default:
		}
		return true;
	}

	/*
	 * FIXME: Create an interface for amObviated() and pass it to this class'
	 * constructor.
	 * 
	 * FIXME: Double-check the flags here for bugs.
	 */
	private boolean amObviated() {
		ReconfigurationRecord<NodeIDType> record = this.DB
				.getReconfigurationRecord(rcRecReq.getServiceName());
		// check if reconfiguration complete is committed
		boolean completeCommit = (rcRecReq.isReconfigurationComplete() || rcRecReq
				.isReconfigurationPrevDropComplete()) && (record == null
		// has moved on
				|| (record.getEpoch() - rcRecReq.getEpochNumber() >= 0));

		// check if reconfiguration complete is not needed
		boolean isNotLocalRCGroup = rcRecReq.isReconfigurationComplete()
				&& this.DB.isRCGroupName(rcRecReq.getServiceName())
				&& !rcRecReq.startEpoch.curEpochGroup.contains(this.DB
						.getMyID());

		// check if reconfiguration intent is complete
		boolean isIntentCommitted = (rcRecReq.isReconfigurationIntent())
				&& (record == null
						|| (record.getEpoch() == rcRecReq.getEpochNumber() - 1 && record
								.getState().equals(RCStates.WAIT_ACK_STOP))
				// has moved on
				|| (record.getEpoch() - rcRecReq.getEpochNumber() >= 0));

		boolean isDeleteIntentCommitted = (rcRecReq.isDeleteIntent() && (record == null
				|| (record.getEpoch() == rcRecReq.getEpochNumber() && record
						.getState().equals(RCStates.WAIT_DELETE))
				// has moved on
				|| (record.getEpoch() - rcRecReq.getEpochNumber() > 0)
		// has moved on
		|| (record.getEpoch() - rcRecReq.getEpochNumber() > 0)));

		boolean isMerged = rcRecReq.isReconfigurationMerge()
		// merges relevant only for RC groups
				&& this.DB.isRCGroupName(rcRecReq.getServiceName())
				// same epoch and merged
				&& (record != null
						&& (record.getEpoch() == this.rcRecReq.getEpochNumber() && record
								.hasBeenMerged(rcRecReq.startEpoch
										.getPrevGroupName()))
				// has moved on to future epoch
				|| record.getEpoch() - rcRecReq.getEpochNumber() > 0);

		// defensive documentation via code
		boolean obviated = completeCommit || isNotLocalRCGroup
				|| isIntentCommitted || isMerged
				// || isMergeIntentCommitted
				|| isDeleteIntentCommitted;
		return obviated;
	}

	@Override
	public Set<PacketType> getEventTypes() {
		return new HashSet<ReconfigurationPacket.PacketType>();
	}

	@Override
	public String getKey() {
		return this.key;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleEvent(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) {
		return null;
	}

	public String toString() {
		return this.refreshKey();
	}

	@Override
	public long getPeriod() {
		return RESTART_PERIOD;
	}
	
	/**
	 * @param rcPacket
	 * @param myID
	 * @return The commit task key.
	 */
	public static String getCommitTaskKey(RCRecordRequest<?> rcPacket,
			String myID) {
		return getCommitTaskKey(rcPacket, rcPacket.getRCRequestType(), myID);
	}

	private static String getCommitTaskKey(RCRecordRequest<?> rcPacket,
			RCRecordRequest.RequestTypes reqType, String myID) {
		return Reconfigurator.getTaskKey(WaitCoordinatedCommit.class, rcPacket, myID)
				+ (rcPacket.startEpoch.isSplitOrMerge() ? ":"
						+ rcPacket.startEpoch.getPrevGroupName() + ":"
						+ rcPacket.startEpoch.getPrevEpochNumber() : "");
	}

}
