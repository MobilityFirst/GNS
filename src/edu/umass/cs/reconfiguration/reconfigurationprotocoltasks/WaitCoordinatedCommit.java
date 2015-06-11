package edu.umass.cs.reconfiguration.reconfigurationprotocoltasks;

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
import edu.umass.cs.utils.MyLogger;

/**
 * @author V. Arun
 * @param <NodeIDType> 
 * 
 * This protocol task is initiated at a reconfigurator in order to commit the
 * completion of the reconfiguration, i.e., to change the state of the
 * reconfiguration record to READY or to execute the actual deletion of the
 * record. We need a task for this because simply invoking handleIncoming (that
 * in turn calls paxos propose(.)) does not suffice to ensure that the command
 * will be committed.
 */
public class WaitCoordinatedCommit<NodeIDType>
		implements
		SchedulableProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String> {

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

	// will keep restarting until explicitly removed by reconfigurator
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] restart() {
		if (amObviated())
			return killMyself();
		// else coordinate RC record request
		log.log(Level.INFO,
				MyLogger.FORMAT[3],
				new Object[] { this.refreshKey()
						, " re-proposing "
						, rcRecReq.getSummary()
						, (rcRecReq.isReconfigurationMerge() ? "; merging state = "
								+ rcRecReq.startEpoch.initialState
								: "") + "\n" });
		// rcRecReq.setNeedsCoordination(true); // need to set explicitly each
		// time
		this.DB.coordinateRequestSuppressExceptions(rcRecReq);
		return null;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] start() {
		// start method must be brief, cannot call handleIncoming here
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
		return Reconfigurator.getCommitTaskKey(this.rcRecReq, this.DB.getMyID()
				.toString())
				+ (this.rcRecReq.startEpoch.isMerge() ? ":"
						+ this.rcRecReq.startEpoch.getPrevGroupName() + ":"
						+ this.rcRecReq.getEpochNumber() : "");
	}

	/*
	 * FIXME: Create an interface for amObviated() and pass it to this class'
	 * constructor.
	 */
	private boolean amObviated() {
		ReconfigurationRecord<NodeIDType> record = this.DB
				.getReconfigurationRecord(rcRecReq.getServiceName());
		// check if reconfiguration complete is committed
		boolean completeCommit = (rcRecReq.isReconfigurationComplete() || rcRecReq
				.isDeleteConfirmation())
				&& (record == null || ((record.getEpoch()
						- rcRecReq.getEpochNumber() >= 0) && record.getState()
						.equals(RCStates.READY)));

		// check if reconfiguration complete is not needed
		boolean isNotLocalRCGroup = rcRecReq.isReconfigurationComplete()
				&& this.DB.isRCGroupName(rcRecReq.getServiceName())
				&& !rcRecReq.startEpoch.curEpochGroup.contains(this.DB
						.getMyID());

		// check if reconfiguration intent is complete
		boolean intentCommitted = rcRecReq.isReconfigurationIntent()
				&& this.DB.isRCGroupName(rcRecReq.getServiceName())
				&& (record == null
						|| (record.getEpoch() == rcRecReq.getEpochNumber() - 1 && record
								.getState().equals(RCStates.WAIT_ACK_STOP)) || (record
						.getEpoch() - rcRecReq.getEpochNumber() >= 0));

		boolean isMerged = rcRecReq.isReconfigurationMerge()
				&& this.DB.isRCGroupName(rcRecReq.getServiceName())
				&& (record != null && record.mergedContains(rcRecReq.startEpoch
						.getPrevGroupName()));

		// defensive documentation via code
		boolean obviated = completeCommit || isNotLocalRCGroup
				|| intentCommitted || isMerged;
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
}
