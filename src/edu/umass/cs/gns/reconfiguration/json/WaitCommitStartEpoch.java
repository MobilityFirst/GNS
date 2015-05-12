package edu.umass.cs.gns.reconfiguration.json;

import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import edu.umass.cs.gns.nio.GenericMessagingTask;
import edu.umass.cs.gns.protocoltask.ProtocolEvent;
import edu.umass.cs.gns.protocoltask.ProtocolExecutor;
import edu.umass.cs.gns.protocoltask.ProtocolTask;
import edu.umass.cs.gns.protocoltask.SchedulableProtocolTask;
import edu.umass.cs.gns.reconfiguration.Reconfigurator;
import edu.umass.cs.gns.reconfiguration.RepliconfigurableReconfiguratorDB;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.RCRecordRequest;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket.PacketType;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.ReconfigurationRecord;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.ReconfigurationRecord.RCStates;

/**
 * @author V. Arun
 */
/*
 * This protocol task is initiated at a reconfigurator in order to commit the
 * completion of the reconfiguration, i.e., to change the state of the
 * reconfiguration record to READY or to execute the actual deletion of the
 * record. We need a task for this because simply invoking handleIncoming (that
 * in turn calls paxos propose(.)) does not suffice to ensure that the command
 * will be committed.
 */
public class WaitCommitStartEpoch<NodeIDType>
		implements
		SchedulableProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String> {

	private final long RESTART_PERIOD = 1000;

	private final RCRecordRequest<NodeIDType> rcRecReq;
	private final RepliconfigurableReconfiguratorDB<NodeIDType> DB;

	private final String key;

	public static final Logger log = Logger.getLogger(Reconfigurator.class
			.getName());

	public WaitCommitStartEpoch(RCRecordRequest<NodeIDType> rcRecReq,
			RepliconfigurableReconfiguratorDB<NodeIDType> DB) {
		this.rcRecReq = rcRecReq;
		this.DB = DB;
		this.key = this.refreshKey();
		if(this.DB.isRCGroupName(rcRecReq.getServiceName()))
			this.DB.addRCTask(getKey());
	}

	// will keep restarting until explicitly removed by reconfigurator
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] restart() {
		if (!this.amObviated())
			return start();
		this.DB.removeRCTask(getKey());
		ProtocolExecutor.cancel(this);
		return null;
	}

	/*
	 * FIXME: Create an interface for amObviated() and pass it to this class'
	 * constructor.
	 */
	private boolean amObviated() {
		ReconfigurationRecord<NodeIDType> record = this.DB
				.getReconfigurationRecord(rcRecReq.getServiceName());
		// check if start epoch has been committed
		boolean obviated = rcRecReq.isReconfigurationComplete()
				&& (record == null || (record.getEpoch() == rcRecReq
						.getEpochNumber() && record.getState().equals(
						RCStates.READY)));

		// check if start epoch is not needed
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

		return obviated || isNotLocalRCGroup || intentCommitted;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] start() {
		if (amObviated())
			return null;
		// else coordinate RC record request
		rcRecReq.setNeedsCoordination(true); // need to set explicitly each time
		System.out.println(this.refreshKey() + " re-proposing "
				+ rcRecReq.getSummary());
		this.DB.handleIncoming(rcRecReq);
		return null;
	}

	@Override
	public String refreshKey() {
		return (this.getClass().getSimpleName() + rcRecReq.getRCRequestType()
				+ this.DB.getMyID() + ":" + this.rcRecReq.getServiceName()
				+ ":" + this.rcRecReq.getEpochNumber());
	}

	// empty as task does not expect any events and will be explicitly removed
	public static final ReconfigurationPacket.PacketType[] types = {};

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
		// TODO Auto-generated method stub
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
