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
	}

	// will keep restarting until explicitly removed by reconfigurator
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] restart() {
		if (this.amObviated())
			ProtocolExecutor.cancel(this);
		System.out.println(this.refreshKey() + " re-proposing "
				+ rcRecReq.getSummary());
		return start();
	}

	private boolean amObviated() {
		ReconfigurationRecord<NodeIDType> record = this.DB
				.getReconfigurationRecord(rcRecReq.getServiceName());
		return (record != null
				&& record.getEpoch() == rcRecReq.getEpochNumber() && record
				.getState().equals(RCStates.READY));
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] start() {
		// coordinate RC record request
		rcRecReq.setNeedsCoordination(true); // need to set this explicitly
		this.DB.handleIncoming(rcRecReq);
		return null;
	}

	@Override
	public String refreshKey() {
		return (this.getClass().getSimpleName() + this.DB.getMyID() + ":"
				+ this.rcRecReq.getServiceName() + ":" + this.rcRecReq
					.getEpochNumber());
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
		return this.getClass().getSimpleName() + this.DB.getMyID();
	}

	@Override
	public long getPeriod() {
		return RESTART_PERIOD;
	}
}
