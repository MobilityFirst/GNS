package edu.umass.cs.gns.reconfiguration;

import java.lang.reflect.InvocationTargetException;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nio.IntegerPacketType;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.AckDropEpochFinalState;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.AckStartEpoch;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.AckStopEpoch;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.DemandReport;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.RCRecordRequest;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.ReconfigurationRecord;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.ReconfigurationRecord.RCStates;
import edu.umass.cs.gns.util.MyLogger;
import edu.umass.cs.gns.util.Stringifiable;

/**
 * @author V. Arun
 */
/*
 * Need to add fault tolerance support via paxos here.
 */
public abstract class AbstractReconfiguratorDB<NodeIDType> implements
		InterfaceRepliconfigurable, InterfaceReconfiguratorDB<NodeIDType> {
	public static final ReconfigurationPacket.PacketType[] types = { };//ReconfigurationPacket.PacketType.RC_RECORD_REQUEST };

	protected final NodeIDType myID;
	protected final ConsistentReconfigurableNodeConfig<NodeIDType> consistentNodeConfig;

	public static final Logger log = Logger.getLogger(Reconfigurator.class
			.getName());

	public AbstractReconfiguratorDB(NodeIDType myID,
			ConsistentReconfigurableNodeConfig<NodeIDType> nc) {
		this.myID = myID;
		this.consistentNodeConfig = nc;
	}

	// default method definition if epoch is specified
	public ReconfigurationRecord<NodeIDType> getReconfigurationRecord(
			String name, int epoch) {
		ReconfigurationRecord<NodeIDType> record = this
				.getReconfigurationRecord(name);
		return record.getEpoch() == epoch ? record : null;
	}

	protected ReconfigurationRecord<NodeIDType> createRecord(String name) {
		ReconfigurationRecord<NodeIDType> record = null;
		record = new ReconfigurationRecord<NodeIDType>(name, 0,
				this.consistentNodeConfig.getReplicatedActives(name));
		return record;
	}

	protected Set<NodeIDType> getActiveReplicas(String name) {
		ReconfigurationRecord<NodeIDType> record = this
				.getReconfigurationRecord(name);
		return record.getActiveReplicas();
	}

	/***************** Paxos related methods below ***********/
	@SuppressWarnings("unchecked")
	@Override
	public boolean handleRequest(InterfaceRequest request,
			boolean doNotReplyToClient) {
		assert (request instanceof BasicReconfigurationPacket<?>); 
		// cast checked by assert above
		BasicReconfigurationPacket<NodeIDType> rcPacket = (BasicReconfigurationPacket<NodeIDType>) request;
		boolean handled = (Boolean) autoInvokeMethod(this, rcPacket,
				this.consistentNodeConfig);
		return handled;
	}

	protected static Object autoInvokeMethod(Object target,
			BasicReconfigurationPacket<?> rcPacket, Stringifiable<?> unstringer) {
		try {
			return target
					.getClass()
					.getMethod(
							ReconfigurationPacket.HANDLER_METHOD_PREFIX
									+ ReconfigurationPacket.getPacketTypeClassName(rcPacket
											.getType()),
							ReconfigurationPacket.getPacketTypeClass(rcPacket
									.getType())).invoke(target, rcPacket);
		} catch (NoSuchMethodException nsme) {
			nsme.printStackTrace();
		} catch (InvocationTargetException ite) {
			ite.printStackTrace();
		} catch (IllegalAccessException iae) {
			iae.printStackTrace();
		}
		return null;
	}

	public boolean handleDemandReport(DemandReport<NodeIDType> report) {
		return this.updateDemandStats(report);
	}

	public boolean handleAckStopEpoch(AckStopEpoch<NodeIDType> ackStopEpoch) {
		ReconfigurationRecord<NodeIDType> record = this
				.getReconfigurationRecord(ackStopEpoch.getServiceName());
		if (!this.isLegitTransition(record, ackStopEpoch.getEpochNumber() + 1,
				RCStates.WAIT_ACK_START))
			return false;
		return this.setState(ackStopEpoch.getServiceName(),
				ackStopEpoch.getEpochNumber() + 1, RCStates.WAIT_ACK_START);
	}

	public boolean handleAckStartEpoch(AckStartEpoch<NodeIDType> ackStartEpoch) {
		ReconfigurationRecord<NodeIDType> record = this
				.getReconfigurationRecord(ackStartEpoch.getServiceName());
		if (!this.isLegitTransition(record, ackStartEpoch.getEpochNumber(),
				RCStates.READY))
			return false;
		return this.setState(ackStartEpoch.getServiceName(),
				ackStartEpoch.getEpochNumber(), RCStates.READY);
	}

	/* This method exists only in case an ackDrop packet arrives
	 * after the corresponding task has terminated.
	 */
	public boolean handleAckDropEpochFinalState(
			AckDropEpochFinalState<NodeIDType> ackDropEpoch) {
		// no state change needed here by design
		return true;
	}

	public boolean handleCreateServiceName(CreateServiceName create) {
		return this.createRecord(create.getServiceName()) != null;
	}

	/*
	 * If a reconfiguration intent is being registered, a protocol task must be
	 * started that ensures that the reconfiguration completes successfully.
	 */
	public boolean handleRCRecordRequest(RCRecordRequest<NodeIDType> rcRecReq) {
		
		log.info("ARDB"+this.myID+" received RCRecordRequest " + rcRecReq);
		// create RC record upon a name creation request
		if (rcRecReq.startEpoch.isInitEpoch()
				&& this.getReconfigurationRecord(rcRecReq.getServiceName()) == null)
			this.createReconfigurationRecord(new ReconfigurationRecord<NodeIDType>(
					rcRecReq.getServiceName(), -1,
					rcRecReq.startEpoch.curEpochGroup));
		ReconfigurationRecord<NodeIDType> record = this
				.getReconfigurationRecord(rcRecReq.getServiceName());

		if (!this.isLegitTransition(rcRecReq, record))
			return false;

		if (rcRecReq.isReconfigurationIntent()) {
			// READY -> WAIT_ACK_STOP
			log.log(Level.INFO, MyLogger.FORMAT[9],
					new Object[] { this, "received", rcRecReq.getSummary(),
							"; changing state", rcRecReq.getServiceName(),
							record.getEpoch(), record.getState(), "->",
							rcRecReq.getEpochNumber()-1,
							ReconfigurationRecord.RCStates.WAIT_ACK_STOP });
			return this.setStateInitReconfiguration(rcRecReq.getServiceName(),
					rcRecReq.getEpochNumber() - 1,
					ReconfigurationRecord.RCStates.WAIT_ACK_STOP,
					rcRecReq.startEpoch.getCurEpochGroup(), rcRecReq.getInitiator());
		} else if (rcRecReq.isReconfigurationComplete()) {
			// WAIT_ACK_START -> READY
			log.log(Level.INFO, MyLogger.FORMAT[9],
					new Object[] { this, "received", rcRecReq.getSummary(),
							"; changing state", rcRecReq.getServiceName(),
							record.getEpoch(), record.getState(), "->",
							rcRecReq.getEpochNumber(),
							ReconfigurationRecord.RCStates.READY });
			return this.setState(rcRecReq.getServiceName(),
					rcRecReq.getEpochNumber(),
					ReconfigurationRecord.RCStates.READY);
		} else if (rcRecReq.isDeleteConfirmation()) {
			// WAIT_ACK_STOP -> DELETE
			log.log(Level.INFO, MyLogger.FORMAT[7],
					new Object[] { this, "received", rcRecReq.getSummary(),
							"; changing state", rcRecReq.getServiceName(),
							record.getEpoch(), record.getState(), "-> DELETE" });

			return this.deleteReconfigurationRecord(rcRecReq.getServiceName()) != null;
		} else
			throw new RuntimeException("Received unexpected RCRecordRequest");
	}

	public String toString() {
		return "RCDB" + myID;
	}

	/*
	 * doNotReplyToClient for this "app" is a no-op as it never replies to some
	 * "client". All messaging is done by a single reconfigurator node. The DB
	 * only reflects state changes.
	 */
	@Override
	public boolean handleRequest(InterfaceRequest request) {
		return this.handleRequest(request, false);
	}

	@SuppressWarnings("unchecked")
	@Override
	public InterfaceRequest getRequest(String stringified)
			throws RequestParseException {
		//throw new RuntimeException("Method should never have been called");
		BasicReconfigurationPacket<NodeIDType> rcPacket=null;
		try {
			rcPacket = (BasicReconfigurationPacket<NodeIDType>) ReconfigurationPacket
					.getReconfigurationPacket(new JSONObject(stringified),
							this.consistentNodeConfig);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return rcPacket;
	}

	/*
	 * The methods below that throw a runtime exception saying that they should
	 * have never been called are so because, with the current design, these
	 * methods are subsumed by Reconfigurator and never directly called. The
	 * current call chain is PacketDemultiplexer -> Reconfigurator ->
	 * RepliconfigurableReconfigurator.handleIncoming(.) ->
	 * this.handleRequest(.). The getRequest and getRequestTypes methods are
	 * only used for demultiplexing and the set of packet types of this class
	 * are a subset of those of Reconfigurator.
	 */

	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		throw new RuntimeException("Method should never have been called");
	}

	// FIXME: Replicable and Reconfigurable methods are unimplemented below.

	// Reconfigurable methods below
	@Override
	public InterfaceReconfigurableRequest getStopRequest(String name, int epoch) {
		throw new RuntimeException("Method not yet implemented");
	}

	@Override
	public String getFinalState(String name, int epoch) {
		throw new RuntimeException("Method not yet implemented");
	}

	@Override
	public void putInitialState(String name, int epoch, String state) {
		throw new RuntimeException("Method not yet implemented");
	}

	@Override
	public boolean deleteFinalState(String name, int epoch) {
		throw new RuntimeException("Method not yet implemented");
	}

	/*
	 * A transition using an RCRecordRequest is legitimate iff if takes a record
	 * in the same epoch from READY
	 */
	private boolean isLegitTransition(RCRecordRequest<NodeIDType> rcRecReq,
			ReconfigurationRecord<NodeIDType> record) {
		// always ignore lower epochs
		if (rcRecReq.getEpochNumber() - record.getEpoch() < 0)
			return false;
		/* We need to consider both ==1 and >1 for epoch numbers as this 
		 * particular node may have missed a few epochs. The received
		 * RC record must either initiate a reconfiguration or announce
		 * its completion even when this replica is waiting on an 
		 * ackStop for the preceding epoch (something that is rare
		 * during gracious execution but can happen if a secondary
		 * replica takes over and completes the reconfiguration while
		 * the primary is still waiting for the previous epoch to stop).
		 */
		if (rcRecReq.getEpochNumber() - record.getEpoch() >= 1) {
			// initiating reconfiguration to next epoch
			return
			// ready to reconfigure OR
			(record.getState().equals(RCStates.READY) && rcRecReq
					.isReconfigurationIntent()) ||
			// waiting on ackStop and reconfiguration complete (unlikely)
					(record.getState().equals(RCStates.WAIT_ACK_STOP) && (rcRecReq
							.isReconfigurationComplete() || rcRecReq.isDeleteConfirmation()));
			/*
			 * If a reconfiguration intent is allowed only from READY, we have a
			 * problem during recovery when reconfiguration completion is not
			 * automatically rolled forward. So reconfiguration initiations will
			 * fail because the current state won't be READY. Every
			 * reconfiguration from after the most recent checkpoint will have
			 * to be explicitly replayed again. One option is to allow illegitimate
			 * transitions during recovery.
			 */
		}
		/* In the same epoch, the only state change possible is by receiving an
		 * RC record announcing reconfiguration completion while waiting for a 
		 * majority ackStarts.
		 */
		if (rcRecReq.getEpochNumber() - record.getEpoch() == 0) {
			return
			// waiting on ackStart and reconfiguration complete
			(record.getState().equals(RCStates.WAIT_ACK_START) && rcRecReq
					.isReconfigurationComplete());
		}
		return false;
	}

	/*
	 * A legitimate state transition must either advance the state in the same
	 * epoch or advance the epoch.
	 */
	protected boolean isLegitTransition(
			ReconfigurationRecord<NodeIDType> record, int epoch, RCStates state) {
		if (epoch - record.getEpoch() < 0)
			return false;
		if (epoch - record.getEpoch() == 0) {
			return
			// WAIT_ACK_START -> READY
			(record.getState().equals(RCStates.WAIT_ACK_START) && state
					.equals(RCStates.READY)) ||
			// READY -> WAIT_ACK_DROP
					(record.getState().equals(RCStates.READY) && state
							.equals(RCStates.WAIT_ACK_DROP));
		}
		if (epoch - record.getEpoch() > 0) {
			// WAIT_ACK_STOP -> READY
			return record.getState().equals(RCStates.WAIT_ACK_STOP)
					|| record.getState().equals(RCStates.READY);
		}
		return false;
	}
}