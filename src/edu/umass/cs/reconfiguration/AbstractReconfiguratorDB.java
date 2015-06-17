package edu.umass.cs.reconfiguration;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.InterfaceRequest;
import edu.umass.cs.nio.IntegerPacketType;
import edu.umass.cs.nio.Stringifiable;
import edu.umass.cs.reconfiguration.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DemandReport;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RCRecordRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.StopEpoch;
import edu.umass.cs.reconfiguration.reconfigurationutils.ConsistentHashing;
import edu.umass.cs.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.ReconfigurationRecord;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.reconfiguration.reconfigurationutils.ReconfigurationRecord.RCStates;
import edu.umass.cs.utils.MyLogger;

/**
 * @author V. Arun
 * @param <NodeIDType>
 */
/*
 * Need to add fault tolerance support via paxos here.
 */
public abstract class AbstractReconfiguratorDB<NodeIDType> implements
		InterfaceRepliconfigurable, InterfaceReconfiguratorDB<NodeIDType> {

	/**
	 * Constant RC record name keys. Currently there is only one, for the set of
	 * all reconfigurators.
	 */
	public static enum RecordNames {
		/**
		 * The record key for the RC record holding the set of all
		 * reconfigurators. This is used to reconfigure the set of all
		 * reconfigurators just like a typical RC record is used to reconfigure
		 * service names.
		 */
		NODE_CONFIG
	};

	private final ArrayList<String> pendingRCProtocolTasks = new ArrayList<String>();
	protected final NodeIDType myID;
	protected final ConsistentReconfigurableNodeConfig<NodeIDType> consistentNodeConfig;

	private static final Logger log = (Reconfigurator.getLogger());

	/**
	 * @param myID
	 * @param nc
	 */
	public AbstractReconfiguratorDB(NodeIDType myID,
			ConsistentReconfigurableNodeConfig<NodeIDType> nc) {
		this.myID = myID;
		this.consistentNodeConfig = nc;
	}

	/**
	 * @param name
	 * @param epoch
	 * @return ReconfigurationRecord for {@code name:epoch}.
	 */
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
		log.info(this + " executing " + request);
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

	/**
	 * @param report
	 * @return True if demand report is handled successfully. False means 
	 * that it may not have been processed. 
	 */
	public boolean handleDemandReport(DemandReport<NodeIDType> report) {
		return this.updateDemandStats(report);
	}

	/**
	 * If a reconfiguration intent is being registered, a protocol task must be
	 * started that ensures that the reconfiguration completes successfully.
	 * @param rcRecReq 
	 * @return True if the record was handled successfully.
	 */
	public boolean handleRCRecordRequest(RCRecordRequest<NodeIDType> rcRecReq) {

		// create RC record upon a name creation request
		if (rcRecReq.startEpoch.isInitEpoch()
				&& this.getReconfigurationRecord(rcRecReq.getServiceName()) == null)
			this.createReconfigurationRecord(new ReconfigurationRecord<NodeIDType>(
					rcRecReq.getServiceName(), rcRecReq.startEpoch
							.getEpochNumber() - 1,
					rcRecReq.startEpoch.curEpochGroup));
		ReconfigurationRecord<NodeIDType> record = this
				.getReconfigurationRecord(rcRecReq.getServiceName());
		assert (record != null);

		log.info("ARDB" + this.myID + " received RCRecordRequest " + rcRecReq
				+ "\n rcRecord = " + record);

		// verify legitimate transition and legitimate node config change
		if (!this.isLegitTransition(rcRecReq, record)
				|| !this.isLegitimateNodeConfigChange(rcRecReq, record))
			return false;

		// wait till node config change is complete
		if (rcRecReq.isNodeConfigChange()
				&& rcRecReq.isReconfigurationComplete()) {
			// should not be here at node config creation time
			assert (!rcRecReq.startEpoch.getPrevEpochGroup().isEmpty());
			// wait for all local RC groups to be up to date
			this.selfWait();
			// delete lower node config versions from node config table
			this.garbageCollectOldReconfigurators(rcRecReq.getEpochNumber() - 1);
			// garbage collect soft socket address mappings for deleted RC nodes
			this.consistentNodeConfig.removeSlatedForRemoval();
			System.out.println(this + " NODE_CONFIG change complete");
		}

		boolean handled = false;
		if (rcRecReq.isReconfigurationIntent()) {
			// READY -> WAIT_ACK_STOP
			log.log(Level.INFO, MyLogger.FORMAT[10],
					new Object[] { this, "received", rcRecReq.getSummary(),
							"; changing state", rcRecReq.getServiceName(),
							record.getEpoch(), record.getState(), "->",
							rcRecReq.getEpochNumber() - 1,
							ReconfigurationRecord.RCStates.WAIT_ACK_STOP,
							rcRecReq.startEpoch.getCurEpochGroup() });
			handled = this.setStateInitReconfiguration(
					rcRecReq.getServiceName(), rcRecReq.getEpochNumber() - 1,
					ReconfigurationRecord.RCStates.WAIT_ACK_STOP,
					rcRecReq.startEpoch.getCurEpochGroup(),
					rcRecReq.getInitiator());
		} else if (rcRecReq.isReconfigurationComplete()) {
			// WAIT_ACK_START -> READY
			log.log(Level.INFO, MyLogger.FORMAT[9],
					new Object[] { this, "received", rcRecReq.getSummary(),
							"; changing state", rcRecReq.getServiceName(),
							record.getEpoch(), record.getState(), "->",
							rcRecReq.getEpochNumber(),
							ReconfigurationRecord.RCStates.READY });
			handled = this.setState(rcRecReq.getServiceName(),
					rcRecReq.getEpochNumber(),
					ReconfigurationRecord.RCStates.READY);
			// notify to wake up node config completion wait
			if (this.isRCGroupName(record.getName())) {
				log.info(this
						+ " selfNotifying upon completing reconfiguration of "
						+ rcRecReq.getServiceName()
						+ (this.isNodeConfigChangeComplete() ? " **all done**"
								: ""));
				selfNotify();
			}
		} else if (rcRecReq.isDeleteConfirmation()) {
			// WAIT_ACK_STOP -> DELETE
			log.log(Level.INFO, MyLogger.FORMAT[7],
					new Object[] { this, "received", rcRecReq.getSummary(),
							"; changing state", rcRecReq.getServiceName(),
							record.getEpoch(), record.getState(), "-> DELETE" });

			handled = this.deleteReconfigurationRecord(
					rcRecReq.getServiceName(), rcRecReq.getEpochNumber() - 1);
		} else if (rcRecReq.isReconfigurationMerge()) {
			// MERGE
			log.log(Level.INFO,
					MyLogger.FORMAT[9],
					new Object[] { this, "received", rcRecReq.getSummary(),
							"; merging state",
							rcRecReq.startEpoch.getPrevGroupName(),
							rcRecReq.startEpoch.getPrevEpochNumber(), "into",
							rcRecReq.getServiceName(), record.getEpoch(),
							record.getState() });
			handled = this.mergeState(rcRecReq.getServiceName(),
					rcRecReq.getEpochNumber(),
					rcRecReq.startEpoch.getPrevGroupName(),
					rcRecReq.startEpoch.initialState);
		} else
			throw new RuntimeException("Received unexpected RCRecordRequest");
		log.info(this + " returning " + handled + " for "
				+ rcRecReq.getSummary());
		return handled;
	}

	/*
	 * Checks that oldGroup is current group and newGroup differs from old by
	 * exactly one node.
	 */
	private boolean isLegitimateNodeConfigChange(
			RCRecordRequest<NodeIDType> rcRecReq,
			ReconfigurationRecord<NodeIDType> record) {
		if (!rcRecReq.getServiceName().equals(
				RecordNames.NODE_CONFIG.toString()))
			return true;
		boolean consistent = rcRecReq.startEpoch.getPrevEpochGroup().equals(
				record.getActiveReplicas());
		Set<NodeIDType> oldGroup = rcRecReq.startEpoch.getPrevEpochGroup();
		Set<NodeIDType> newGroup = rcRecReq.startEpoch.getCurEpochGroup();
		consistent = consistent && differByOne(oldGroup, newGroup);
		return consistent;
	}

	private boolean differByOne(Set<NodeIDType> s1, Set<NodeIDType> s2) {
		return (s1.containsAll(s2) && (s1.size() == (s2.size() + 1)))
				|| (s2.containsAll(s1) && (s2.size() == (s1.size() + 1)));
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
		// throw new RuntimeException("Method should never have been called");
		BasicReconfigurationPacket<NodeIDType> rcPacket = null;
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
	 * Some methods below that throw a runtime exception saying that they should
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
		return new HashSet<IntegerPacketType>(); // empty
	}


	// Reconfigurable methods below
	@Override
	public InterfaceReconfigurableRequest getStopRequest(String name, int epoch) {
		StopEpoch<NodeIDType> stop = new StopEpoch<NodeIDType>(this.getMyID(),
				name, epoch);
		assert (stop instanceof InterfaceReplicableRequest);
		return stop;
	}

	@Override
	public String getFinalState(String name, int epoch) {
		return null;
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
		assert (record != null) : rcRecReq;
		// always ignore lower epochs
		if (rcRecReq.getEpochNumber() - record.getEpoch() < 0)
			return false;
		/*
		 * We need to consider both ==1 and >1 for epoch numbers as this
		 * particular node may have missed a few epochs. The received RC record
		 * must either initiate a reconfiguration or announce its completion
		 * even when this replica is waiting on an ackStop for the preceding
		 * epoch (something that is rare during gracious execution but can
		 * happen if a secondary replica takes over and completes the
		 * reconfiguration while the primary is still waiting for the previous
		 * epoch to stop).
		 */
		if (rcRecReq.getEpochNumber() - record.getEpoch() >= 1) {
			// initiating reconfiguration to next epoch
			return
			// ready to reconfigure OR
			(record.getState().equals(RCStates.READY) && rcRecReq
					.isReconfigurationIntent()) ||
			// waiting on ackStop and reconfiguration complete (unlikely)
					(record.getState().equals(RCStates.WAIT_ACK_STOP) && (rcRecReq
							.isReconfigurationComplete() || rcRecReq
							.isDeleteConfirmation()));
			/*
			 * If a reconfiguration intent is allowed only from READY, we have a
			 * problem during recovery when reconfiguration completion is not
			 * automatically rolled forward. So reconfiguration initiations will
			 * fail because the current state won't be READY. Every
			 * reconfiguration from after the most recent checkpoint will have
			 * to be explicitly replayed again. One option is to allow
			 * illegitimate transitions during recovery.
			 */
		}
		/*
		 * In the same epoch, the only state change possible is by receiving an
		 * RC record announcing reconfiguration completion while waiting for a
		 * majority ackStarts.
		 */
		if (rcRecReq.getEpochNumber() - record.getEpoch() == 0) {
			return
			// waiting on ackStart and reconfiguration complete
			(record.getState().equals(RCStates.WAIT_ACK_START) && rcRecReq
					.isReconfigurationComplete())
					|| (record.getState().equals(RCStates.READY) && rcRecReq
							.isReconfigurationMerge());
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

	/*
	 * Checks if all new RC groups are ready. FIXME: This check should be done
	 * atomically, and we also need to check for the new NC group itself
	 * existing.
	 */
	private boolean isNodeConfigChangeComplete() {
		Map<String, Set<NodeIDType>> newRCGroups = this.getNewRCGroups();
		boolean complete = true;
		ReconfigurationRecord<NodeIDType> ncRecord = this
				.getReconfigurationRecord(AbstractReconfiguratorDB.RecordNames.NODE_CONFIG
						.toString());
		for (String newRCGroup : newRCGroups.keySet()) {
			ReconfigurationRecord<NodeIDType> record = this
					.getReconfigurationRecord(newRCGroup);
			complete = complete
					&& record.getState().equals(RCStates.READY)
					&& record.getActiveReplicas().equals(
							newRCGroups.get(newRCGroup))
					&& ((record.getEpoch() == ncRecord.getRCEpoch(newRCGroup)));
			if (!complete)
				break;
		}
		if (!complete)
			log.info(this + " does not have all RC group records ready yet");
		complete = complete && this.isPendingRCTasksEmpty();
		if (!this.isPendingRCTasksEmpty())
			log.info(this + " has pending RC tasks: "
					+ this.pendingRCProtocolTasks);
		return complete;
	}

	protected boolean isBeingAdded(String newRCGroup) {
		Set<NodeIDType> newRCs = this.getReconfigurationRecord(
				AbstractReconfiguratorDB.RecordNames.NODE_CONFIG.toString())
				.getActiveReplicas();
		boolean presentInOld = false;
		for (NodeIDType node : newRCs) {
			if (this.getRCGroupName(node).equals(newRCGroup))
				presentInOld = true;
		}
		return !presentInOld;
	}

	protected Map<String, Set<NodeIDType>> getNewRCGroups() {
		ReconfigurationRecord<NodeIDType> ncRecord = this
				.getReconfigurationRecord(AbstractReconfiguratorDB.RecordNames.NODE_CONFIG
						.toString());
		return this.getRCGroups(this.getMyID(), ncRecord.getNewActives());
	}

	protected Map<String, Set<NodeIDType>> getOldRCGroups() {
		ReconfigurationRecord<NodeIDType> ncRecord = this
				.getReconfigurationRecord(AbstractReconfiguratorDB.RecordNames.NODE_CONFIG
						.toString());
		return this.getRCGroups(this.getMyID(), ncRecord.getActiveReplicas());
	}

	/*
	 * FIXME: This method currently reconstructs a new consistent hashing
	 * structure afresh each time it is called, which may be inefficient. But it
	 * is unclear where we can store it in a manner that is safe, so we just
	 * reconstruct it from the DB on demand.
	 */
	protected Map<String, Set<NodeIDType>> getRCGroups(NodeIDType rc,
			Set<NodeIDType> allRCs, boolean print) {
		assert (rc != null && allRCs != null);
		ConsistentHashing<NodeIDType> newRCCH = new ConsistentHashing<NodeIDType>(
				allRCs);
		HashMap<String, Set<NodeIDType>> groups = new HashMap<String, Set<NodeIDType>>();
		String s = "RC groups with " + allRCs + " at " + getMyID() + " = ";
		// compute RC groups as in createDefaultGroups
		for (NodeIDType node : allRCs) {
			Set<NodeIDType> group = newRCCH.getReplicatedServers(node
					.toString());
			if (group.contains(rc)) {
				s += " [" + (node + ":" + group) + "] ";
				groups.put(this.getRCGroupName(node), group);
			}
		}
		if (print)
			System.out.println(s + "\n");

		return groups;
	}

	protected Map<String, Set<NodeIDType>> getRCGroups(NodeIDType rc,
			Set<NodeIDType> allRCs) {
		return this.getRCGroups(rc, allRCs, false);
	}

	private NodeIDType getMyID() {
		return this.myID;
	}

	private synchronized void selfWait() {
		try {
			while (!this.isNodeConfigChangeComplete()) {
				this.wait();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private synchronized void selfNotify() {
		this.notifyAll();
	}

	/**
	 * @param stopEpoch
	 * @return If this {@code stopEpoch} was handled successfully.
	 */
	public boolean handleStopEpoch(StopEpoch<NodeIDType> stopEpoch) {
		log.info(this + " stopped " + stopEpoch.getSummary());
		this.clearMerged(stopEpoch.getServiceName(), stopEpoch.getEpochNumber());
		return true;
	}

	protected String getRCGroupName(NodeIDType node) {
		return node.toString();
	}

	protected String getRCGroupName(String name) {
		if (name.equals(AbstractReconfiguratorDB.RecordNames.NODE_CONFIG
				.toString()))
			return name;
		else if (this.isRCGroupName(name))
			return name;
		else
			return this.getRCGroupName(this.consistentNodeConfig
					.getFirstReconfigurator(name));
	}

	protected boolean isRCGroupName(String name) {
		for (NodeIDType rc : this.consistentNodeConfig.getReconfigurators())
			if (this.getRCGroupName(rc).equals(name))
				return true;
		return false;
	}

	/*
	 * Insert next nodeConfig version into DB. We have the necessary nodeID info
	 * from the NODE_CONFIG reconfiguration record, but we do need
	 * consistentNodeConfig for the corresponding InetSocketAddresses.
	 * 
	 * FIXME: This should probably be done atomically, not one record at a time.
	 */
	protected boolean updateDBNodeConfig(int version) {
		ReconfigurationRecord<NodeIDType> ncRecord = this
				.getReconfigurationRecord(AbstractReconfiguratorDB.RecordNames.NODE_CONFIG
						.toString());
		boolean added = true;
		for (NodeIDType rc : ncRecord.getNewActives())
			added = added
					&& this.addReconfigurator(rc,
							this.consistentNodeConfig.getNodeSocketAddress(rc),
							version);
		return added;
	}

	protected Set<NodeIDType> setRCEpochs(Set<NodeIDType> addNodes,
			Set<NodeIDType> deleteNodes) {
		ReconfigurationRecord<NodeIDType> ncRecord = this
				.getReconfigurationRecord(AbstractReconfiguratorDB.RecordNames.NODE_CONFIG
						.toString());
		Set<NodeIDType> affectedNodes = new HashSet<NodeIDType>();
		// affected by adds
		for (NodeIDType addNode : addNodes) {
			affectedNodes.add(addNode);
			for (NodeIDType oldNode : ncRecord.getActiveReplicas())
				if (this.isAffected(oldNode, addNode))
					affectedNodes.add(oldNode);
		}

		// affected by deletes
		for (NodeIDType deleteNode : deleteNodes)
			for (NodeIDType oldNode : ncRecord.getActiveReplicas())
				if (this.isAffected(oldNode, deleteNode))
					affectedNodes.add(oldNode);

		ncRecord.setRCEpochs(affectedNodes, addNodes, deleteNodes);
		this.setRCEpochs(ncRecord);
		return affectedNodes;
	}

	/*
	 * Determines if rcNode's group needs to be reconfigured because of the
	 * addition or deletion of addOrDelNode. We need this to correctly track the
	 * epoch numbers of all RC groups.
	 */
	protected boolean isAffected(NodeIDType rcNode, NodeIDType addOrDelNode) {
		if (addOrDelNode == null)
			return false;
		boolean affected = false;
		ConsistentHashing<NodeIDType> oldRing = this.getOldConsistentHashRing();
		NodeIDType hashNode = oldRing.getReplicatedServersArray(
				this.getRCGroupName(addOrDelNode)).get(0);

		ReconfigurationRecord<NodeIDType> ncRecord = this
				.getReconfigurationRecord(AbstractReconfiguratorDB.RecordNames.NODE_CONFIG
						.toString());

		for (NodeIDType oldNode : ncRecord.getActiveReplicas()) {
			if (oldRing.getReplicatedServers(this.getRCGroupName(oldNode))
					.contains(hashNode)) {
				affected = true;
			}
		}
		return affected;
	}

	protected ConsistentHashing<NodeIDType> getOldConsistentHashRing() {
		return new ConsistentHashing<NodeIDType>(this.getReconfigurationRecord(
				AbstractReconfiguratorDB.RecordNames.NODE_CONFIG.toString())
				.getActiveReplicas());
	}

	protected ConsistentHashing<NodeIDType> getNewConsistentHashRing() {
		return new ConsistentHashing<NodeIDType>(this.getReconfigurationRecord(
				AbstractReconfiguratorDB.RecordNames.NODE_CONFIG.toString())
				.getNewActives());
	}

	/**
	 * @param taskKey
	 * @return True as specified by {@link Collection#add}.
	 */
	public synchronized boolean addRCTask(String taskKey) {
		return this.pendingRCProtocolTasks.add(taskKey);
	}

	protected synchronized boolean removeRCTask(String taskKey) {
		boolean removed = this.pendingRCProtocolTasks.remove(taskKey);
		this.selfNotify();
		return removed;
	}

	protected synchronized boolean isPendingRCTasksEmpty() {
		return this.pendingRCProtocolTasks.isEmpty();
	}
}