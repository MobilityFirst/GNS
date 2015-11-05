/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.reconfiguration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxosutil.PaxosInstanceCreationException;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.reconfiguration.AbstractReconfiguratorDB.RecordNames;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.StartEpoch;
import edu.umass.cs.reconfiguration.reconfigurationutils.ConsistentHashing;
import edu.umass.cs.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.ReconfigurationRecord;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

/**
 * @author V. Arun
 *         <p>
 *         We need this class to extend both PaxosReplicationCoordinator and
 *         AbstractReconfiguratorDB, so we use an interface for the latter.
 * @param <NodeIDType>
 */
public class RepliconfigurableReconfiguratorDB<NodeIDType> extends
		PaxosReplicaCoordinator<NodeIDType> {

	private static enum ReplicaCoordinator {
		PAXOS, DYNAMO
	};

	private static ReplicaCoordinator RC_REPLICA_COORDINATOR = ReplicaCoordinator.PAXOS;

	protected final AbstractReconfiguratorDB<NodeIDType> app;
	protected final ConsistentReconfigurableNodeConfig<NodeIDType> consistentNodeConfig;
	private final HashMap<NodeIDType, Long> pendingReconfiguratorDeletions = new HashMap<NodeIDType, Long>();

	/**
	 * @param app
	 * @param myID
	 * @param consistentNodeConfig
	 * @param niot
	 * @param startCleanSlate
	 */
	public RepliconfigurableReconfiguratorDB(
			AbstractReconfiguratorDB<NodeIDType> app,
			NodeIDType myID,
			ConsistentReconfigurableNodeConfig<NodeIDType> consistentNodeConfig,
			Messenger<NodeIDType, JSONObject> niot,
			boolean startCleanSlate) {
		// setting paxosManager out-of-order limit to 1
		super(app, myID, consistentNodeConfig, niot, 1);
		assert (niot != null);
		this.app = app;
		this.consistentNodeConfig = consistentNodeConfig;
		// only request that needs coordination;
		this.registerCoordination(ReconfigurationPacket.PacketType.RC_RECORD_REQUEST);
		// default groups need only be created for paxos, not dynamo
		if (RC_REPLICA_COORDINATOR.equals(ReplicaCoordinator.PAXOS)
				&& !startCleanSlate)
			this.createDefaultGroups();
		this.setLargeCheckpoints();
	}

	// needed by Reconfigurator
	protected Set<NodeIDType> getActiveReplicas(String name) {
		ReconfigurationRecord<NodeIDType> record = this
				.getReconfigurationRecord(name);
		return record != null ? record.getActiveReplicas() : null;
	}

	/*
	 * FIXME: need to prevent nodes from falling behind on node config changes
	 * in the first place to prevent the severe log below.
	 */
	@Override
	public boolean coordinateRequest(Request request)
			throws IOException, RequestParseException {
		String rcGroupName = this.getRCGroupName(request.getServiceName());
		// can only send stop request to own RC group
		if (!rcGroupName.equals(request.getServiceName())
				&& (request instanceof ReconfigurableRequest)
				&& ((ReconfigurableRequest) request).isStop()) {
			ReconfigurableRequest stop = ((ReconfigurableRequest) request);
			log.log(Level.INFO,
					"{0} received stop request for RC group {1}:{2} that is not (yet) "
							+ " node config likely because this node has fallen behind.",
					new Object[] { this, stop.getServiceName(),
							stop.getEpochNumber() });
			rcGroupName = request.getServiceName();
		}

		return super.coordinateRequest(rcGroupName, request);
	}

	/**
	 * @param request
	 * @return Returns the result of
	 *         {@link #coordinateRequest(Request)}.
	 */
	public boolean coordinateRequestSuppressExceptions(Request request) {
		try {
			return this.coordinateRequest(request);
		} catch (RequestParseException | IOException e) {
			log.warning(this + " incurred " + e.getClass().getSimpleName()
					+ " while coordinating " + request);
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * Allows uncoordinated access to DB state. In-memory "DB" could also allow
	 * outsiders to modify DB state through returned references, which is
	 * problematic unless the returned values are deep copied.
	 * 
	 * @param name
	 * @return ReconfigurationRecord for {@code name}.
	 */
	public ReconfigurationRecord<NodeIDType> getReconfigurationRecord(
			String name) {
		return this.app.getReconfigurationRecord(name);
	}

	/*
	 * Create one group for every set of contiguous nodes on the ring of which
	 * this node is a part. The name of the group is the name of the first node
	 * in the group.
	 * 
	 * Upon recovery, if any of the groups exists already, it will be recovered
	 * from the most recent checkpoint.
	 */
	private boolean createDefaultGroups() {
		Set<NodeIDType> reconfigurators = this.consistentNodeConfig
				.getReconfigurators();
		// iterate over all nodes
		for (NodeIDType node : reconfigurators) {
			Set<NodeIDType> group = this.consistentNodeConfig
					.getReplicatedReconfigurators(this.app.getRCGroupName(node));
			// if I am present, create group
			if (group.contains(this.getMyID())) {
				log.log(Level.INFO,
						"{0} creating reconfigurator group {1} with members {2}",
						new Object[] { this, this.app.getRCGroupName(node),
								group });
				try {
				this.createReplicaGroup(
						this.app.getRCGroupName(node),
						0,
						this.getInitialRCGroupRecord(
								this.app.getRCGroupName(node), group)
								.toString(), group);
				} catch(PaxosInstanceCreationException pice) {
					// can happen during recovery
					log.info(this
							+ " encountered paxos instance creation exception (not unusual during recovery): "
							+ pice.getMessage());
				}
			}
		}
		/*
		 * Create NODE_CONFIG record, the master copy of the set of all
		 * reconfigurators.
		 */
		this.createReplicaGroup(
				RecordNames.NODE_CONFIG.toString(),
				0,
				this.getInitialRCGroupRecord(
						RecordNames.NODE_CONFIG.toString(),
						this.consistentNodeConfig.getReconfigurators())
						.toString(), this.consistentNodeConfig
						.getReconfigurators());
		return false;
	}

	@Override
	public boolean createReplicaGroup(String groupName, int epoch,
			String state, Set<NodeIDType> nodes) {
		boolean created = super.createReplicaGroup(groupName, epoch, state,
				nodes);
		return created;
	}

	/** The initial RC group record for RC nodes is just directly inserted into
	 * the reconfiguration DB by paxos as the initial state and does not go
	 * through {@link AbstractReconfiguratorDB#handleRCRecordRequest(edu.umass.cs.reconfiguration.reconfigurationpackets.RCRecordRequest)},
	 * so it does not have to pass the {@link StartEpoch#isInitEpoch()} check and will indeed
	 * not pass that check as both the previous group is not empty.
	 */
	private ReconfigurationRecord<NodeIDType> getInitialRCGroupRecord(
			String groupName, Set<NodeIDType> group) {
		return new ReconfigurationRecord<NodeIDType>(groupName, 0, group, group);
	}

	// needed by Reconfigurator
	protected String getDemandStats(String name) {
		return this.app.getDemandStats(name);
	}

	protected String[] getPendingReconfigurations() {
		return this.app.getPendingReconfigurations();
	}

	protected void close() {
		this.app.close();
	}

	@Override
	public Set<NodeIDType> getReplicaGroup(String serviceName) {
		return super.getReplicaGroup(getRCGroupName(serviceName));
	}

	protected String getRCGroupName(String serviceName) {
		return this.app.getRCGroupName(serviceName);
	}
	
	protected void setRecovering(boolean b) {
		this.app.recovering = false;
	}

	/******************* Reconfigurator reconfiguration methods ***************/

	protected Set<String> getMergeList(String newRCGroupName) {
		return this.app.getMergeList(newRCGroupName);
	}

	protected Map<String, Set<String>> getMergeLists() {
		return this.app.getMergeLists();
	}

	/*
	 * Checks if I am affected because of the addition or deletion of the node
	 * argument.
	 * 
	 * I am affected if either "node" consistent-hashes to an existing RC group
	 * or the consistent hash node is a member of one of my RC groups. This
	 * check is the same for both add and remove operations.
	 */
	protected boolean amAffected(NodeIDType node) {
		if (node == null)
			return false;
		boolean affected = false;
		NodeIDType hashNode = this.getOldConsistentHashRing()
				.getReplicatedServersArray(this.app.getRCGroupName(node))
				.get(0);
		String hashGroup = this.app.getRCGroupName(hashNode);

		Map<String, Set<NodeIDType>> myRCGroups = this.getOldRCGroups();

		for (String rcGroup : myRCGroups.keySet()) {
			if (hashGroup.equals(rcGroup)
					|| myRCGroups.get(rcGroup).contains(hashNode))
				affected = true;
		}
		return affected;
	}

	protected Set<NodeIDType> setRCEpochs(Set<NodeIDType> addNodes,
			Set<NodeIDType> deleteNodes) {
		return this.app.setRCEpochs(addNodes, deleteNodes);
	}

	/*
	 * This method gets RC group names based on NodeConfig. This may in general
	 * be different from the RC groups actually in the DB.
	 * 
	 * Could be a static method with nodeconfig arg.
	 */
	private Set<String> getNodeConfigRCGroupNames(
			ConsistentReconfigurableNodeConfig<NodeIDType> nodeConfig) {
		Set<String> groupNames = new HashSet<String>();
		Set<NodeIDType> reconfigurators = nodeConfig.getReconfigurators();
		// iterate over all nodes
		for (NodeIDType node : reconfigurators)
			// if I am present, add to return set
			if (this.consistentNodeConfig.getReplicatedReconfigurators(
					this.app.getRCGroupName(node)).contains(this.getMyID()))
				groupNames.add(this.app.getRCGroupName(node));
		return groupNames;
	}

	// FIXME: use or remove
	protected Set<String> getNodeConfigRCGroupNames() {
		return this.getNodeConfigRCGroupNames(this.consistentNodeConfig);
	}

	protected Map<String, Set<NodeIDType>> getOldRCGroups() {
		return this.app.getOldRCGroups();
	}

	protected Map<String, Set<NodeIDType>> getNewRCGroups() {
		return this.app.getNewRCGroups();
	}

	/**
	 * Checks if RC group name is name itself by consulting the soft copy of
	 * node config. We could also have checked if the name is node.toString()
	 * for some node in the current set of reconfigurators.
	 * 
	 * @param name
	 * @return True if {@code name} represents a reconfigurator group name.
	 */
	public boolean isRCGroupName(String name) {
		return this.app.isRCGroupName(name);
	}

	/**
	 * RC group name of node is just node.toString(). Changing it to anything
	 * else will break code.
	 * 
	 * @param node
	 * @return String form of {@code node}.
	 */
	public final String getRCGroupName(NodeIDType node) {
		return this.app.getRCGroupName(node);
	}

	/*
	 * The methods below generate the old and new consistent hash rings on
	 * demand. We may want to cache them as a minor (unimplemented)
	 * optimization. We need both rings in order to correctly conduct
	 * reconfigurator add/delete operations. It is unwise to use the soft copy
	 * of consistentNodeConfig. The only reliable information is in the
	 * paxos-managed NODE_CONFIG record that has the current and new (possibly
	 * identical) set of reconfigurators, so we generate the consistent hash
	 * rings on demand using that information.
	 */
	protected String getOldGroupName(String name) {
		return this.app.getRCGroupName(this.getOldConsistentHashRing()
				.getReplicatedServersArray(name).get(0));
	}

	protected ConsistentHashing<NodeIDType> getOldConsistentHashRing() {
		return this.app.getOldConsistentHashRing();
	}

	protected ConsistentHashing<NodeIDType> getNewConsistentHashRing() {
		return this.app.getNewConsistentHashRing();
	}

	// needed because we have no copy of the old consistent hash ring
	protected Map<String, Set<NodeIDType>> getOldGroup(String newRCNode) {
		ArrayList<NodeIDType> oldGroup = this.getOldConsistentHashRing()
				.getReplicatedServersArray(newRCNode);
		// oldGroupName != newRCNode in the case of RC node additions
		String oldGroupName = this.app.getRCGroupName(oldGroup.get(0));
		Map<String, Set<NodeIDType>> group = new HashMap<String, Set<NodeIDType>>();
		group.put(oldGroupName, new HashSet<NodeIDType>(oldGroup));

		return group;
	}

	protected Set<String> getNodeSetAsStringSet(Set<NodeIDType> nodeSet) {
		Set<String> strSet = new HashSet<String>();
		for (NodeIDType node : nodeSet)
			strSet.add(this.getRCGroupName(node));
		return strSet;
	}

	/*
	 * Needed because we may have no copy of the new consistent hash ring when
	 * nodes are being deleted.
	 */
	protected Map<String, Set<NodeIDType>> getNewGroup(String oldRCNode) {
		ArrayList<NodeIDType> newGroup = this.getNewConsistentHashRing()
				.getReplicatedServersArray(oldRCNode);
		String newGroupName = this.app.getRCGroupName(newGroup.get(0));
		Map<String, Set<NodeIDType>> group = new HashMap<String, Set<NodeIDType>>();
		group.put(newGroupName, new HashSet<NodeIDType>(newGroup));
		return group;
	}

	/*
	 * Changes node config copy in DB. We need a persistent copy there as we can
	 * not rely on the inital node config supplied in the constructor as that
	 * may be out-of-date. Actually, we need to store node config information in
	 * the DB primarily for the nodeID -> socketAddress mapping. We always have
	 * the set of reconfigurators available in the NODE_CONFIG RC record.
	 */
	protected boolean changeDBNodeConfig(int version) {
		return this.app.updateDBNodeConfig(version);
	}

	@Override
	public boolean deleteFinalState(String rcGroupName, int epoch) {
		// special case for node config changes
		if (rcGroupName.equals(AbstractReconfiguratorDB.RecordNames.NODE_CONFIG
				.toString()))
			return isNCReady(epoch + 1); // final state deletion unnecessary
		return super.deleteFinalState(rcGroupName, epoch);
	}

	private boolean isNCReady(int epoch) {
		ReconfigurationRecord<NodeIDType> ncRecord = this
				.getReconfigurationRecord(AbstractReconfiguratorDB.RecordNames.NODE_CONFIG
						.toString());
		if (ncRecord != null && ncRecord.getEpoch() == epoch
				&& ncRecord.isReady())
			return true;
		else {
			String debug = this.app.isNodeConfigChangeCompleteDebug();
			log.log(Level.INFO,
					"{0} has *NOT* completed node config change to epoch {1}; state = {2}; {3}",
					new Object[] { this, epoch, ncRecord.getSummary(), debug });
		}
		assert (ncRecord == null || ncRecord.getEpoch() != epoch || !ncRecord
				.isReady()) : ncRecord;
		return false;
	}

	protected boolean isBeingDeleted(String curRCGroup) {
		Set<NodeIDType> newRCs = this.getReconfigurationRecord(
				AbstractReconfiguratorDB.RecordNames.NODE_CONFIG.toString())
				.getNewActives();
		boolean presentInNew = false;
		for (NodeIDType node : newRCs) {
			if (this.getRCGroupName(node).equals(curRCGroup))
				presentInNew = true;
		}
		return !presentInNew;
	}

	protected void delayedDeleteComplete() {
		this.app.delayedDeleteComplete();
	}

	/**
	 * FIXME: Unsafe if the set of all actives can be out-of-date. This is meant
	 * to be used only for testing purposes.
	 * 
	 * @return The set of all active replicas. This is used by WaitAckDropEpoch
	 *         to force *all* active replica nodes, not just the previous
	 *         epoch's active replicas, to drop any final state for the name
	 *         before deleting it. We need this to speed up re-creations (delete
	 *         followed by a creation of the same name) faster than
	 *         MAX_FINAL_STATE_AGE time.
	 */
	public Set<NodeIDType> getAllActives() {
		return this.consistentNodeConfig.getActiveReplicas();
	}

	protected int getCurNCEpoch() {
		return this.getReconfigurationRecord(
				AbstractReconfiguratorDB.RecordNames.NODE_CONFIG.toString())
				.getEpoch();
	}

	protected void garbageCollectDeletedNode(NodeIDType node) {
		// to stop paxos failure detection
		this.stopFailureMonitoring(node);
		/*
		 * To clean old checkpoint crap lying around. Could also put time here
		 * and wait for MAX_FINAL_STATE_AGE.
		 */
		long curNCEpoch = getCurNCEpoch();
		log.log(Level.INFO,
				"{0} queueing {1}:{2} for garbage collection of old checkpoints; pending queue size = {3}",
				new Object[] { this, node, curNCEpoch,
						this.pendingReconfiguratorDeletions.size() + 1 });
		this.pendingReconfiguratorDeletions.put(node, curNCEpoch);
	}

	/*
	 * A reconfigurator's file system based checkpoints can be dropped after it
	 * is more than 2 epochs old. If other reconfigurators have not kept pace
	 * with node config changes, they have to be deleted from the system first
	 * anyway before joining back in. There is no way for a reconfigurator to
	 * recover and "roll forward" node config changes if it has missed multiple
	 * node config changes. Subsequent RC node config changes should not be
	 * continued with if some nodes have not completed the current node config
	 * change, as doing so essentially means that those RC nodes will be treated
	 * as failed.
	 */
	private static int RECONFIGURATOR_GC_WAIT_EPOCHS = 2;

	protected void garbageCollectOldFileSystemBasedCheckpoints() {
		for (Iterator<NodeIDType> iter = this.pendingReconfiguratorDeletions
				.keySet().iterator(); iter.hasNext();) {
			NodeIDType removedRC = iter.next();
			long removedEpoch = this.pendingReconfiguratorDeletions
					.get(removedRC);
			if (getCurNCEpoch() - removedEpoch > RECONFIGURATOR_GC_WAIT_EPOCHS) {
				log.log(Level.INFO, "{0} invoking RC GC on {1}", new Object[] {
						this, removedRC });
				this.app.garbageCollectedDeletedNode(removedRC);
				iter.remove();
			}
		}
	}

	/**
	 * Waits until the specified group is ready with timeout specifying how
	 * frequently we should check. A better alternative is to synchronized
	 * explicitly over the exact event being waited for, which would obviate the
	 * DB read based check.
	 * 
	 * FIXME: To be deprecated.
	 * 
	 * @param name
	 * @param epoch
	 * @param timeout
	 */
	public void waitReady(String name, int epoch, long timeout) {
		synchronized (this.app) {
			ReconfigurationRecord<NodeIDType> record = null;
			while ((record = this.app.getReconfigurationRecord(name, epoch)) == null
					|| (record.getEpoch() - epoch < 0))
				try {
					this.app.wait(timeout);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			;
		}
	}

	protected boolean isNCRecord(String name) {
		return name.equals(AbstractReconfiguratorDB.RecordNames.NODE_CONFIG
				.toString());
	}
}
