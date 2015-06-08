package edu.umass.cs.reconfiguration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.umass.cs.gigapaxos.InterfaceRequest;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.reconfiguration.AbstractReconfiguratorDB.RecordNames;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationutils.ConsistentHashing;
import edu.umass.cs.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.ReconfigurationRecord;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

/*
 * We need this class to extend both PaxosReplicationCoordinator and
 * AbstractReconfiguratorDB, so we use an interface for the latter.
 */
public class RepliconfigurableReconfiguratorDB<NodeIDType> extends
		PaxosReplicaCoordinator<NodeIDType> {

	private static enum ReplicaCoordinator {
		PAXOS, DYNAMO
	};

	private static ReplicaCoordinator RC_REPLICA_COORDINATOR = ReplicaCoordinator.PAXOS;

	private final AbstractReconfiguratorDB<NodeIDType> app;
	private final ConsistentReconfigurableNodeConfig<NodeIDType> consistentNodeConfig;

	public RepliconfigurableReconfiguratorDB(
			AbstractReconfiguratorDB<NodeIDType> app,
			NodeIDType myID,
			ConsistentReconfigurableNodeConfig<NodeIDType> consistentNodeConfig,
			JSONMessenger<NodeIDType> niot) {
		// setting paxosManager out-of-order limit to 1
		super(app, myID, consistentNodeConfig, niot, 1);
		assert (niot != null);
		this.app = app;
		this.consistentNodeConfig = consistentNodeConfig;
		this.registerCoordination(ReconfigurationPacket.PacketType.RC_RECORD_REQUEST);
		// default groups need only be created for paxos, not dynamo
		if (RC_REPLICA_COORDINATOR.equals(ReplicaCoordinator.PAXOS))
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
	 * FIXME: implement durability
	 */
        @Override
	public boolean coordinateRequest(InterfaceRequest request)
			throws IOException, RequestParseException {
		String rcGroupName = this.getRCGroupName(request.getServiceName());
		// assert (this.getReplicaGroup(rcGroupName) != null);
                
//		assert (request.getServiceName().equals(rcGroupName) || request
//				.getServiceName().equals("name0")) : rcGroupName + " "
//				+ request;
		return super.coordinateRequest(rcGroupName, request);
	}

	public boolean coordinateRequestSuppressExceptions(InterfaceRequest request) {
		try {
			return this.coordinateRequest(request);
		} catch (IOException | RequestParseException e) {
			log.warning(this + " incurred " + e.getClass().getSimpleName()
					+ " while coordinating " + request);
			e.printStackTrace();
		}
		return false;
	}

	/*
	 * FIXME: allows uncoordinated access to DB state. In-memory "DB" could also
	 * allow outsiders to modify DB state through returned references, which is
	 * problematic unless the returned values are deep copied.
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
				log.info("Creating reconfigurator group "
						+ this.app.getRCGroupName(node) + " with members "
						+ group);
				this.createReplicaGroup(
						this.app.getRCGroupName(node),
						0,
						this.getInitialRCGroupRecord(
								this.app.getRCGroupName(node), group)
								.toString(), group);
			}
		}
		/*
		 * create NODE_CONFIG record, the master copy of the set of all
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
		/*
		 * In case of split or merge operations, the createReplicaGroup call may
		 * fail because the group has already been created for other reasons,
		 * but we still need to update state.
		 * 
		 * FIXME: This way of updating state can not possibly be correct. Only
		 * paxos must be able to call this method or for that matter any method
		 * that changes app state.
		 */
		return created;
	}

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

	/******************* Reconfigurator reconfiguration methods ***************/

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

	/*
	 * Checks if RC group name is name itself by consulting the soft copy of
	 * node config. We could also have checked if the name is node.toString()
	 * for some node in the current set of reconfigurators.
	 */
	public boolean isRCGroupName(String name) {
		return this.app.isRCGroupName(name);
	}

	// RC group name of node is just node.toString()
	public String getRCGroupName(NodeIDType node) {
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
		String oldGroupName = this.app.getRCGroupName(oldGroup.get(0));
		Map<String, Set<NodeIDType>> group = new HashMap<String, Set<NodeIDType>>();
		group.put(oldGroupName, new HashSet<NodeIDType>(oldGroup));

		return group;
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
		boolean paxosInstanceDeleted = super.deleteFinalState(rcGroupName,
				epoch);
		// need to delete record itself if present
		return paxosInstanceDeleted
				&& this.app.deleteReconfigurationRecord(rcGroupName, epoch);
	}

	/*
	 * We need to keep track of ongoing RC tasks in order to know when a node
	 * config change is complete. We can track it with volatile state as
	 * incomplete operations will be rolled forward correctly in case of
	 * failures anyway. We need this in-memory state only for RC group
	 * reconfigurations, not regular serviceName record reconfigurations.
	 */
	public boolean addRCTask(String taskKey) {
		return this.app.addRCTask(taskKey);
	}

	// inverts addRCTask
	public boolean removeRCTask(String taskKey) {
		return this.app.removeRCTask(taskKey);
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

}
