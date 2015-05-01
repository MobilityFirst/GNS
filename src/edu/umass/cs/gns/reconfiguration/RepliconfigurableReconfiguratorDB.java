package edu.umass.cs.gns.reconfiguration;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.umass.cs.gns.nio.InterfaceJSONNIOTransport;
import edu.umass.cs.gns.protocoltask.ProtocolTask;
import edu.umass.cs.gns.reconfiguration.AbstractReconfiguratorDB.RecordNames;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.ConsistentHashing;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.ReconfigurationRecord;

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
			InterfaceJSONNIOTransport<NodeIDType> niot) {
		super(app, myID, consistentNodeConfig, niot);
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
		return record!=null ? record.getActiveReplicas() : null;
	}

	/*
	 * FIXME: implement durability
	 */
	public boolean coordinateRequest(InterfaceRequest request)
			throws IOException, RequestParseException {
		String rcGroupName = this.getRCGroupName(request.getServiceName());
		//assert (this.getReplicaGroup(rcGroupName) != null);
		assert(request.getServiceName().equals(rcGroupName) || request.getServiceName().equals("name0")) : rcGroupName + " " + request;
		return super.coordinateRequest(rcGroupName, request);
	}
	
	// FIXME: use or remove
	/*
	 * Uses reflection to invoke appropriate method in AbstractReconfiguratorDB.
	 */
	public boolean handleRequest(
			BasicReconfigurationPacket<NodeIDType> rcPacket,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
		log.info(this + " executing " + rcPacket);
		Object retval = AbstractReconfiguratorDB.autoInvokeMethod(this.app,
				rcPacket, this.consistentNodeConfig);
		return (retval != null) ? (Boolean) retval : null;
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
				System.out.println("Creating reconfigurator group "
						+ this.app.getRCGroupName(node) + " with members " + group);
				this.createReplicaGroup(
						this.app.getRCGroupName(node),
						0,
						this.getInitialRCGroupRecord(this.app.getRCGroupName(node),
								group).toString(), group);
			}
			else System.out.println(this.getMyID() + " not in group " + group);
		}
		// create nodeconfig record
		this.createReplicaGroup(
				RecordNames.NODE_CONFIG.toString(),
				0,
				this.getInitialRCGroupRecord(RecordNames.NODE_CONFIG.toString(),
						this.consistentNodeConfig.getReconfigurators())
						.toString(), this.consistentNodeConfig
						.getReconfigurators());
		return false;
	}
		
	private ReconfigurationRecord<NodeIDType> getInitialRCGroupRecord(String groupName, Set<NodeIDType> group) {
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

	/* Checks if I am affected because of the addition or deletion
	 * of the node argument. The check is performed based on the 
	 * RC groups actually present in the DB, not NodeConfig.
	 */
	protected boolean amAffected(NodeIDType node) {
		if (node == null)
			return false;
		boolean affected = false;
		NodeIDType hashNode = this.getOldConsistentHashRing()
				.getReplicatedServersArray(this.app.getRCGroupName(node))
				.get(0);
		Set<String> myRCGroupNames = this.app.getRCGroupNames();
		/*
		 * We need to fetch all current RC groups. These groups in general may
		 * or may not be consistent with those dictated by NodeConfig.
		 */
		for (String rcGroup : myRCGroupNames) {
			if (this.app.getRCGroupName(node).equals(rcGroup)
					|| this.app.getRCGroupName(hashNode).equals(rcGroup))
				affected = true;
		}
		return affected;
	}
	
	/* This method gets RC group names based on NodeConfig. This
	 * may in general be different from the RC groups actually
	 * in the DB.
	 * 
	 * Could be a static method with nodeconfig arg.
	 */
	private Set<String> getNodeConfigRCGroupNames(ConsistentReconfigurableNodeConfig<NodeIDType> nodeConfig) {
		Set<String> groupNames = new HashSet<String>();
		Set<NodeIDType> reconfigurators = nodeConfig
				.getReconfigurators();
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
	
	protected Set<String> getRCGroupNames() {
		return this.app.getRCGroupNames();
	}
	
	protected Map<String, Set<NodeIDType>> getRCGroups() {
		Set<String> rcGroupNames = this.getRCGroupNames();
		HashMap<String, Set<NodeIDType>> rcGroups = new HashMap<String, Set<NodeIDType>>();
		for(String rcGroupName : rcGroupNames) {
			ReconfigurationRecord<NodeIDType> record = this.getReconfigurationRecord(rcGroupName);
			assert(record.getActiveReplicas()!=null);
			rcGroups.put(rcGroupName, record.getActiveReplicas());
		}
		return rcGroups;
	}
	
	protected Map<String, Set<NodeIDType>> getNewRCGroups() {
		ReconfigurationRecord<NodeIDType> ncRecord = this
				.getReconfigurationRecord(AbstractReconfiguratorDB.RecordNames.NODE_CONFIG
						.toString());
		return this.app.getRCGroups(this.getMyID(), ncRecord.getNewActives());
	}
	
	protected boolean isRCGroupName(String name) {
		return this.app.isRCGroupName(name);
	}

	protected String getOldGroupName(String name) {
		return this.app.getRCGroupName(this.getOldConsistentHashRing().getReplicatedServersArray(name).get(0));
	}
	protected ConsistentHashing<NodeIDType> getOldConsistentHashRing() {
		return new ConsistentHashing<NodeIDType>(this.getReconfigurationRecord(
				AbstractReconfiguratorDB.RecordNames.NODE_CONFIG.toString())
				.getActiveReplicas());
	}
	// needed because we have no copy of the old consistent hash ring
	protected Set<NodeIDType> getOldGroupToSplitFrom(NodeIDType newRCNode) {
		return new ConsistentHashing<NodeIDType>(this.getReconfigurationRecord(
				AbstractReconfiguratorDB.RecordNames.NODE_CONFIG.toString())
				.getActiveReplicas()).getReplicatedServers(this.app
				.getRCGroupName(newRCNode));
	}
	
	protected boolean changeDBNodeConfig(int version) {
		return this.app.updateNodeConfig(version);
	}
}
