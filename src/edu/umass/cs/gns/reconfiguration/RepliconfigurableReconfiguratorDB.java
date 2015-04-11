package edu.umass.cs.gns.reconfiguration;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import edu.umass.cs.gns.nio.InterfaceJSONNIOTransport;
import edu.umass.cs.gns.protocoltask.ProtocolTask;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.ReconfigurationRecord;
import java.util.logging.Logger;

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
        @Override
	public boolean coordinateRequest(InterfaceRequest request)
			throws IOException, RequestParseException {
		String rcGroupName = this.getRCGroupName(request);
                // In here while testing, but there is no reason for it to hold in general.
//                if (this.getReplicaGroup(rcGroupName) == null) {
//                  System.out.println("REPLICA GROUP IS NULL for rcgroup: " + rcGroupName + " servicename: " + request.getServiceName());
//                }
//		assert (this.getReplicaGroup(rcGroupName) != null);
		return super.coordinateRequest(rcGroupName, request);
	}

	// assumes RC group name is the name of the first node
	private String getRCGroupName(InterfaceRequest request) {
		Set<NodeIDType> reconfigurators = this.consistentNodeConfig
				.getReplicatedReconfigurators(request.getServiceName());
		assert (reconfigurators != null && !reconfigurators.isEmpty());
		return reconfigurators.iterator().next().toString();
	}
	
	// Translate RC node to RC group name
	private String getRCGroupName(NodeIDType node) {
		return node.toString();
	}

	/*
	 * Uses reflection to invoke appropriate method in AbstractReconfiguratorDB.
	 */
	public boolean handleRequest(
			BasicReconfigurationPacket<NodeIDType> rcPacket,
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {
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
	 * FIXME: Need to add support for changing these groups midway.
	 */
	private boolean createDefaultGroups() {
		Set<NodeIDType> reconfigurators = this.consistentNodeConfig
				.getReconfigurators();
		// iterate over all nodes
		for (NodeIDType node : reconfigurators) {
			Set<NodeIDType> group = this.consistentNodeConfig
					.getReplicatedReconfigurators(node.toString());
			// if I am present, create group
			if (group.contains(this.getMyID())) {
				System.out.println("Creating reconfigurator group "
						+ node.toString() + " with members " + group);
				this.createReplicaGroup(
						node.toString(),
						0,
						this.getInitialRCGroupRecord(this.getRCGroupName(node),
								group).toString(), group);
			}
		}
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
	
	private String getRCGroupName(String serviceName) {
		return this.consistentNodeConfig.getFirstReconfigurator(serviceName)
				.toString();
	}	

	/******************* Reconfigurator reconfiguration methods ***************/

	/* Checks if I am affected because of the addition or deletion
	 * of the node argument. The check is performed based on the 
	 * RC groups actually present in the DB, not NodeConfig.
	 */
	protected boolean amAffected(NodeIDType node) {
		boolean affected = false;
		NodeIDType hashNode = this.consistentNodeConfig
				.getFirstReconfigurator(node.toString());
		Set<String> myRCGroupNames = this.app.getRCGroupNames();
		/*
		 * We need to fetch all current RC groups. These groups in general may
		 * or may not be consistent with those dictated by NodeConfig.
		 */
		for (String rcGroup : myRCGroupNames) {
			if (node.equals(rcGroup) || hashNode.equals(rcGroup))
				affected = true;
		}
		return affected;
	}
	
	/* This method gets RC group names based on NodeConfig. This
	 * may in general be different from the RC groups actually
	 * in the DB.
	 */
	protected Set<String> getNodeConfigRCGroupNames() {
		Set<String> groupNames = new HashSet<String>();
		Set<NodeIDType> reconfigurators = this.consistentNodeConfig
				.getReconfigurators();
		// iterate over all nodes
		for (NodeIDType node : reconfigurators)
			// if I am present, add to return set
			if (this.consistentNodeConfig.getReplicatedReconfigurators(
					node.toString()).contains(this.getMyID()))
				groupNames.add(node.toString());
		return groupNames;
	}
	
	protected Set<String> getRCGroupNames() {
		return this.app.getRCGroupNames();
	}
}