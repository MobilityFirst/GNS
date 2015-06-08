package edu.umass.cs.reconfiguration.reconfigurationutils;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;

import edu.umass.cs.reconfiguration.InterfaceModifiableActiveConfig;
import edu.umass.cs.reconfiguration.InterfaceModifiableRCConfig;
import edu.umass.cs.reconfiguration.InterfaceReconfigurableNodeConfig;

/*
 * This class is a wrapper around NodeConfig to ensure that it is consistent,
 * i.e., it returns consistent results even if it changes midway. In particular,
 * it does not allow the use of a method like getNodeIDs().
 * 
 * It also has consistent hashing utility methods.
 */
public class ConsistentReconfigurableNodeConfig<NodeIDType> extends
		ConsistentNodeConfig<NodeIDType> implements
		InterfaceModifiableActiveConfig<NodeIDType>, InterfaceModifiableRCConfig<NodeIDType>,
                InterfaceGetActiveIPs {
	private final SimpleReconfiguratorNodeConfig<NodeIDType> nodeConfig;
	private Set<NodeIDType> activeReplicas; // most recent cached copy
	private Set<NodeIDType> reconfigurators; // most recent cached copy

	// need to refresh when nodeConfig changes
	private final ConsistentHashing<NodeIDType> CH_RC;
	// need to refresh when nodeConfig changes
	private final ConsistentHashing<NodeIDType> CH_AR;
	
	/* We need to track reconfigurators slated for removal separately because
	 * we still need ID to socket address mappings for deleted nodes (e.g.,
	 * in order to do networking for completing deletion operations) but not
	 * include deleted nodes in the consistent hash ring. Thus, the ring
	 * transitions immediately to the new ring when reconfigurators are 
	 * added or slated for removal, but socket addresses for removal-slated
	 * nodes are maintained until explicitly told to garbage collect them.
	 */
	private Set<NodeIDType> reconfiguratorsSlatedForRemoval = new HashSet<NodeIDType>();
	
	public ConsistentReconfigurableNodeConfig(
			InterfaceReconfigurableNodeConfig<NodeIDType> nc) {
		super(nc);
		this.nodeConfig = new SimpleReconfiguratorNodeConfig<NodeIDType>(nc);
		this.activeReplicas = this.nodeConfig.getActiveReplicas();
		this.reconfigurators = this.nodeConfig.getReconfigurators();
		this.CH_RC = new ConsistentHashing<NodeIDType>(this.reconfigurators);
		this.CH_AR = new ConsistentHashing<NodeIDType>(this.activeReplicas, true);
	}

	@Override
	public Set<NodeIDType> getValuesFromStringSet(Set<String> strNodes) {
		return this.nodeConfig.getValuesFromStringSet(strNodes);
	}

	@Override
	public Set<NodeIDType> getValuesFromJSONArray(JSONArray array)
			throws JSONException {
		return this.nodeConfig.getValuesFromJSONArray(array);
	}

	@Override
	public Set<NodeIDType> getNodeIDs() {
		throw new RuntimeException("The use of this method is not permitted");
	}

	@Override
	public Set<NodeIDType> getActiveReplicas() {
		return this.nodeConfig.getActiveReplicas();
	}

	@Override
	public Set<NodeIDType> getReconfigurators() {
		return this.nodeConfig.getReconfigurators();
	}

	// consistent coz it always consults nodeConfig
	public ArrayList<InetAddress> getNodeIPs(Set<NodeIDType> nodeIDs) {
		ArrayList<InetAddress> addresses = new ArrayList<InetAddress>();
		for (NodeIDType id : nodeIDs) {
			addresses.add(this.nodeConfig.getNodeAddress(id));
		}
		assert (addresses != null);
		return addresses;
	}

        @Override
        public ArrayList<InetAddress> getActiveIPs() {
          return getNodeIPs(getActiveReplicas());
        }

	// refresh before returning
	public Set<NodeIDType> getReplicatedReconfigurators(String name) {
		this.refreshReconfigurators();
		return this.CH_RC.getReplicatedServers(name);
	}

	// refresh before returning
	public NodeIDType getFirstReconfigurator(String name) {
		this.refreshReconfigurators();
		return this.CH_RC.getNode(name);
	}
	
	// NOT USED IN NEW APP. FOR BACKWARDS COMPATIBILITY WITH OLD APP.
	// WILL BE REMOVED AFTER NEW APP IS TESTED.
	/**
	 * Returns the hash for this name.
	 * @param name 
	 * @return Returns the node id to which name consistent-hashes.
	 */
	@Deprecated
	public NodeIDType getReconfiguratorHash(String name) {
		return this.getFirstReconfigurator(name);
	}

	// refresh before returning
	public Set<NodeIDType> getReplicatedActives(String name) {
		this.refreshActives();
		return this.CH_AR.getReplicatedServers(name);
	}

	public ArrayList<InetAddress> getReplicatedActivesIPs(String name) {
		return this.getNodeIPs(this.getReplicatedActives(name));
	}

	/*
	 * This method maps a set of addresses, newAddresses, to a set of nodes such
	 * that there is maximal overlap with the specified set of nodes, oldNodes.
	 * It is somewhat nontrivial only because there is a many-to-one mapping
	 * from nodes to addresses, so a simple reverse lookup is not meaningful.
	 */
	public Set<NodeIDType> getIPToActiveReplicaIDs(ArrayList<InetAddress> newAddresses,
			Set<NodeIDType> oldNodes) {
		Set<NodeIDType> newNodes = new HashSet<NodeIDType>(); // return value
		ArrayList<InetAddress> unassigned = new ArrayList<InetAddress>();
		for (InetAddress address : newAddresses)
			unassigned.add(address);
		// assign old nodes first if they match any new address
		for (NodeIDType oldNode : oldNodes) {
			InetAddress oldAddress = this.nodeConfig.getNodeAddress(oldNode);
			if (unassigned.contains(oldAddress)) {
				newNodes.add(oldNode);
				unassigned.remove(oldAddress);
			}
		}
		// assign any node to unassigned addresses
		for (NodeIDType node : this.nodeConfig.getActiveReplicas()) {
			InetAddress address = this.nodeConfig.getNodeAddress(node);
			if (unassigned.contains(address)) {
				newNodes.add(node);
				unassigned.remove(address);
			}
		}
		return newNodes;
	}

	public InterfaceReconfigurableNodeConfig<NodeIDType> getUnderlyingNodeConfig() {
		return this.nodeConfig;
	}
	
	// refresh consistent hash structure if changed
	private synchronized boolean refreshActives() {
		Set<NodeIDType> curActives = this.nodeConfig.getActiveReplicas();
		if (curActives.equals(this.getLastActives()))
			return false;
		this.setLastActives(curActives);
		this.CH_AR.refresh(curActives);
		return true;
	}

	// refresh consistent hash structure if changed
	private synchronized boolean refreshReconfigurators() {
		Set<NodeIDType> curReconfigurators = this.nodeConfig
				.getReconfigurators();
		// remove those slated for removal for CH ring purposes
		curReconfigurators.removeAll(this.reconfiguratorsSlatedForRemoval);
		if (curReconfigurators.equals(this.getLastReconfigurators()))
			return false;
		this.setLastReconfigurators(curReconfigurators);
		this.CH_RC.refresh(curReconfigurators);
		return true;
	}

	private synchronized Set<NodeIDType> getLastActives() {
		return this.activeReplicas;
	}

	private synchronized Set<NodeIDType> getLastReconfigurators() {
		return this.reconfigurators;
	}

	private synchronized Set<NodeIDType> setLastActives(
			Set<NodeIDType> curActives) {
		return this.activeReplicas = curActives;
	}

	private synchronized Set<NodeIDType> setLastReconfigurators(
			Set<NodeIDType> curReconfigurators) {
		return this.reconfigurators = curReconfigurators;
	}

	//@Override
	public InetSocketAddress addReconfigurator(NodeIDType id,
			InetSocketAddress sockAddr) {
		InetSocketAddress isa = this.nodeConfig.addReconfigurator(id, sockAddr);
		return isa;
	}

	//@Override
	public InetSocketAddress removeReconfigurator(NodeIDType id) {
		return this.nodeConfig.removeReconfigurator(id);
	}

	public InetSocketAddress slateForRemovalReconfigurator(NodeIDType id) {
		this.reconfiguratorsSlatedForRemoval.add(id);
		return this.getNodeSocketAddress(id);
	}
	public boolean removeSlatedForRemoval() {
		boolean removed = false;
		for(NodeIDType slated : this.reconfiguratorsSlatedForRemoval) {
			removed = removed || (this.removeReconfigurator(slated)!=null);
		}
		return removed;
	}

	@Override
	public InetSocketAddress addActiveReplica(NodeIDType id,
			InetSocketAddress sockAddr) {
		return this.nodeConfig.addActiveReplica(id, sockAddr);
	}

	@Override
	public InetSocketAddress removeActiveReplica(NodeIDType id) {
		return this.nodeConfig.removeActiveReplica(id);
	}

	@Override
	public long getVersion() {
		return this.nodeConfig.getVersion();
	}
}