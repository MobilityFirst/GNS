package edu.umass.cs.reconfiguration.reconfigurationutils;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;

import edu.umass.cs.nio.InterfaceNodeConfig;

/**
 *  This class isn't really used for anything other than as 
 * a parent for ConsistentReconfigurableNodeConfig, so it
 * has been declared abstract (even though it has no 
 * abstract methods).
 * @param <NodeIDType> 
 */
public abstract class ConsistentNodeConfig<NodeIDType> implements
		InterfaceNodeConfig<NodeIDType> {

	private final InterfaceNodeConfig<NodeIDType> nodeConfig;
	private Set<NodeIDType> nodes; // most recent cached copy

	private final ConsistentHashing<NodeIDType> CH; // need to refresh when nodeConfig changes

	/**
	 * @param nc
	 */
	public ConsistentNodeConfig(InterfaceNodeConfig<NodeIDType> nc) {
		this.nodeConfig = nc;
		this.nodes = this.nodeConfig.getNodeIDs();
		this.CH = new ConsistentHashing<NodeIDType>(this.nodes);
	}

	private synchronized boolean refresh() {
		Set<NodeIDType> curActives = this.nodeConfig.getNodeIDs();
		if (curActives.equals(this.nodes))
			return false;
		this.nodes = (curActives);
		this.CH.refresh(curActives);
		return true;
	}

	/**
	 * @param name
	 * @return Consecutive servers on the consistent hash ring to which
	 * this name hashes returned as an array.
	 */
	public Set<NodeIDType> getReplicatedServers(String name) {
		refresh();
		return this.CH.getReplicatedServers(name);
	}

	@Override
	public boolean nodeExists(NodeIDType id) {
		return this.nodeConfig.nodeExists(id);
	}

	@Override
	public InetAddress getNodeAddress(NodeIDType id) {
		return this.nodeConfig.getNodeAddress(id);
	}
        
        @Override
	public InetAddress getBindAddress(NodeIDType id) {
		return this.nodeConfig.getBindAddress(id);
	}

	@Override
	public int getNodePort(NodeIDType id) {
		return this.nodeConfig.getNodePort(id);
	}
	
	/**
	 * @param id
	 * @return Socket address corresponding to node {@code id}.
	 */
	public InetSocketAddress getNodeSocketAddress(NodeIDType id) {
		InetAddress ip = this.getNodeAddress(id);
		return (ip!=null ? new InetSocketAddress(ip, this.getNodePort(id)) : null);
	}

	// FIXME: disallow the use of this method
	@Override
	public Set<NodeIDType> getNodeIDs() {
		throw new RuntimeException("The use of this method is not permitted");
		//return this.nodeConfig.getNodeIDs();
	}

	@Override
	public NodeIDType valueOf(String strValue) {
		return this.nodeConfig.valueOf(strValue);
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
}
