package edu.umass.cs.gns.reconfiguration.reconfigurationutils;

import java.net.InetAddress;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;

import edu.umass.cs.gns.nio.InterfaceNodeConfig;

public class ConsistentNodeConfig<NodeIDType> implements
		InterfaceNodeConfig<NodeIDType> {

	private final InterfaceNodeConfig<NodeIDType> nodeConfig;
	private Set<NodeIDType> nodes; // most recent cached copy

	private final ConsistentHashing<NodeIDType> CH; // need to refresh when nodeConfig changes

	public ConsistentNodeConfig(InterfaceNodeConfig<NodeIDType> nc) {
		this.nodeConfig = nc;
		this.nodes = this.nodeConfig.getNodeIDs();
		this.CH = new ConsistentHashing<NodeIDType>(this.nodes.toArray());
	}

	private synchronized boolean refresh() {
		Set<NodeIDType> curActives = this.nodeConfig.getNodeIDs();
		if (curActives.equals(this.nodes))
			return false;
		this.nodes = (curActives);
		this.CH.refresh(curActives.toArray());
		return true;
	}

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
	public int getNodePort(NodeIDType id) {
		return this.nodeConfig.getNodePort(id);
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
