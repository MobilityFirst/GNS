package edu.umass.cs.reconfiguration.reconfigurationutils;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;

import edu.umass.cs.reconfiguration.InterfaceModifiableActiveConfig;
import edu.umass.cs.reconfiguration.InterfaceModifiableRCConfig;
import edu.umass.cs.reconfiguration.InterfaceReconfigurableNodeConfig;

/**
 * @author arun
 *
 * @param <NodeIDType>
 * 
 *  
 *            The point of this class is to retain the supplied nodeConfig only
 *            for actives so that the supplier can change them at will. We make
 *            a separate copy of the reconfigurators as they have to be changed
 *            carefully.
 * 
 *            As the purpose of this class is also to serve as the "underlying"
 *            node config to ConsistentNodeConfig, we allow getNodeIDs() to be
 *            called here.
 */
public class SimpleReconfiguratorNodeConfig<NodeIDType> implements
		InterfaceModifiableActiveConfig<NodeIDType>,
		InterfaceModifiableRCConfig<NodeIDType> {

	private final InterfaceReconfigurableNodeConfig<NodeIDType> nodeConfig;
	private int version = 0;
	private final HashMap<NodeIDType, InetSocketAddress> rcMap = new HashMap<NodeIDType, InetSocketAddress>();

	/**
	 * @param nc
	 */
	public SimpleReconfiguratorNodeConfig(
			InterfaceReconfigurableNodeConfig<NodeIDType> nc) {
		this.nodeConfig = nc;
		for (NodeIDType node : nc.getReconfigurators()) {
			this.rcMap.put(node, new InetSocketAddress(nc.getNodeAddress(node),
					nc.getNodePort(node)));
		}
	}

	@Override
	public Set<NodeIDType> getActiveReplicas() {
		return this.nodeConfig.getActiveReplicas();
	}

	@Override
	public Set<NodeIDType> getReconfigurators() {
		return new HashSet<NodeIDType>(this.rcMap.keySet());
	}

	@Override
	public boolean nodeExists(NodeIDType id) {
		return this.rcMap.containsKey(id)
				|| this.nodeConfig.getActiveReplicas().contains(id);
	}

	@Override
	public InetAddress getNodeAddress(NodeIDType id) {
		return this.rcMap.containsKey(id) ? this.rcMap.get(id).getAddress()
				: (this.nodeConfig.getActiveReplicas().contains(id) ? this.nodeConfig
						.getNodeAddress(id) : null);
	}

	@Override
	public int getNodePort(NodeIDType id) {
		return this.rcMap.containsKey(id) ? this.rcMap.get(id).getPort()
				: (this.nodeConfig.getActiveReplicas().contains(id) ? this.nodeConfig
						.getNodePort(id) : -1);
	}

	@Override
	public Set<NodeIDType> getNodeIDs() {
		return this.nodeConfig.getNodeIDs();
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

	@Override
	public long getVersion() {
		return this.version;
	}

	@Override
	public InetSocketAddress addReconfigurator(NodeIDType id,
			InetSocketAddress sockAddr) {
		InetSocketAddress prevSockAddr = this.rcMap.put(id, sockAddr);
		return prevSockAddr;
	}

	// @Override
	public InetSocketAddress removeReconfigurator(NodeIDType id) {
		return this.rcMap.remove(id);
	}

	@Override
	public InetSocketAddress addActiveReplica(NodeIDType id,
			InetSocketAddress sockAddr) {
		InetSocketAddress old = (this.nodeConfig.getActiveReplicas().contains(
				id) ? new InetSocketAddress(this.nodeConfig.getNodeAddress(id),
				this.nodeConfig.getNodePort(id)) : null);
		if (this.nodeConfig instanceof InterfaceModifiableActiveConfig)
			((InterfaceModifiableActiveConfig<NodeIDType>) this.nodeConfig)
					.addActiveReplica(id, sockAddr);
		else
			throw new RuntimeException(
					"Underlying nodeConfig does not implement InterfaceModifiableActiveConfig");
		return old;
	}

	@Override
	public InetSocketAddress removeActiveReplica(NodeIDType id) {
		InetSocketAddress old = (this.nodeConfig.getActiveReplicas().contains(
				id) ? new InetSocketAddress(this.nodeConfig.getNodeAddress(id),
				this.nodeConfig.getNodePort(id)) : null);
		if (this.nodeConfig instanceof InterfaceModifiableActiveConfig)
			((InterfaceModifiableActiveConfig<NodeIDType>) this.nodeConfig)
					.removeActiveReplica(id);
		else
			throw new RuntimeException(
					"Underlying nodeConfig does not implement InterfaceModifiableActiveConfig");
		return old;

	}
}
