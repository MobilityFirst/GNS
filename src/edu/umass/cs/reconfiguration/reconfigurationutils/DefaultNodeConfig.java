package edu.umass.cs.reconfiguration.reconfigurationutils;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.json.JSONArray;
import org.json.JSONException;

import edu.umass.cs.reconfiguration.interfaces.ModifiableActiveConfig;
import edu.umass.cs.reconfiguration.interfaces.ModifiableRCConfig;

/**
 * @author arun
 *
 * @param <NodeIDType>
 */
public class DefaultNodeConfig<NodeIDType> implements
		ModifiableRCConfig<NodeIDType>,
		ModifiableActiveConfig<NodeIDType> {

	final ConcurrentHashMap<NodeIDType, InetSocketAddress> actives = new ConcurrentHashMap<NodeIDType, InetSocketAddress>();
	final ConcurrentHashMap<NodeIDType, InetSocketAddress> reconfigurators = new ConcurrentHashMap<NodeIDType, InetSocketAddress>();

	/**
	 * @param actives
	 * @param reconfigurators
	 */
	public DefaultNodeConfig(Map<NodeIDType, InetSocketAddress> actives,
			Map<NodeIDType, InetSocketAddress> reconfigurators) {
		this.actives.putAll(actives);
		this.reconfigurators.putAll(reconfigurators);
	}

	@Override
	public Set<NodeIDType> getActiveReplicas() {
		return new HashSet<NodeIDType>(this.actives.keySet());
	}

	@Override
	public Set<NodeIDType> getReconfigurators() {
		return new HashSet<NodeIDType>(this.reconfigurators.keySet());
	}

	@Override
	public boolean nodeExists(NodeIDType id) {
		return this.actives.containsKey(id)
				|| this.reconfigurators.containsKey(id);
	}

	@Override
	public InetAddress getNodeAddress(NodeIDType id) {
		return this.actives.containsKey(id) ? this.actives.get(id).getAddress()
				: this.reconfigurators.containsKey(id) ? this.reconfigurators
						.get(id).getAddress() : null;
	}

	@Override
	public InetAddress getBindAddress(NodeIDType id) {
		return this.getNodeAddress(id);
	}

	@Override
	public int getNodePort(NodeIDType id) {
		return this.actives.containsKey(id) ? this.actives.get(id).getPort()
				: this.reconfigurators.containsKey(id) ? this.reconfigurators
						.get(id).getPort() : null;
	}

	@Override
	public Set<NodeIDType> getNodeIDs() {
		Set<NodeIDType> nodes = this.getActiveReplicas();
		nodes.addAll(this.getReconfigurators());
		return nodes;
	}

	@SuppressWarnings("unchecked")
	@Override
	public NodeIDType valueOf(String nodeAsString) {
		NodeIDType node = null;
		Iterator<NodeIDType> nodeIter = this.actives.keySet().iterator();
		if (nodeIter.hasNext() && (node = nodeIter.next()) != null) {
			if (node instanceof String) {
				return (NodeIDType) nodeAsString;
			} else if (node instanceof Integer) {
				return (NodeIDType) (Integer.valueOf(nodeAsString.trim()));
			} else if (node instanceof InetAddress) {
				try {
					return (NodeIDType) (InetAddress.getByName(nodeAsString
							.trim()));
				} catch (UnknownHostException e) {
					e.printStackTrace();
				}
			}
		}
		return null;
	}

	@Override
	public Set<NodeIDType> getValuesFromStringSet(Set<String> strNodes) {
		Set<NodeIDType> nodes = new HashSet<NodeIDType>();
		for (String strNode : strNodes) {
			NodeIDType node = this.valueOf(strNode);
			if (node != null)
				nodes.add(node);
		}
		return nodes;
	}

	@Override
	public Set<NodeIDType> getValuesFromJSONArray(JSONArray array)
			throws JSONException {
		Set<NodeIDType> nodes = new HashSet<NodeIDType>();
		for (int i = 0; i < array.length(); i++) {
			NodeIDType node = this.valueOf(array.getString(i).toString());
			if (node != null)
				nodes.add(node);
		}
		return nodes;
	}

	@Override
	public InetSocketAddress addActiveReplica(NodeIDType id,
			InetSocketAddress sockAddr) {
		return this.actives.putIfAbsent(id, sockAddr);
	}

	@Override
	public InetSocketAddress removeActiveReplica(NodeIDType id) {
		return this.actives.remove(id);
	}

	@Override
	public long getVersion() {
		return 0;
	}

	@Override
	public InetSocketAddress addReconfigurator(NodeIDType id,
			InetSocketAddress sockAddr) {
		return this.reconfigurators.putIfAbsent(id, sockAddr);
	}

	@Override
	public InetSocketAddress removeReconfigurator(NodeIDType id) {
		return this.reconfigurators.remove(id);
	}
	
	public String toString() {
		String s="";
		for(NodeIDType id : this.getNodeIDs()) {
			s = (s + id+":"+this.getNodeAddress(id)+":"+this.getNodePort(id) + " " );
		}
		return s;
	}
}
