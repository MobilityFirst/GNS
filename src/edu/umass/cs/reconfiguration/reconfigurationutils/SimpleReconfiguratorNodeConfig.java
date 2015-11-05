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
package edu.umass.cs.reconfiguration.reconfigurationutils;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;

import edu.umass.cs.reconfiguration.interfaces.ModifiableActiveConfig;
import edu.umass.cs.reconfiguration.interfaces.ModifiableRCConfig;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableNodeConfig;

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
		ModifiableActiveConfig<NodeIDType>,
		ModifiableRCConfig<NodeIDType> {

	private final ReconfigurableNodeConfig<NodeIDType> nodeConfig;
	private int version = 0;
	private final HashMap<NodeIDType, InetSocketAddress> rcMap = new HashMap<NodeIDType, InetSocketAddress>();
        // Just for getBindAddress below
        private final HashMap<NodeIDType, InetAddress> rcBindMap = new HashMap<NodeIDType, InetAddress>();

	/**
	 * @param nc
	 */
	public SimpleReconfiguratorNodeConfig(
			ReconfigurableNodeConfig<NodeIDType> nc) {
		this.nodeConfig = nc;
		for (NodeIDType node : nc.getReconfigurators()) {
			this.rcMap.put(node, new InetSocketAddress(nc.getNodeAddress(node),
					nc.getNodePort(node)));
                        this.rcBindMap.put(node, nc.getBindAddress(node));
		}
	}

	@Override
	public synchronized Set<NodeIDType> getActiveReplicas() {
		return this.nodeConfig.getActiveReplicas();
	}

	@Override
	public synchronized Set<NodeIDType> getReconfigurators() {
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
	public InetAddress getBindAddress(NodeIDType id) {
		return this.rcMap.containsKey(id) ? this.rcBindMap.get(id)
				: (this.nodeConfig.getActiveReplicas().contains(id) ? this.nodeConfig
						.getBindAddress(id) : null);
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
	public synchronized InetSocketAddress addReconfigurator(NodeIDType id,
			InetSocketAddress sockAddr) {
		InetSocketAddress prevSockAddr = this.rcMap.put(id, sockAddr);
		if (this.nodeConfig instanceof ModifiableRCConfig)
			((ModifiableRCConfig<NodeIDType>) this.nodeConfig)
					.addReconfigurator(id, sockAddr);
                // FIXME - need to add an entry in rcBindMap as well
		return prevSockAddr;
	}

	@Override
	public synchronized InetSocketAddress removeReconfigurator(NodeIDType id) {
		InetSocketAddress removed = this.rcMap.remove(id);
                this.rcBindMap.remove(id);
		if (this.nodeConfig instanceof ModifiableRCConfig)
			((ModifiableRCConfig<NodeIDType>) this.nodeConfig)
					.removeReconfigurator(id);
                
		return removed;
	}

	@Override
	public synchronized InetSocketAddress addActiveReplica(NodeIDType id,
			InetSocketAddress sockAddr) {
		InetSocketAddress old = (this.nodeConfig.getActiveReplicas().contains(
				id) ? new InetSocketAddress(this.nodeConfig.getNodeAddress(id),
				this.nodeConfig.getNodePort(id)) : null);
		if (this.nodeConfig instanceof ModifiableActiveConfig)
			((ModifiableActiveConfig<NodeIDType>) this.nodeConfig)
					.addActiveReplica(id, sockAddr);
		else
			throw new RuntimeException(
					"Underlying nodeConfig does not implement InterfaceModifiableActiveConfig");
		return old;
	}

	@Override
	public synchronized InetSocketAddress removeActiveReplica(NodeIDType id) {
		InetSocketAddress old = (this.nodeConfig.getActiveReplicas().contains(
				id) ? new InetSocketAddress(this.nodeConfig.getNodeAddress(id),
				this.nodeConfig.getNodePort(id)) : null);
		if (this.nodeConfig instanceof ModifiableActiveConfig)
			((ModifiableActiveConfig<NodeIDType>) this.nodeConfig)
					.removeActiveReplica(id);
		else
			throw new RuntimeException(
					"Underlying nodeConfig does not implement InterfaceModifiableActiveConfig");
		return old;

	}
}
