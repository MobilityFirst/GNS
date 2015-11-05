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
package edu.umass.cs.reconfiguration.interfaces;

import java.net.InetSocketAddress;

/**
 * @author arun
 *
 * @param <NodeIDType>
 */
public interface ModifiableActiveConfig<NodeIDType> extends
		ReconfigurableNodeConfig<NodeIDType> {
	
	/**
	 * @param id
	 * @param sockAddr
	 * @return Socket address previously mapped to this id. But we really should
	 * not allow mappings to be changed via an add method. 
	 */
	public InetSocketAddress addActiveReplica(NodeIDType id, InetSocketAddress sockAddr);
	/**
	 * @param id
	 * @return Socket address to which {@code id} was mapped.
	 */
	public InetSocketAddress removeActiveReplica(NodeIDType id);
	
	/**
	 * @return Version number of node config.
	 */
	public long getVersion();
}
