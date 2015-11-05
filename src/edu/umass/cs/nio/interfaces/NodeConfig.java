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
package edu.umass.cs.nio.interfaces;

import java.net.InetAddress;
import java.util.Set;

/**
 * @author V. Arun
 * @param <NodeIDType>
 * 
 *            An interface to translate from integer IDs to socket addresses.
 */
public interface NodeConfig<NodeIDType> extends
		Stringifiable<NodeIDType> {

	/**
	 * @param id
	 * @return Whether the node id exists in the node config.
	 */
	public abstract boolean nodeExists(NodeIDType id);

	/**
	 * @param id
	 * @return InetAddress corresponding to {@code id}.
	 */
	public abstract InetAddress getNodeAddress(NodeIDType id);
        
        /**
	 * @param id
	 * @return Locally bindable InetAddress corresponding to {@code id}.
	 */
        public abstract InetAddress getBindAddress(NodeIDType id);

	/**
	 * @param id
	 * @return Port number corresponding to {@code id}.
	 */
	public abstract int getNodePort(NodeIDType id);

	/**
	 * @return Set of all node IDs. Avoid using this method or at least avoid
	 *         reusing the result of this method if the underlying set of nodes
	 *         can change.
	 */
	public abstract Set<NodeIDType> getNodeIDs();

}
