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
package edu.umass.cs.nio.nioutils;

import edu.umass.cs.nio.interfaces.NodeConfig;

/**
 * @author arun
 *
 * @param <NodeIDType>
 */
public interface InterfaceDelayEmulator<NodeIDType> extends NodeConfig<NodeIDType> {

	/**
	 * @param node2
	 * @return The emulated delay in milliseconds to node2.
	 */
	public long getEmulatedDelay(NodeIDType node2);
}
