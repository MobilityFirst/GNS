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
import java.util.ArrayList;

/**
 * Get IP addresses of all active replica nodes. Demand profile implementations
 * may need this information in order to implement some placement policies.
 */
public interface InterfaceGetActiveIPs {

	/**
	 * @return Array of IP addresses corresponding to all active replica nodes.
	 */
	public ArrayList<InetAddress> getActiveIPs();

}
