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
