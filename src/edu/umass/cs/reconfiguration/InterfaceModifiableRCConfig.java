package edu.umass.cs.reconfiguration;

import java.net.InetSocketAddress;

/* This class exists primarily to introduce methods needed but missing
 * in InterfaceReconfigurableNodeConfig in a backwards compatible 
 * manner. 
 */
public interface InterfaceModifiableRCConfig<NodeIDType> extends
		InterfaceReconfigurableNodeConfig<NodeIDType> {

	// add/remove methods return old InetSocketAddress if any	
	public InetSocketAddress addReconfigurator(NodeIDType id, InetSocketAddress sockAddr);
	public InetSocketAddress removeReconfigurator(NodeIDType id);

}
