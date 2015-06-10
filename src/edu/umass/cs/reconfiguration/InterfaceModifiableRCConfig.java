package edu.umass.cs.reconfiguration;

import java.net.InetSocketAddress;

/**
 * This class exists primarily to introduce methods needed but missing in
 * InterfaceReconfigurableNodeConfig in a backwards compatible manner.
 * 
 * @param <NodeIDType>
 * 
 */

public interface InterfaceModifiableRCConfig<NodeIDType> extends
		InterfaceReconfigurableNodeConfig<NodeIDType> {

	/**
	 * @param id
	 * @param sockAddr
	 * @return Socket address of previous mapping if any. FIXME: We really
	 * shouldn't be allowing remapping via an add method.
	 */
	public InetSocketAddress addReconfigurator(NodeIDType id,
			InetSocketAddress sockAddr);

	/**
	 * @param id
	 * @return Socket address to which {@code id} was mapped.
	 */
	public InetSocketAddress removeReconfigurator(NodeIDType id);

}
