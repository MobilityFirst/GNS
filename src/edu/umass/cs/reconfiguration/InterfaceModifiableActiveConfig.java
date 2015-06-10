package edu.umass.cs.reconfiguration;

import java.net.InetSocketAddress;

/**
 * @author arun
 *
 * @param <NodeIDType>
 */
public interface InterfaceModifiableActiveConfig<NodeIDType> extends
		InterfaceReconfigurableNodeConfig<NodeIDType> {
	
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
