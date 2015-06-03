package edu.umass.cs.reconfiguration;

import java.net.InetSocketAddress;

public interface InterfaceModifiableActiveConfig<NodeIDType> extends
		InterfaceReconfigurableNodeConfig<NodeIDType> {
	
	public InetSocketAddress addActiveReplica(NodeIDType id, InetSocketAddress sockAddr);
	public InetSocketAddress removeActiveReplica(NodeIDType id);
	
	public long getVersion();
}
