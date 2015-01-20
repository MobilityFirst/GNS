package edu.umass.cs.gns.reconfiguration;

import java.util.Set;

import edu.umass.cs.gns.nio.InterfaceNodeConfig;

/**
 * @author V. Arun
 * @param <NodeIDType>
 */
public interface InterfaceReconfigurableNodeConfig<NodeIDType> extends InterfaceNodeConfig<NodeIDType> {
	public Set<NodeIDType> getActiveReplicas();
	public Set<NodeIDType> getReconfigurators();
}
