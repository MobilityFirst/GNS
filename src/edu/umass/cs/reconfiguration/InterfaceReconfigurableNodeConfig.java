package edu.umass.cs.reconfiguration;

import java.util.Set;

import edu.umass.cs.nio.InterfaceNodeConfig;

/**
@author V. Arun
 * @param <NodeIDType> 
 */
public interface InterfaceReconfigurableNodeConfig<NodeIDType> extends InterfaceNodeConfig<NodeIDType> {
	/**
	 * @return Set of all active replicas.
	 */
	public Set<NodeIDType> getActiveReplicas();
	/**
	 * @return Set of all reconfigurators.
	 */
	public Set<NodeIDType> getReconfigurators();
}
