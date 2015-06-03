package edu.umass.cs.reconfiguration;

import java.util.Set;

import edu.umass.cs.nio.InterfaceNodeConfig;

/**
@author V. Arun
 */
public interface InterfaceReconfigurableNodeConfig<NodeIDType> extends InterfaceNodeConfig<NodeIDType> {
	public Set<NodeIDType> getActiveReplicas();
	public Set<NodeIDType> getReconfigurators();
}
