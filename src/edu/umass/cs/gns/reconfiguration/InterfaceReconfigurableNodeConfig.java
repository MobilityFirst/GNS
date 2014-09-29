package edu.umass.cs.gns.reconfiguration;

import java.util.Set;

import edu.umass.cs.gns.nsdesign.nodeconfig.InterfaceNodeConfig;

/**
@author V. Arun
 */
public interface InterfaceReconfigurableNodeConfig<NodeIDType> extends InterfaceNodeConfig<NodeIDType> {
	public Set<NodeIDType> getActiveReplicas();
	public Set<NodeIDType> getReconfigurators();
}
