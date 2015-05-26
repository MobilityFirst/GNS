package edu.umass.cs.gns.nsdesign.replicaController;

import java.util.concurrent.ConcurrentHashMap;

import edu.umass.cs.gns.nodeconfig.GNSNodeConfig;

/**
@author V. Arun
 */
@Deprecated
public interface ReconfiguratorInterface<NodeIDType> {
	public GNSNodeConfig<NodeIDType> getGnsNodeConfig();

	public ConcurrentHashMap<NodeIDType, Double> getNsRequestRates();

}
