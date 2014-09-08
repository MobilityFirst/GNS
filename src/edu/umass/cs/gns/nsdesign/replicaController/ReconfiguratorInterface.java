package edu.umass.cs.gns.nsdesign.replicaController;

import java.util.concurrent.ConcurrentHashMap;

import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;

/**
@author V. Arun
 */
public interface ReconfiguratorInterface {
	public GNSNodeConfig getGnsNodeConfig();

	public ConcurrentHashMap<Integer, Double> getNsRequestRates();

}
