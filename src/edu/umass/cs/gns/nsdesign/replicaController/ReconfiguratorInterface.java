package edu.umass.cs.gns.nsdesign.replicaController;

import java.util.concurrent.ConcurrentHashMap;

import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;

/**
@author V. Arun
 */
public interface ReconfiguratorInterface {
	public GNSNodeConfig getGnsNodeConfig();

	public ConcurrentHashMap<NodeId<String>, Double> getNsRequestRates();

}
