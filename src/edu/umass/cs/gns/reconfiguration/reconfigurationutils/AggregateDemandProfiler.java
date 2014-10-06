package edu.umass.cs.gns.reconfiguration.reconfigurationutils;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Set;

import edu.umass.cs.gns.reconfiguration.InterfaceRequest;

/**
@author V. Arun
 */
/* A utility class for maintaining demand profiles, specifically
 * geo-distribution of demand in order to send it to reconfigurators.
 */
public class AggregateDemandProfiler {
	private final HashMap<String,DemandProfile> map = new HashMap<String,DemandProfile>();
		
	public synchronized DemandProfile register(InterfaceRequest request, InetAddress sender) {
		String name = request.getServiceName();
		DemandProfile demand = this.getDemandProfile(name);
		if(demand==null) demand = new DemandProfile(name);
		demand.register(request, sender);
		this.map.put(name, demand);
		return demand;
	}
	private synchronized DemandProfile getDemandProfile(String name) {
		DemandProfile demand = this.map.get(name);
		return demand;
	}
	// FIXME: aggregate size based policy
	public synchronized Set<DemandProfile> trim() {
		return null;
	}
	public synchronized DemandProfile pluckDemandProfile(String name) {
		DemandProfile demand = this.map.get(name);
		DemandProfile copy = new DemandProfile(demand);
		demand.reset();
		this.map.put(name, demand);
		return copy;
	}
}
