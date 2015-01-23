package edu.umass.cs.gns.reconfiguration.reconfigurationutils;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import edu.umass.cs.gns.reconfiguration.InterfaceRequest;

/**
 * @author V. Arun
 */
/*
 * A utility class for maintaining demand profiles, specifically geo-distribution of demand in order
 * to send it to reconfigurators.
 */
public class AggregateDemandProfiler {
	private static final int DEFAULT_MAX_SIZE = 100000;
	private static final int DEFAULT_PLUCK_SIZE = 100;

	private final HashMap<String, AbstractDemandProfile> map = new HashMap<String, AbstractDemandProfile>();

	public synchronized AbstractDemandProfile register(
			InterfaceRequest request, InetAddress sender) {
		String name = request.getServiceName();
		AbstractDemandProfile demand = this.getDemandProfile(name);
		if (demand == null)
			demand = AbstractDemandProfile.createDemandProfile(name); // reflection
		demand.register(request, sender);
		this.map.put(name, demand);
		return demand;
	}

	private synchronized AbstractDemandProfile getDemandProfile(String name) {
		AbstractDemandProfile demand = this.map.get(name);
		return demand;
	}
	public synchronized boolean contains(String name) {
		return this.map.containsKey(name);
	}
	public synchronized void putIfEmpty(AbstractDemandProfile profile) {
		if(!this.map.containsKey(profile.getName()))
			this.map.put(profile.getName(), profile);
	}

	public synchronized ArrayList<InetAddress> shouldReconfigure(String name, ArrayList<InetAddress> curActives) {
		AbstractDemandProfile demand = this.getDemandProfile(name);
		if (demand == null)
			return null;
		return demand.shouldReconfigure(curActives);
	}

	public synchronized ArrayList<InetAddress> testAndSetReconfigured(String name, ArrayList<InetAddress> curActives) {
		AbstractDemandProfile demand = this.getDemandProfile(name);
		ArrayList<InetAddress> newActives = null;
		if (demand == null || (newActives = demand.shouldReconfigure(curActives))==null)
			return curActives;
		// else should reconfigure
		demand.justReconfigured();
		return newActives;
	}

	/*
	 * If aggregate size becomes more than DEFAULT_MAX_SIZE, this method will pluck
	 * DEFAULT_PLUCK_SIZE out so that an active replica can report it to reconfigurators. This
	 * design allows an active replica to simply use transmission to reconfigurators as an
	 * alternative to persistently storing this information locally.
	 */
	public synchronized Set<AbstractDemandProfile> trim() {
		Set<AbstractDemandProfile> plucked = new HashSet<AbstractDemandProfile>();
		if (this.map.size() >= DEFAULT_MAX_SIZE) {
			int count = 0;
			for (Iterator<Entry<String, AbstractDemandProfile>> iter = this.map
					.entrySet().iterator(); iter.hasNext(); /* nothing */) {
				Entry<String, AbstractDemandProfile> entry = iter.next();
				plucked.add(entry.getValue());
				iter.remove();
				if (count++ >= DEFAULT_PLUCK_SIZE)
					break;
			}
		}
		return plucked;
	}

	public synchronized AbstractDemandProfile pluckDemandProfile(String name) {
		AbstractDemandProfile demand = this.map.get(name);
		AbstractDemandProfile copy = demand.clone();
		demand.reset();
		this.map.put(name, demand);
		return copy;
	}

	public synchronized AbstractDemandProfile combine(
			AbstractDemandProfile update) {
		AbstractDemandProfile existing = this.map.get(update.getName());
		if (existing != null)
			existing.combine(update);
		else
			existing = update;
		this.map.put(update.getName(), existing);
		return existing;
	}
}
