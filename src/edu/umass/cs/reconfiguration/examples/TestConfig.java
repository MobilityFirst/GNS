package edu.umass.cs.reconfiguration.examples;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import edu.umass.cs.gns.util.Util;


/**
 * @author V. Arun
 */
@SuppressWarnings("javadoc")
public class TestConfig {

	public static enum ServerSelectionPolicy {FIRST, RANDOM};
	public static final ServerSelectionPolicy serverSelectionPolicy = ServerSelectionPolicy.FIRST;

	public static final String[] DEFAULT_RECONFIGURATORS = { "127.0.0.1:3100",
			"127.0.0.1:3101", "127.0.0.1:3102"};
	
	public static final String[] DEFAULT_ACTIVES = { "127.0.0.1:2100",
		"127.0.0.1:2101", "127.0.0.1:2102", "127.0.0.1:2103",
		"127.0.0.1:2104" };
	
	public static Set<InetSocketAddress> getReconfigurators() {
		Set<InetSocketAddress> reconfigurators = new HashSet<InetSocketAddress>();
		for(String sockAddrStr : DEFAULT_RECONFIGURATORS) 
			reconfigurators.add(Util.getInetSocketAddressFromString(sockAddrStr));
		return reconfigurators;
	}
	public static Set<InetSocketAddress> getActiveReplicas() {
		Set<InetSocketAddress> actives = new HashSet<InetSocketAddress>();
		for(String sockAddrStr : DEFAULT_ACTIVES) 
			actives.add(Util.getInetSocketAddressFromString(sockAddrStr));
		return actives;
	}
	
	public static final int numNodes = 3;
	public static final int startNodeID = 100;
	public static Set<Integer> getNodes() {
		TreeSet<Integer> nodes = new TreeSet<Integer>();
		for(int i=startNodeID; i<startNodeID+numNodes; i++) {
			nodes.add(i);
		}
		return nodes;
	}
}
