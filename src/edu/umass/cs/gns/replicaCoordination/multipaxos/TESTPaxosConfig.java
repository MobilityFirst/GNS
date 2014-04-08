package edu.umass.cs.gns.replicaCoordination.multipaxos;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeSet;

import edu.umass.cs.gns.nio.DefaultPacketDemultiplexer;
import edu.umass.cs.gns.nio.GNSNIOTransport;
import edu.umass.cs.gns.nio.JSONMessageExtractor;
import edu.umass.cs.gns.nio.SampleNodeConfig;
import edu.umass.cs.gns.nsdesign.Replicable;

/**
@author V. Arun
 */
public class TESTPaxosConfig {
	public static final boolean MEMORY_TESTING=true;
	public static final int MAX_TEST_REQS = 1000000;
	public static final int MAX_NODE_ID = 10000;
	public static final String TEST_GUID_PREFIX = "paxos";
	public static final String TEST_GUID = "paxos0";
	public static final int MAX_CONFIG_GROUPS = 10;
	
	private static final SampleNodeConfig nodeConfig = new SampleNodeConfig();
	private static final HashMap<String,int[]> groups = new HashMap<String,int[]>();
	private static final TreeSet<Integer> nodes = new TreeSet<Integer>();
	private static final int[] defaultGroup = {100, 101, 102};
	public static final int TEST_CLIENT_ID = 200;
	
	private static PaxosManager[] pms;
	private static Replicable[] apps;
	
	private static ArrayList<Integer> failedNodes = new ArrayList<Integer>();
	private static boolean[] committed = new boolean[MAX_TEST_REQS];
	private static boolean[] executedAtAll = new boolean[MAX_TEST_REQS];
	private static boolean[] recovered = new boolean[MAX_NODE_ID];

	public void startPaxosManager(int id, Replicable app) {
		try {
		PaxosManager pm = new PaxosManager(id, nodeConfig, new GNSNIOTransport(id, TESTPaxosConfig.getNodeConfig(), 
				new JSONMessageExtractor(new DefaultPacketDemultiplexer())), app, null);
		} catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}
	public static void setDefaultGroups(int numGroups) {
		for(int i=0; i<Math.min(MAX_CONFIG_GROUPS, numGroups); i++) {
			groups.put(TEST_GUID_PREFIX+i, defaultGroup);
		}
	}
	public static int[] getGroup(String groupID) {
		int[] members = groups.get(groupID);
		return members!=null ? members : defaultGroup;
	}
	public static int[] getGroup(int groupID) {
		int[] members = groups.get(TEST_GUID_PREFIX+groupID);
		return members!=null ? members : defaultGroup;
	}
	public static void createGroup(String groupID, int[] members) {
		if(groups.size() <= MAX_CONFIG_GROUPS) groups.put(groupID, members);
	}
	
	public static SampleNodeConfig getNodeConfig() {
		return nodeConfig;
	}
	public synchronized static void crash(int nodeID) {
		TESTPaxosConfig.failedNodes.add(nodeID);
	}
	public synchronized static void recover(int nodeID) {
		TESTPaxosConfig.failedNodes.remove(new Integer(nodeID));
	}
	public synchronized static boolean isCrashed(int nodeID) {
		return TESTPaxosConfig.failedNodes.contains(nodeID);
	}
	public synchronized static void setRecovered(int id, String paxosID, boolean b) {
		assert(id < MAX_NODE_ID);
		if(paxosID.equals(TEST_GUID)) {
			System.out.println("Node " + id + " has recovered");
			recovered[id] = b;
		}
	}
	public synchronized static boolean getRecovered(int id, String paxosID) {
		assert(id < MAX_NODE_ID);
		if(paxosID.equals(TEST_GUID)) return recovered[id];
		else return true;
	}

	public synchronized static boolean isCommitted(int reqnum) {
		assert(reqnum < MAX_TEST_REQS);
		return committed[reqnum];
	}
	public synchronized static boolean isExecutedAtAll(int reqnum) {
		assert(reqnum < MAX_TEST_REQS);
		return executedAtAll[reqnum];
	}
	public synchronized static void execute(int reqnum) {
		executedAtAll[reqnum] = true;
	}
	public synchronized static void commit(int reqnum) {
		committed[reqnum] = true;
	}
	
	public static void main(String[] args) {
		assert(!TESTPaxosConfig.isCrashed(100));
		TESTPaxosConfig.crash(100);
		assert(TESTPaxosConfig.isCrashed(100));
		TESTPaxosConfig.recover(100);
		assert(!TESTPaxosConfig.isCrashed(100));		
	}
}
