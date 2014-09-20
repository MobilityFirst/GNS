package edu.umass.cs.gns.replicaCoordination.multipaxos;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import edu.umass.cs.gns.nio.nioutils.SampleNodeConfig;
import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;

/**
@author V. Arun
 */
public class TESTPaxosConfig {
	public static final boolean DEBUG = PaxosManager.DEBUG;
	public static final boolean MEMORY_TESTING=true;
	public static final boolean DISK_ACCESS_TESTING=true;

	public static final int MAX_TEST_REQS = 1000000;
	private static final int RANDOM_SEED = 3142;
	private static final double NODE_INCLUSION_PROB = 0.6;

	public static final int MILLION = 1000000;

	private static boolean TEST_ASSERT_ENABLED = false;

	public static final boolean TEST_WITH_RECOVERY = true;

	public static final int MAX_NODE_ID = 10000;
	public static final int TEST_START_NODE_ID = 100; // nodeID's can start from a non-zero value. FIXME: make them non-consecutive as well
	public static final int NUM_NODES = 10;

	public static final String TEST_GUID_PREFIX = "paxos";
	public static final String TEST_GUID = "paxos0";
	public static final int MAX_CONFIG_GROUPS = 10;
	
	/**************** Number of paxos groups *******************/
	public static final int NUM_GROUPS = 100000;  // NUM_GROUPS could be set to much greater than MAX_CONFIG_GROUPS
	/***********************************************************/

	/**************** Load parameters *******************/
	public static final int NUM_CLIENTS = 10; // 1;// 4 default
	public static final int NUM_REQUESTS = 1000; // 20;  // 40000 default
	public static final int NUM_REQUESTS_PER_CLIENT = NUM_REQUESTS/NUM_CLIENTS;
	public static final double TOTAL_LOAD = 1000; // 2000 reqs/sec default (across all clients)
	/***********************************************************/

	private static final SampleNodeConfig<NodeId<String>> nodeConfig = new SampleNodeConfig<NodeId<String>>();
	//private static final TreeSet<Integer> nodes = new TreeSet<Integer>();
	static {for(int i=TEST_START_NODE_ID; i<TEST_START_NODE_ID+NUM_NODES; i++) nodeConfig.addLocal(new NodeId<String>(i));}

	private static final HashMap<String, NodeId<String>[]> groups = new HashMap<String, NodeId<String>[]>();
	static {setDefaultGroups(MAX_CONFIG_GROUPS);}
	//static {setRandomGroups(MAX_CONFIG_GROUPS);}

	private static final NodeId[] defaultGroup = {new NodeId<String>(TEST_START_NODE_ID), 
          new NodeId<String>(TEST_START_NODE_ID+1), 
          new NodeId<String>(TEST_START_NODE_ID+2)};
	public static final int TEST_CLIENT_ID = 200;

	private static boolean reply_to_client = true;

	private static boolean clean_db = false;
	
	static{assert(NUM_CLIENTS <= MAX_CONFIG_GROUPS);} // all tests should be with at most MAX_CONFIG_GROUPS

	private static ArrayList<NodeId<String>> failedNodes = new ArrayList<NodeId<String>>();
	//static {crash(TEST_START_NODE_ID);} // by default, first node is always crashed

	private static boolean[] committed = new boolean[MAX_TEST_REQS];
	private static boolean[] executedAtAll = new boolean[MAX_TEST_REQS];
	private static boolean[] recovered = new boolean[MAX_NODE_ID];

	public static void setCleanDB(boolean b) {clean_db=b;}
	public static boolean getCleanDB() {return clean_db;}

	public static Set<NodeId<String>> getNodes() {return nodeConfig.getNodes();}

	public static void setSendReplyToClient(boolean b) {reply_to_client=b;}
	public static boolean getSendReplyToClient() {return reply_to_client;}

	/******************** Distributed settings **************************
	 * FIXME: Need to take these from a config properties file.
	 */
	public static void setDistributedClients() {
		try {
			for(int i=0; i<NUM_CLIENTS; i++) 
				TESTPaxosConfig.getNodeConfig().add(TESTPaxosConfig.TEST_CLIENT_ID+i, InetAddress.getByName("fig.cs.umass.edu"));
		} catch(UnknownHostException e) {e.printStackTrace();}
	}
	public static void setDistributedServers() {
		try {
			TESTPaxosConfig.getNodeConfig().add(0, InetAddress.getByName("localhost"));
			TESTPaxosConfig.getNodeConfig().add(TESTPaxosConfig.TEST_START_NODE_ID, InetAddress.getByName("date.cs.umass.edu"));
			TESTPaxosConfig.getNodeConfig().add(TESTPaxosConfig.TEST_START_NODE_ID+1, InetAddress.getByName("plum.cs.umass.edu"));
			TESTPaxosConfig.getNodeConfig().add(TESTPaxosConfig.TEST_START_NODE_ID+2, InetAddress.getByName("pear.cs.umass.edu"));
		} catch(UnknownHostException e) {e.printStackTrace();}
	}
	/******************** End of distributed settings **************************/

	public static void setLocalServers() {
		for(int i=0; i<NUM_NODES; i++) 
			TESTPaxosConfig.getNodeConfig().addLocal(TESTPaxosConfig.TEST_START_NODE_ID+i);
	}
	public static void setLocalClients() {
		for(int i=0; i<NUM_CLIENTS; i++) 
			TESTPaxosConfig.getNodeConfig().addLocal(TESTPaxosConfig.TEST_CLIENT_ID+i);
	}

	public static void setDefaultGroups(int numGroups) {
		for(int i=0; i<Math.min(MAX_CONFIG_GROUPS, numGroups); i++) {
			groups.put(TEST_GUID_PREFIX+i, defaultGroup);
		}
	}
	// Sets consistent, random groups starting with the same random seed
	public static void setRandomGroups(int numGroups) {
		Random r = new Random(RANDOM_SEED);
		for(int i=0; i<Math.min(MAX_CONFIG_GROUPS, numGroups); i++) {
			groups.put(TEST_GUID_PREFIX+i, defaultGroup);
			if(i==0) continue;// first group is always default group
			TreeSet<NodeId<String>> members = new TreeSet<NodeId<String>>();
			for(NodeId<String> id : TESTPaxosConfig.getNodes()) {
				if(r.nextDouble() > NODE_INCLUSION_PROB) {
					members.add(id);
				}
			}
			TESTPaxosConfig.setGroup(TESTPaxosConfig.getGroupName(i), members);
		}
	}
	public static void setGroup(String groupID, Set<NodeId<String>> members) {
		NodeId<String>[] array = new NodeId[members.size()];
		int j=0; for(NodeId<String> id : members) array[j++] = id;
		groups.put(groupID, array);
	}

	public static void setGroup(String groupID, NodeId<String>[] members) {
		groups.put(groupID, members);
	}
	public static NodeId<String>[] getDefaultGroup() {
		return defaultGroup;
	}
	public static NodeId<String>[] getGroup(String groupID) {
		NodeId<String>[] members = groups.get(groupID);
		return members!=null ? members : defaultGroup;
	}
	public static NodeId<String>[] getGroup(int groupID) {
		NodeId<String>[] members = groups.get(TEST_GUID_PREFIX+groupID);
		return members!=null ? members : defaultGroup;
	}
	public static String getGroupName(int groupID) {
		return TEST_GUID_PREFIX + groupID;
	}
	public static Collection<String> getGroups() {
		return groups.keySet();
	}
	public static void createGroup(String groupID, NodeId<String>[] members) {
		if(groups.size() <= MAX_CONFIG_GROUPS) groups.put(groupID, members);
	}

	public static SampleNodeConfig getNodeConfig() {
		return nodeConfig;
	}
	public synchronized static void crash(NodeId<String> nodeID) {
		TESTPaxosConfig.failedNodes.add(nodeID);
	}
	public synchronized static void recover(NodeId<String> nodeID) {
		TESTPaxosConfig.failedNodes.remove(nodeID);
	}
	public synchronized static boolean isCrashed(NodeId<String>nodeID) {
		return TESTPaxosConfig.failedNodes.contains(nodeID);
	}
	public synchronized static void setRecovered(NodeId<String> id, String paxosID, boolean b) {
                //FIXME: removed this comparison which doesn't make sense for string NodeIds
		//assert(id.compareTo(new NodeId<String>(MAX_NODE_ID)) < 0) : " id = "+id + ", MAX_NODE_ID = " + MAX_NODE_ID;
		if(paxosID.equals(TEST_GUID)) {
                  // FIXME OR NOT: this will break if the id isn't an integer string
                  recovered[Integer.parseInt(id.get())] = b;
		}
	}
	public synchronized static boolean getRecovered(NodeId<String> id, String paxosID) {
                //FIXME: removed this comparison which doesn't make sense for string NodeIds
		//assert(id.compareTo(new NodeId<String>(MAX_NODE_ID)) < 0);
                // FIXME OR NOT: this will break if the id isn't an integer string
		if(paxosID.equals(TEST_GUID)) return recovered[Integer.parseInt(id.get())];
		else return true;
	}

	public synchronized static boolean isCommitted(int reqnum) {
		assert(reqnum < MAX_TEST_REQS);
		return committed[reqnum];
	}
	public synchronized static void execute(int reqnum) {
		assert(reqnum>=0);
		executedAtAll[reqnum] = true;
	}
	public synchronized static void commit(int reqnum) {
		if(reqnum>=0 && reqnum<committed.length)
			committed[reqnum] = true;
	}
	public static void testAssert(boolean b) { assert(!TEST_ASSERT_ENABLED || b);}

	// Checks if the IP specified for the id argument is local
	public static boolean findMyIP(NodeId<String> myID) throws SocketException {
		Enumeration<NetworkInterface> netfaces = NetworkInterface.getNetworkInterfaces();
		ArrayList<InetAddress> myIPs = new ArrayList<InetAddress>();
		while(netfaces.hasMoreElements()) {
			NetworkInterface iface = netfaces.nextElement();
			Enumeration<InetAddress> allIPs = iface.getInetAddresses();
			while(allIPs.hasMoreElements()) {
				InetAddress addr = allIPs.nextElement();
				if((addr instanceof Inet4Address)) myIPs.add((InetAddress)addr);
			}
		}
		System.out.println(myIPs);
		boolean found = false;
		if(myIPs.contains(getNodeConfig().getNodeAddress(myID))) {found = true;}
		if(found) System.out.println("Found my IP");
		else {
			System.out.println("\n\n****Could not locally find the IP " + 
					getNodeConfig().getNodeAddress(myID) +
					"; should change all addresses to localhost instead.****\n\n.");
		}
		return found;
	}

	public static void main(String[] args) {
		assert(!TESTPaxosConfig.isCrashed(new NodeId<String>(100)));
		TESTPaxosConfig.crash(new NodeId<String>(100));
		assert(TESTPaxosConfig.isCrashed(new NodeId<String>(100)));
		TESTPaxosConfig.recover(new NodeId<String>(100));
		assert(!TESTPaxosConfig.isCrashed(new NodeId<String>(100)));
	}
}
