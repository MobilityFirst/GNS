package edu.umass.cs.gigapaxos.testing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import edu.umass.cs.nio.nioutils.SampleNodeConfig;
import edu.umass.cs.utils.Util;

/**
 * @author V. Arun
 * 
 *         Configuration parameters for the gigapaxos testing suite.
 */
@SuppressWarnings("javadoc")
public class TESTPaxosConfig {

	/**
	 * Will turn on more verbose logging.
	 */
	public static final boolean DEBUG = false;

	protected static final String SINGLE_NODE_CONFIG_DIR = "/Users/arun/GNS/conf/gigapaxos/";
	private static final String DISTRIBUTED_CONFIG_DIR = "/home/arun/GNS/conf/gigapaxos/";
	private static final String DEFAULT_PROPERTIES_FILENAME = "testing.properties";
	private static final String DEFAULT_SERVERS_FILENAME = "testing_servers.conf";

	protected static final void setSingleNodeTest(String dir) {
		setConfigDir(dir != null ? dir : SINGLE_NODE_CONFIG_DIR, false);
	}

	protected static final void setDistribtedTest(String dir) {
		setConfigDir(dir != null ? dir : DISTRIBUTED_CONFIG_DIR, true);
	}

	/**
	 * We have different config paths for single node and distributed tests as
	 * they may be running on different platforms, e.g., Mac and Linux.
	 */

	private static void setConfigDir(String dir, boolean distributed) {
		String configDir = (dir.endsWith("/") ? dir : dir + "/");
		if (distributed)
			loadServersFromFile(configDir + DEFAULT_SERVERS_FILENAME);
		loadPropertiesFromFile(configDir + DEFAULT_PROPERTIES_FILENAME,
				distributed);
	}

	/**
	 * When memory testing, the number of paxos instances can be very large, so
	 * this flag is used to disable some logging.
	 */
	public static final boolean MEMORY_TESTING = false;

	private static final int MAX_TEST_REQS = 1000000;
	private static final int RANDOM_SEED = 3142;
	private static final double NODE_INCLUSION_PROB = 0.6;

	/**
	 * Disables persistent logging if true.
	 */
	public static final boolean DISABLE_LOGGING = false; 

	private static final boolean TEST_WITH_RECOVERY = true;

	public static final int MAX_NODE_ID = 10000;
	/**
	 *  Node IDs can start from a non-zero value.
	 */
	public static final int TEST_START_NODE_ID = 100; 

	public static final int NUM_NODES = 10;

	/**
	 * Default paxos group name prefix.
	 */
	public static final String TEST_GUID_PREFIX = "paxos";
	/**
	 * Default paxos group name (for the first group).
	 */
	public static final String TEST_GUID = "paxos0";
	/**
	 * Number of pre_configured groups for testing. An arbitrarily higher number
	 * of additional groups can be created.
	 */
	public static final int PRE_CONFIGURED_GROUPS = 10;

	/**************** Number of paxos groups *******************/
	private static final int DEFAULT_NUM_GROUPS = 10000;
	private static int numGroups = DEFAULT_NUM_GROUPS;
	/***********************************************************/

	/**************** Load parameters *******************/
	private static final int DEFAULT_NUM_CLIENTS = 10;
	private static int numClients = DEFAULT_NUM_CLIENTS;

	private static final int DEFAULT_NUM_REQUESTS = 30000;
	private static int numRequests = DEFAULT_NUM_REQUESTS;

	private static final double DEFAULT_TOTAL_LOAD = 1500; // across all clients
	private static double totalLoad = DEFAULT_TOTAL_LOAD;

	/***********************************************************/

	public static int getNumGroups() {
		return numGroups;
	}

	public static int getNumClients() {
		return numClients = Math.min(numClients, numRequests);
	}

	public static int getNumRequests() {
		return numRequests;
	}

	public static int getNumRequestsPerClient() {
		return getNumRequests() / getNumClients();
	}

	public static double getTotalLoad() {
		return totalLoad;
	}

	/*
	 * This will assert the RSM invariant upon execution of every request. It is
	 * meaningful only in a single node test and consumes some cycles, so it
	 * should be disabled in production runs.
	 */
	private static boolean assertRSMInvariant = false;

	public static final boolean shouldAssertRSMInvariant() {
		return assertRSMInvariant;
	}

	public static final void setAssertRSMInvariant(boolean b) {
		assertRSMInvariant = b;
	}

	// to enable retransmission of requests by TESTPaxosClient
	public static final boolean ENABLE_CLIENT_REQ_RTX = false;
	// default retransmission timeout
	public static final long CLIENT_REQ_RTX_TIMEOUT = 8000;

	public static final int DEFAULT_INIT_PORT = SampleNodeConfig.DEFAULT_START_PORT;

	private static final SampleNodeConfig<Integer> nodeConfig = new SampleNodeConfig<Integer>(
			DEFAULT_INIT_PORT);
	static {
		for (int i = TEST_START_NODE_ID; i < TEST_START_NODE_ID + NUM_NODES; i++)
			nodeConfig.addLocal(i);
	}

	private static final HashMap<String, int[]> groups = new HashMap<String, int[]>();
	static {
		setDefaultGroups(PRE_CONFIGURED_GROUPS);
	}

	private static final int[] defaultGroup = { TEST_START_NODE_ID,
			TEST_START_NODE_ID + 1, TEST_START_NODE_ID + 2 };
	private static final Set<Integer> defaultGroupSet;
	static {
		defaultGroupSet = Util.arrayToIntSet(defaultGroup);
	}
	public static final int TEST_CLIENT_ID = 200;

	private static boolean reply_to_client = true;

	private static boolean clean_db = !TEST_WITH_RECOVERY;

	static {
		assert (DEFAULT_NUM_CLIENTS <= PRE_CONFIGURED_GROUPS);
	}

	private static ArrayList<Object> failedNodes = new ArrayList<Object>();

	private static boolean[] committed = new boolean[MAX_TEST_REQS];
	private static boolean[] executedAtAll = new boolean[MAX_TEST_REQS];
	private static boolean[] recovered = new boolean[MAX_NODE_ID];

	public static void setCleanDB(boolean b) {
		clean_db = b;
	}

	public static boolean getCleanDB() {
		return clean_db;
	}

	public static Set<Integer> getNodes() {
		return nodeConfig.getNodes();
	}

	public static void setSendReplyToClient(boolean b) {
		reply_to_client = b;
	}

	public static boolean getSendReplyToClient() {
		return reply_to_client;
	}

	/******************** End of distributed settings **************************/

	public static void setLocalServers() {
		for (int i = 0; i < NUM_NODES; i++)
			TESTPaxosConfig.getNodeConfig().addLocal(
					TESTPaxosConfig.TEST_START_NODE_ID + i);
	}

	public static void setLocalClients() {
		for (int i = 0; i < DEFAULT_NUM_CLIENTS; i++)
			TESTPaxosConfig.getNodeConfig().addLocal(
					TESTPaxosConfig.TEST_CLIENT_ID + i);
	}

	public static void setDefaultGroups(int numGroups) {
		for (int i = 0; i < Math.min(PRE_CONFIGURED_GROUPS, numGroups); i++) {
			groups.put(TEST_GUID_PREFIX + i, defaultGroup);
		}
	}

	// Sets consistent, random groups starting with the same random seed
	public static void setRandomGroups(int numGroups) {
		// if(!getCleanDB()) return;
		Random r = new Random(RANDOM_SEED);
		for (int i = 0; i < Math.min(PRE_CONFIGURED_GROUPS, numGroups); i++) {
			groups.put(TEST_GUID_PREFIX + i, defaultGroup);
			if (i == 0)
				continue;// first group is always default group
			TreeSet<Integer> members = new TreeSet<Integer>();
			for (int id : TESTPaxosConfig.getNodes()) {
				if (r.nextDouble() > NODE_INCLUSION_PROB) {
					members.add(id);
				}
			}
			TESTPaxosConfig.setGroup(TESTPaxosConfig.getGroupName(i), members);
		}
	}

	public static final void setCleanDB(String[] args) {
		for (String arg : args)
			if (arg.trim().equals("-c"))
				TESTPaxosConfig.setCleanDB(true);
	}

	public static final String getConfDirArg(String[] args) {
		for (String arg : args)
			if (!arg.trim().equals("-c"))
				return arg;
		return null;
	}

	public static void setGroup(String groupID, Set<Integer> members) {
		int[] array = new int[members.size()];
		int j = 0;
		for (int id : members)
			array[j++] = id;
		groups.put(groupID, array);
	}

	public static void setGroup(String groupID, int[] members) {
		groups.put(groupID, members);
	}

	public static int[] getDefaultGroup() {
		return defaultGroup;
	}

	public static Set<Integer> getDefaultGroupSet() {
		return defaultGroupSet;
	}

	public static int[] getGroup(String groupID) {
		int[] members = groups.get(groupID);
		return members != null ? members : defaultGroup;
	}

	public static int[] getGroup(int groupID) {
		int[] members = groups.get(TEST_GUID_PREFIX + groupID);
		return members != null ? members : defaultGroup;
	}

	public static String getGroupName(int groupID) {
		return TEST_GUID_PREFIX + groupID;
	}

	public static Collection<String> getGroups() {
		return groups.keySet();
	}

	public static void createGroup(String groupID, int[] members) {
		if (groups.size() <= PRE_CONFIGURED_GROUPS)
			groups.put(groupID, members);
	}

	public static SampleNodeConfig<Integer> getNodeConfig() {
		return nodeConfig;
	}

	public synchronized static void crash(int nodeID) {
		TESTPaxosConfig.failedNodes.add(nodeID);
	}

	public synchronized static void recover(int nodeID) {
		TESTPaxosConfig.failedNodes.remove(new Integer(nodeID));
	}

	public synchronized static boolean isCrashed(Object nodeID) {
		return TESTPaxosConfig.failedNodes.contains(nodeID);
	}

	public synchronized static void setRecovered(int id, String paxosID,
			boolean b) {
		// assert (id < MAX_NODE_ID) : " id = " + id + ", MAX_NODE_ID = "+
		// MAX_NODE_ID;
		if (paxosID.equals(TEST_GUID)) {
			recovered[id] = b;
		}
	}

	public synchronized static boolean getRecovered(int id, String paxosID) {
		assert (id < MAX_NODE_ID);
		if (paxosID.equals(TEST_GUID))
			return recovered[id];
		else
			return true;
	}

	public synchronized static boolean isCommitted(int reqnum) {
		assert (reqnum < MAX_TEST_REQS);
		return committed[reqnum];
	}

	public synchronized static void execute(int reqnum) {
		assert (reqnum >= 0);
		executedAtAll[reqnum] = true;
	}

	public synchronized static void commit(int reqnum) {
		if (reqnum >= 0 && reqnum < committed.length)
			committed[reqnum] = true;
	}

	// Checks if the IP specified for the id argument is local
	public static boolean findMyIP(Integer myID) throws SocketException {
		if (myID == null)
			return false;
		Enumeration<NetworkInterface> netfaces = NetworkInterface
				.getNetworkInterfaces();
		ArrayList<InetAddress> myIPs = new ArrayList<InetAddress>();
		while (netfaces.hasMoreElements()) {
			NetworkInterface iface = netfaces.nextElement();
			Enumeration<InetAddress> allIPs = iface.getInetAddresses();
			while (allIPs.hasMoreElements()) {
				InetAddress addr = allIPs.nextElement();
				if ((addr instanceof Inet4Address))
					myIPs.add((InetAddress) addr);
			}
		}
		System.out.println(myIPs);
		boolean found = false;
		if (myIPs.contains(getNodeConfig().getNodeAddress(myID))) {
			found = true;
		}
		if (found)
			System.out.println("Found my IP");
		else {
			System.out
					.println("\n\n****Could not locally find the IP "
							+ getNodeConfig().getNodeAddress(myID)
							+ "; should change all addresses to localhost instead.****\n\n.");
		}
		return found;
	}

	private static Properties readProperties(String filename) {
		Properties props = null;
		InputStream inStream;
		try {
			inStream = new FileInputStream(new File(filename));
			props = new Properties();
			props.load(inStream);
		} catch (IOException e) {
			System.out.println("Could not find the properties file at "
					+ filename
					+ "; using default properties in TESTPaxosConfig.");
		}
		return props;
	}

	private static void loadPropertiesFromFile(String filename,
			boolean distributed) {
		Properties props = readProperties(filename);
		if (props == null)
			return;
		// FIXME: actually set properties here
		for (Object prop : props.keySet()) {
			String key = prop.toString();
			switch (key) {
			case "NUM_GROUPS":
				TESTPaxosConfig.numGroups = Integer.valueOf(props
						.getProperty(key));
				break;
			case "NUM_REQUESTS":
				TESTPaxosConfig.numRequests = Integer.valueOf(props
						.getProperty(key));
				break;
			case "TOTAL_LOAD":
				TESTPaxosConfig.totalLoad = Double.valueOf(props
						.getProperty(key));
				break;
			case "SERVERS_FILENAME":
				if (distributed)
					TESTPaxosConfig.loadServersFromFile(props.getProperty(key));
			}
		}
	}

	// FIXME: Currenty only for gigapaxos use. Need to use NodeConfig.
	private static final void loadServersFromFile(String filename) {
		BufferedReader reader;
		try {
			reader = new BufferedReader(new FileReader(filename));
			String line = null;
			int count = 0;
			while ((line = reader.readLine()) != null) {
				if (line.contains("#"))
					continue;
				String[] tokens = line.split("\\s");
				assert (tokens.length >= 2);
				int id = TESTPaxosConfig.TEST_START_NODE_ID + count;
				try {
					id = Integer.valueOf(tokens[0].trim());
				} catch (NumberFormatException nfe) {
					nfe.printStackTrace();
				}
				TESTPaxosConfig.getNodeConfig().add(id,
						InetAddress.getByName(tokens[1].trim()));
			}
			reader.close();
		} catch (IOException e) {
			System.err
					.println("Could not find the file with the list of distributed servers at "
							+ filename + "; exiting.");
			System.exit(1);
		}
	}

	public static void main(String[] args) {
	}
}
