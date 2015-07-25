package edu.umass.cs.gigapaxos.testing;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.nio.nioutils.SampleNodeConfig;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.Util;

/**
 * @author V. Arun
 * 
 *         Configuration parameters for the gigapaxos testing suite.
 */
public class TESTPaxosConfig {

	protected static void load() {
		// general gigapaxos config parameters
		PaxosConfig.load();
		// testing specific config parameters
		try {
			Config.register(TC.class, TC.CONFIG_DIR.getDefaultValue()
					.toString()
					+ TC.PROPERTIES_FILENAME.getDefaultValue()
							.toString());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	static {
		load();
	}

	/**
	 * Gigapaxos testing config parameters.
	 */
	public static enum TC implements Config.DefaultValueEnum {
		/**
		 * 
		 */
		CONFIG_DIR("conf/gigapaxos/"),
		/**
		 * 
		 */
		PROPERTIES_FILENAME("testing.properties"),
		/**
		 * 
		 */
		SERVERS_FILENAME("testing_servers.conf"),
		/**
		 * 
		 */
		NUM_NODES(10),
		/**
		 * default paxos group name prefix.
		 */
		TEST_GUID_PREFIX("paxos"),

		/**
		 * 
		 */
		NODE_INCLUSION_PROB(0.6),
		/**
		 * node IDs can start from a non-zero value
		 */
		TEST_START_NODE_ID(100),
		/**
		 * starting ID for clients
		 */
		TEST_CLIENT_ID(200),
		/**
		 * 
		 */
		TEST_GUID("paxos0"),
		/**
		 * Number of pre_configured groups for testing. An arbitrarily higher
		 * number of additional groups can be created. These preconfigured
		 * groups will have consistently random group membership.
		 */
		PRE_CONFIGURED_GROUPS(10),
		/**
		 * Total number of paxos groups. Groups beyond preconfigured groups will
		 * have a fixed default group membership.
		 */
		NUM_GROUPS(10000),

		/**
		 * 
		 */
		NUM_CLIENTS(10),

		/**
		 * 
		 */
		NUM_REQUESTS(25000),

		/**
		 * 
		 */
		TOTAL_LOAD(1000), // across all clients

		/**
		 * to disable some exceptions/logging while testing
		 */
		MEMORY_TESTING(true);

		final Object defaultValue;

		TC(Object defaultValue) {
			this.defaultValue = defaultValue;
		}

		@Override
		public Object getDefaultValue() {
			return this.defaultValue;
		}
	}

	// unchangeable, probably deprecated anyway
	private static final int MAX_TEST_REQS = 1000000;
	private static final int RANDOM_SEED = 3142;
	/**
	 * 
	 */
	public static final int MAX_NODE_ID = 10000;

	protected static final void setSingleNodeTest(String dir) {
		setConfigDir(dir != null ? dir : Config.getGlobalString(TC.CONFIG_DIR),
				false);
	}

	// same as setSingleNodeTest
	protected static final void setDistribtedTest(String dir) {
		setConfigDir(dir != null ? dir : Config.getGlobalString(TC.CONFIG_DIR),
				true);
	}

	/**
	 * We have different config paths for single node and distributed tests as
	 * they may be running on different platforms, e.g., Mac and Linux.
	 */

	private static void setConfigDir(String dir, boolean distributed) {
		String configDir = (dir.endsWith("/") ? dir : dir + "/");
		if (distributed)
			loadServersFromFile(configDir
					+ Config.getGlobalString(TC.SERVERS_FILENAME));
		loadPropertiesFromFile(
				configDir
						+ Config.getGlobalString(TC.PROPERTIES_FILENAME),
				distributed);
	}

	/*
	 * This will assert the RSM invariant upon execution of every request. It is
	 * meaningful only in a single node test and consumes some cycles, so it
	 * should be disabled in production runs.
	 */
	private static boolean assertRSMInvariant = false;

	/**
	 * @return True if RSM invariant should be asserted.
	 */
	public static final boolean shouldAssertRSMInvariant() {
		return assertRSMInvariant;
	}

	/**
	 * @param b
	 */
	public static final void setAssertRSMInvariant(boolean b) {
		assertRSMInvariant = b;
	}

	/**
	 * to enable retransmission of requests by TESTPaxosClient
	 */
	public static final boolean ENABLE_CLIENT_REQ_RTX = false;
	/**
	 * default retransmission timeout
	 */
	public static final long CLIENT_REQ_RTX_TIMEOUT = 8000;

	/**
	 * 
	 */
	public static final int DEFAULT_INIT_PORT = SampleNodeConfig.DEFAULT_START_PORT;

	private static final SampleNodeConfig<Integer> nodeConfig = new SampleNodeConfig<Integer>(
			DEFAULT_INIT_PORT);
	static {
		for (int i = Config.getGlobalInt(TC.TEST_START_NODE_ID); i < Config
				.getGlobalInt(TC.TEST_START_NODE_ID)
				+ Config.getGlobalInt(TC.NUM_NODES); i++)
			nodeConfig.addLocal(i);
	}

	private static final HashMap<String, int[]> groups = new HashMap<String, int[]>();
	static {
		setDefaultGroups(Config.getGlobalInt(TC.PRE_CONFIGURED_GROUPS));
	}

	private static final int[] defaultGroup = {
			Config.getGlobalInt(TC.TEST_START_NODE_ID),
			Config.getGlobalInt(TC.TEST_START_NODE_ID) + 1,
			Config.getGlobalInt(TC.TEST_START_NODE_ID) + 2 };
	private static final Set<Integer> defaultGroupSet;
	static {
		defaultGroupSet = Util.arrayToIntSet(defaultGroup);
	}

	private static boolean reply_to_client = true;

	private static boolean clean_db = Config
			.getGlobalBoolean(PC.DISABLE_LOGGING);

	private static ArrayList<Object> failedNodes = new ArrayList<Object>();

	private static boolean[] committed = new boolean[MAX_TEST_REQS];
	private static boolean[] executedAtAll = new boolean[MAX_TEST_REQS];
	private static boolean[] recovered = new boolean[MAX_NODE_ID];

	/**
	 * @param b
	 */
	public static void setCleanDB(boolean b) {
		clean_db = b;
	}

	/**
	 * @return True if DB should be cleaned.
	 */
	public static boolean getCleanDB() {
		return clean_db;
	}

	/**
	 * @return All nodeIDs in node config.
	 */
	public static Set<Integer> getNodes() {
		return nodeConfig.getNodes();
	}

	/**
	 * @param b
	 */
	public static void setSendReplyToClient(boolean b) {
		reply_to_client = b;
	}

	/**
	 * @return True means send reply to client.
	 */
	public static boolean getSendReplyToClient() {
		return reply_to_client;
	}

	/******************** End of distributed settings **************************/

	public static void setLocalServers() {
		for (int i = 0; i < Config.getGlobalInt(TC.NUM_NODES); i++)
			TESTPaxosConfig.getNodeConfig().addLocal(
					Config.getGlobalInt(TC.TEST_START_NODE_ID) + i);
	}

	/**
	 * 
	 */
	public static void setLocalClients() {
		for (int i = 0; i < Config.getGlobalInt(TC.NUM_CLIENTS); i++)
			TESTPaxosConfig.getNodeConfig().addLocal(
					Config.getGlobalInt(TC.TEST_CLIENT_ID) + i);
	}

	/**
	 * @param numGroups
	 */
	public static void setDefaultGroups(int numGroups) {
		for (int i = 0; i < Math.min(
				Config.getGlobalInt(TC.PRE_CONFIGURED_GROUPS), numGroups); i++) {
			groups.put(Config.getGlobalString(TC.TEST_GUID_PREFIX) + i,
					defaultGroup);
		}
	}

	/**
	 * Sets consistent, random groups starting with the same random seed.
	 * 
	 * @param numGroups
	 */
	public static void setRandomGroups(int numGroups) {
		// if(!getCleanDB()) return;
		Random r = new Random(RANDOM_SEED);
		for (int i = 0; i < Math.min(
				Config.getGlobalInt(TC.PRE_CONFIGURED_GROUPS), numGroups); i++) {
			groups.put(Config.getGlobalString(TC.TEST_GUID_PREFIX) + i,
					defaultGroup);
			if (i == 0)
				continue;// first group is always default group
			TreeSet<Integer> members = new TreeSet<Integer>();
			for (int id : TESTPaxosConfig.getNodes()) {
				if (r.nextDouble() > Config
						.getGlobalDouble(TC.NODE_INCLUSION_PROB)) {
					members.add(id);
				}
			}
			TESTPaxosConfig.setGroup(TESTPaxosConfig.getGroupName(i), members);
		}
	}

	/**
	 * Cleans DB if -c command line arg is specified.
	 * 
	 * @param args
	 */
	public static final void setCleanDB(String[] args) {
		for (String arg : args)
			if (arg.trim().equals("-c"))
				TESTPaxosConfig.setCleanDB(true);
	}

	/**
	 * @param args
	 * 
	 * @return Config directory parsed as the first argument other than "-c".
	 */
	public static final String getConfDirArg(String[] args) {
		for (String arg : args)
			if (!arg.trim().equals("-c"))
				return arg;
		return null;
	}

	/**
	 * @param groupID
	 * @param members
	 */
	public static void setGroup(String groupID, Set<Integer> members) {
		int[] array = new int[members.size()];
		int j = 0;
		for (int id : members)
			array[j++] = id;
		groups.put(groupID, array);
	}

	/**
	 * @param groupID
	 * @param members
	 */
	public static void setGroup(String groupID, int[] members) {
		groups.put(groupID, members);
	}

	/**
	 * @return Default group members.
	 */
	public static int[] getDefaultGroup() {
		return defaultGroup;
	}

	/**
	 * @return Default group members as set.
	 */
	public static Set<Integer> getDefaultGroupSet() {
		return defaultGroupSet;
	}

	/**
	 * @param groupID
	 * @return Group for groupID.
	 */
	public static int[] getGroup(String groupID) {
		int[] members = groups.get(groupID);
		return members != null ? members : defaultGroup;
	}

	/**
	 * @param groupID
	 * @return Group members for integer groupID.
	 */
	public static int[] getGroup(int groupID) {
		int[] members = groups.get(Config.getGlobalString(TC.TEST_GUID_PREFIX)
				+ groupID);
		return members != null ? members : defaultGroup;
	}

	/**
	 * @param groupID
	 * @return Group name given integer groupID.
	 */
	public static String getGroupName(int groupID) {
		return Config.getGlobalString(TC.TEST_GUID_PREFIX) + groupID;
	}

	/**
	 * @return Alll group names.
	 */
	public static Collection<String> getGroups() {
		return groups.keySet();
	}

	/**
	 * @param groupID
	 * @param members
	 */
	public static void createGroup(String groupID, int[] members) {
		if (groups.size() <= Config.getGlobalInt(TC.PRE_CONFIGURED_GROUPS))
			groups.put(groupID, members);
	}

	/**
	 * @return Node config.
	 */
	public static SampleNodeConfig<Integer> getNodeConfig() {
		return nodeConfig;
	}

	/**
	 * @param nodeID
	 */
	public synchronized static void crash(int nodeID) {
		TESTPaxosConfig.failedNodes.add(nodeID);
	}

	/**
	 * @param nodeID
	 */
	public synchronized static void recover(int nodeID) {
		TESTPaxosConfig.failedNodes.remove(new Integer(nodeID));
	}

	/**
	 * @param nodeID
	 * @return True if crash is being simulated.
	 */
	public synchronized static boolean isCrashed(Object nodeID) {
		return TESTPaxosConfig.failedNodes.contains(nodeID);
	}

	/**
	 * @param id
	 * @param paxosID
	 * @param b
	 */
	public synchronized static void setRecovered(int id, String paxosID,
			boolean b) {
		if (paxosID.equals(Config.getGlobalString(TC.TEST_GUID))) {
			recovered[id] = b;
		}
	}

	/**
	 * @param id
	 * @param paxosID
	 * @return True if recovered.
	 */
	@Deprecated
	public synchronized static boolean getRecovered(int id, String paxosID) {
		assert (id < MAX_NODE_ID);
		if (paxosID.equals(Config.getGlobalString(TC.TEST_GUID)))
			return recovered[id];
		else
			return true;
	}

	/**
	 * @param reqnum
	 * @return True if committed.
	 */
	@Deprecated
	public synchronized static boolean isCommitted(int reqnum) {
		assert (reqnum < MAX_TEST_REQS);
		return committed[reqnum];
	}

	/**
	 * @param reqnum
	 */
	public synchronized static void execute(int reqnum) {
		assert (reqnum >= 0);
		executedAtAll[reqnum] = true;
	}

	/**
	 * @param reqnum
	 */
	public synchronized static void commit(int reqnum) {
		if (reqnum >= 0 && reqnum < committed.length)
			committed[reqnum] = true;
	}

	// Checks if the IP specified for the id argument is local
	/**
	 * @param myID
	 * @return True if found my IP.
	 * @throws SocketException
	 */
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

	private static void loadPropertiesFromFile(String filename,
			boolean distributed) {
		try {
			Config.register(TC.class, filename);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

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
				int id = Config.getGlobalInt(TC.TEST_START_NODE_ID) + count;
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
}
