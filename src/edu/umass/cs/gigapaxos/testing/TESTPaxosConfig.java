/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): V. Arun
 */
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
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Level;

import org.json.JSONArray;
import org.json.JSONException;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.nio.interfaces.NodeConfig;
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
					+ TC.PROPERTIES_FILENAME.getDefaultValue().toString());
		} catch (IOException e) {
			// ignore as defaults will be used
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
		 * Default paxos group name prefix.
		 */
		TEST_GUID_PREFIX(TESTPaxosApp.class.getSimpleName()),
		
		/**
		 * First paxos group name.
		 */
		TEST_GUID(TESTPaxosApp.class.getSimpleName()+"0"),

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
		NUM_CLIENTS(9),
		
		/**
		 * Whether a testing client should be pinned to send request to 
		 * a single server.
		 */
		PIN_CLIENT(true),

		/**
		 * 
		 */
		NUM_REQUESTS(25000),

		/**
		 * 
		 */
		TOTAL_LOAD(1000), // across all clients

		/**
		 * Payload size in test requests.
		 */
		REQUEST_BAGGAGE_SIZE (10),
		
		/**
		 * Whether the request is highly compressible. Otherwise, the request is
		 * a bunch of random ascii characters from 0 to z. Non-ascii characters
		 * result in inflated and unpredictable byte[] lengths, which screws up
		 * testing.
		 */
		COMPRESSIBLE_REQUEST (false),
		
		/**
		 * 
		 */
		OVERHEAD_TESTING (false),
		
		/**
		 * to disable some exceptions/logging while testing
		 */
		MEMORY_TESTING(true),
		
		/**
		 * Make the test app an absolute noop.
		 */
		ABSOLUTE_NOOP_APP (true),
		
		/**
		 * Testing parameter that makes the testing app heavier-weight by doing
		 * operations that result in roughly the specified latency (in
		 * microseconds) per request.
		 */
		TEST_APP_DELAY (0), 
		
		;

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
		else setupGroups();
		
		loadPropertiesFromFile(
				configDir + Config.getGlobalString(TC.PROPERTIES_FILENAME),
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
		assertRSMInvariant = b
				&& !Config.getGlobalBoolean(PC.EXECUTE_UPON_ACCEPT)
				&& (!Config.getGlobalBoolean(PC.DISABLE_LOGGING) || Config
						.getGlobalBoolean(PC.ENABLE_JOURNALING));
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

	private static final SampleNodeConfig<Integer> nodeConfig = new SampleNodeConfig<Integer>(
			SampleNodeConfig.DEFAULT_START_PORT, Config.getGlobalInt(TC.TEST_START_NODE_ID), Config.getGlobalInt(TC.NUM_NODES));
	private static final HashMap<String, int[]> groups = new HashMap<String, int[]>();
	private static int[] defaultGroup = new int[Config
			.getGlobalInt(TC.NUM_NODES)];

	private static void setupGroups() {
		defaultGroup = new int[Math.min(3,  Config.getGlobalInt(TC.NUM_NODES))];
		assert(defaultGroup.length>0) : Config.getGlobalInt(TC.NUM_NODES);
		for (int i = 0; i < defaultGroup.length; i++)
			defaultGroup[i] = Config.getGlobalInt(TC.TEST_START_NODE_ID) + i;
		
		setDefaultGroups(Config.getGlobalInt(TC.PRE_CONFIGURED_GROUPS));
	}

	static { // is this needed?
		//setupGroups();
	}

	// replies directly sendable by paxos via InterfaceClientRequest
	private static boolean reply_to_client = false;//true;

	private static boolean clean_db = Config
			.getGlobalBoolean(PC.DISABLE_LOGGING) && !Config.getGlobalBoolean(PC.ENABLE_JOURNALING);

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
	 * 
	 */
	public static final boolean PAXOS_MANAGER_UNIT_TEST = false;

	/**
	 * @return True means send reply to client.
	 */
	public static boolean getSendReplyToClient() {
		return reply_to_client;
	}

	/******************** End of distributed settings **************************/

	/**
	 * @param numGroups
	 */
	public static void setDefaultGroups(int numGroups) {

		groups.clear();
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
	 * @return True if -c flag is present.
	 */
	public static final boolean shouldCleanDB(String[] args) {
		for (String arg : args)
			if (arg.trim().equals("-c"))
				return true;
		return false;
	}

	/**
	 * @param args
	 * 
	 * @return Config directory parsed as the first argument other than "-c".
	 */
	public static final String getConfDirArg(String[] args) {
		for (String arg : args)
			if (arg.trim().contains("-D"))
				return arg.replace("-D", "");
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
		return Util.arrayToIntSet(defaultGroup);
	}

	/**
	 * @param groupID
	 * @return Group for groupID.
	 */
	public static int[] getGroup(String groupID) {
		int[] members = groups.get(groupID);
		assert(defaultGroup.length>0);
		return members != null && members.length>0 ? members : defaultGroup;
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
	public static boolean isCrashed(Object nodeID) {
		return TESTPaxosConfig.failedNodes.contains(nodeID);
	}

	/**
	 * @param id
	 * @param paxosID
	 * @param b
	 */
	@Deprecated
	public static void setRecovered(int id, String paxosID,
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
	public synchronized static boolean isCommitted(long reqnum) {
		assert (reqnum < MAX_TEST_REQS);
		return committed[(int)reqnum];
	}

	/**
	 * @param reqnum
	 */
	public synchronized static void execute(long reqnum) {
		assert (reqnum >= 0);
		executedAtAll[(int)reqnum] = true;
	}

	/**
	 * @param reqnum
	 */
	@Deprecated
	public static void commit(int reqnum) {
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
	protected static NodeConfig<Integer> getFromPaxosConfig() {
		return getFromPaxosConfig(false);
	}
	protected static NodeConfig<Integer> getFromPaxosConfig(final boolean clientFacing) {
		final NodeConfig<String> defaultNC = PaxosConfig
				.getDefaultNodeConfig();
		return new NodeConfig<Integer>() {

			@Override
			public Integer valueOf(String strValue) {
				return Integer.valueOf(strValue);
			}

			@Override
			public Set<Integer> getValuesFromStringSet(Set<String> strNodes) {
				throw new RuntimeException("Method not implemented");
			}

			@Override
			public Set<Integer> getValuesFromJSONArray(JSONArray array)
					throws JSONException {
				throw new RuntimeException("Method not implemented");
			}

			@Override
			public boolean nodeExists(Integer id) {
				return defaultNC.nodeExists(id.toString());
			}

			@Override
			public InetAddress getNodeAddress(Integer id) {
				return defaultNC.nodeExists(id.toString()) ? defaultNC
						.getNodeAddress(id.toString()) : 
							nodeConfig.getNodeAddress(id);
							//SampleNodeConfig.getLocalAddress();
			}

			@Override
			public InetAddress getBindAddress(Integer id) {
				return this.getNodeAddress(id);
			}

			@Override
			public int getNodePort(Integer id) {
				return (defaultNC.nodeExists(id.toString()) ? defaultNC
						.getNodePort(id.toString()) : nodeConfig
						.getNodePort(id))
						//+ (clientFacing ? Config
							//	.getGlobalInt(PC.CLIENT_PORT_OFFSET) : 0)
						;
			}

			@Override
			public Set<Integer> getNodeIDs() {
				Set<String> nodes = defaultNC.getNodeIDs();
				Set<Integer> intIDs = new HashSet<Integer>();
				for (String s : nodes)
					intIDs.add(Integer.valueOf(s));
				for(int id : nodeConfig.getNodeIDs())
					intIDs.add(id);
				return intIDs;
			}
			
			public String toString() {
				String s="";
				for(Integer id : this.getNodeIDs()) {
					s = (s + id+":"+this.getNodeAddress(id)+":"+this.getNodePort(id) + " " );
				}
				return s;
			}
		};
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

			TESTPaxosConfig.getNodeConfig().clear();

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
			// re-make default groups
			Config.getConfig(TC.class).put(TC.NUM_NODES,
					TESTPaxosConfig.getFromPaxosConfig().getNodeIDs().size());
			setupGroups();
			reader.close();
		} catch (IOException e) {
			System.err
					.println("Could not find the file with the list of distributed servers at "
							+ filename + "; exiting.");
			System.exit(1);
		}
	}
	
	/**
	 * @param level
	 */
	public static void setConsoleHandler(Level level) {
		PaxosConfig.setConsoleHandler(level);
	}
	protected static void setConsoleHandler() {
		setConsoleHandler(Level.INFO);
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		NodeConfig<Integer> nc = TESTPaxosConfig.getFromPaxosConfig();
		for(Integer i : nc.getNodeIDs())
		System.out.println(i + ":" + nc.getNodeAddress(i)+":"+nc.getNodePort(i));
	}
}
