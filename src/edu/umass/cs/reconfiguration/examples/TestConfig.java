package edu.umass.cs.reconfiguration.examples;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;

import edu.umass.cs.gns.util.Util;
import edu.umass.cs.reconfiguration.InterfaceModifiableActiveConfig;
import edu.umass.cs.reconfiguration.InterfaceModifiableRCConfig;
import edu.umass.cs.reconfiguration.InterfaceReconfigurableNodeConfig;

/**
 * @author V. Arun
 */
@SuppressWarnings("javadoc")
public class TestConfig {

	public static enum ServerSelectionPolicy {
		FIRST, RANDOM
	};

	public static final ServerSelectionPolicy serverSelectionPolicy = ServerSelectionPolicy.FIRST;

	public static final String[] DEFAULT_RECONFIGURATORS = { "127.0.0.1:3100",
			"127.0.0.1:3101", "127.0.0.1:3102" };// , "127.0.0.1:3103",
													// "127.0.0.1:3104" };

	public static final String[] DEFAULT_ACTIVES = { "127.0.0.1:2100",
			"127.0.0.1:2101", "127.0.0.1:2102" }; // , "127.0.0.1:2103",
													// "127.0.0.1:2104",
													// "127.0.0.1:2105",
													// "127.0.0.1:2106" };

	private final static Set<InetSocketAddress> reconfigurators = new HashSet<InetSocketAddress>();
	private final static Set<InetSocketAddress> actives = new HashSet<InetSocketAddress>();

	static {
		for (String sockAddrStr : DEFAULT_RECONFIGURATORS)
			reconfigurators.add(Util
					.getInetSocketAddressFromString(sockAddrStr));

		for (String sockAddrStr : DEFAULT_ACTIVES)
			actives.add(Util.getInetSocketAddressFromString(sockAddrStr));
	}

	public static Set<InetSocketAddress> getReconfigurators() {
		return reconfigurators;
	}

	public static final String AR_PREFIX = "AR";
	public static final String RC_PREFIX = "RC";

	public static final int numActives = DEFAULT_ACTIVES.length;
	public static final int numRCs = DEFAULT_RECONFIGURATORS.length;
	public static final boolean TEST_CLEAN_SLATE = false;

	public static int getNumActives() {
		return numActives;
	}

	public static int getNumReconfigurators() {
		return numRCs;
	}

	// hacks up a simple node config with the above addresses
	public static InterfaceReconfigurableNodeConfig<String> getTestNodeConfig() {
		return new TestNodeConfig(reconfigurators, actives);
	}

	static class TestNodeConfig implements
			InterfaceReconfigurableNodeConfig<String>,
			InterfaceModifiableRCConfig<String>,
			InterfaceModifiableActiveConfig<String> {
		HashMap<String, InetSocketAddress> rcMap = new HashMap<String, InetSocketAddress>();
		HashMap<String, InetSocketAddress> arMap = new HashMap<String, InetSocketAddress>();

		TestNodeConfig(Set<InetSocketAddress> rcs, Set<InetSocketAddress> ars) {
			int i = 0;
			for (InetSocketAddress sockAddr : rcs)
				rcMap.put(RC_PREFIX + (i++), sockAddr);
			i = 0;
			for (InetSocketAddress sockAddr : ars)
				arMap.put(AR_PREFIX + (i++), sockAddr);
		}

		@Override
		public boolean nodeExists(String id) {
			return rcMap.containsKey(id) || arMap.containsKey(id);
		}

		@Override
		public InetAddress getNodeAddress(String id) {
			return rcMap.containsKey(id) ? rcMap.get(id).getAddress() : arMap
					.containsKey(id) ? arMap.get(id).getAddress() : null;
		}
                
                @Override
		public InetAddress getBindAddress(String id) {
			return rcMap.containsKey(id) ? rcMap.get(id).getAddress() : arMap
					.containsKey(id) ? arMap.get(id).getAddress() : null;
		}

		@Override
		public int getNodePort(String id) {
			return rcMap.containsKey(id) ? rcMap.get(id).getPort() : arMap
					.containsKey(id) ? arMap.get(id).getPort() : -1;
		}

		@Override
		public Set<String> getNodeIDs() {
			Set<String> nodeIDs = new HashSet<String>();
			for (String rc : rcMap.keySet())
				nodeIDs.add(rc);
			for (String active : arMap.keySet())
				nodeIDs.add(active);
			return nodeIDs;
		}

		@Override
		public String valueOf(String strValue) {
			return strValue;
		}

		@Override
		public Set<String> getValuesFromStringSet(Set<String> strNodes) {
			throw new RuntimeException("Not implemented yet");
		}

		@Override
		public Set<String> getValuesFromJSONArray(JSONArray array)
				throws JSONException {
			throw new RuntimeException("Not implemented yet");
		}

		@Override
		public Set<String> getActiveReplicas() {
			return arMap.keySet();
		}

		@Override
		public Set<String> getReconfigurators() {
			return rcMap.keySet();
		}

		@Override
		public InetSocketAddress addReconfigurator(String id,
				InetSocketAddress sockAddr) {
			return this.rcMap.put(id, sockAddr);
		}

		@Override
		public InetSocketAddress removeReconfigurator(String id) {
			return this.rcMap.remove(id);
		}

		@Override
		public InetSocketAddress addActiveReplica(String id,
				InetSocketAddress sockAddr) {
			return this.arMap.put(id, sockAddr);
		}

		@Override
		public InetSocketAddress removeActiveReplica(String id) {
			return this.arMap.remove(id);
		}

		@Override
		public long getVersion() {
			return 0;
		}
	}
}
