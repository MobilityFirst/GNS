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
package edu.umass.cs.gigapaxos;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;

import edu.umass.cs.gigapaxos.examples.noop.NoopPaxosApp;
import edu.umass.cs.nio.InterfaceNodeConfig;
import edu.umass.cs.nio.NIOTransport;
import edu.umass.cs.reconfiguration.AbstractReconfiguratorDB;
import edu.umass.cs.reconfiguration.interfaces.InterfaceReconfigurableNodeConfig;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.MultiArrayMap;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 * 
 *         A container class for storing gigapaxos config parameters as an enum.
 */
public class PaxosConfig {
	/**
	 * Default file name for gigapaxos config parameters.
	 */
	public static final String DEFAULT_GIGAPAXOS_CONFIG_FILE = "gigapaxos.properties";
	/**
	 * Gigapaxos config file information can be specified using
	 * -DgigapaxosConfig=<filename> as a JVM argument.
	 */
	public static final String GIGAPAXOS_CONFIG_FILE_KEY = "gigapaxosConfig";

	private static String DEFAULT_SERVER_PREFIX = "active.";

	/**
	 * Loads from a default file or file name specified as a system property. We
	 * take a type argument so that ReconfigurationConfig.RC can also mooch off
	 * the same properties file.
	 * 
	 * @param type
	 */
	public static void load(Class<?> type) {
		try {
			Config.register(type, GIGAPAXOS_CONFIG_FILE_KEY,
					DEFAULT_GIGAPAXOS_CONFIG_FILE);
		} catch (IOException e) {
			// ignore as default will still be used
		}
	}

	/**
	 * By default, PaxosConfig.PC will be registered.
	 */
	public static void load() {
		load(PC.class);
	}


	static {
		load();
		NIOTransport.setCompressionThreshold(Config
				.getGlobalInt(PC.COMPRESSION_THRESHOLD));
	}

	/**
	 * @return A map of names and socket addresses corresponding to servers
	 *         hosting paxos replicas.
	 */
	public static Map<String, InetSocketAddress> getActives() {
		Map<String, InetSocketAddress> map = new HashMap<String, InetSocketAddress>();
		Config config = Config.getConfig(PC.class);
		Set<String> keys = config.stringPropertyNames();
		for (String key : keys) {
			if (key.trim().startsWith(DEFAULT_SERVER_PREFIX)) {
				map.put(key.replaceFirst(DEFAULT_SERVER_PREFIX, ""),
						Util.getInetSocketAddressFromString(config
								.getProperty(key)));
			}
		}
		return map;
	}
	
	private static Class<?> getClassSuppressExceptions(String className) {
		Class<?> clazz = null;
		try {
			clazz = Class.forName(className);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return clazz;
	}

	/**
	 * Paxos controlled application.
	 */
	public static final Class<?> application = getClassSuppressExceptions(Config
			.getGlobalString(PC.APPLICATION));

	/**
	 * All gigapaxos config parameters that can be specified via a configuration
	 * file.
	 */
	public static enum PC implements Config.DefaultValueEnum {

		/**
		 * Default application managed by gigapaxos.
		 */
		APPLICATION(NoopPaxosApp.class.getName()),

		/**
		 * Verbose debugging and request instrumentation
		 */
		DEBUG(false),
		/**
		 * True means no persistent logging
		 */
		DISABLE_LOGGING(false),

		/**
		 * 
		 */
		ENABLE_COMPRESSION(true),

		/**
		 * 
		 */
		COMPRESSION_THRESHOLD(4 * 1024 * 1024),

		/**
		 * The default size of the {@link MultiArrayMap} used to store paxos
		 * instances.
		 */
		PINSTANCES_CAPACITY(2000000), // 2M
		/**
		 * The waiting period for paxos instance corpses in order to prevent
		 * inadvertant rebirths because of the missed birthing paxos instance
		 * creation mechanism.
		 */
		MORGUE_DELAY(30000),
		/**
		 * Whether the hibernate option is enabled.
		 */
		HIBERNATE_OPTION(false),
		/**
		 * Whether the pause option is enabled.
		 */
		PAUSE_OPTION(true),
		/**
		 * The time after which the deactivation thread will attempt to pause
		 * idle paxos instances by making a pass over all currently unpaused
		 * instances. This is also the period for which a paxos instance must be
		 * idle in order to be paused.
		 */
		DEACTIVATION_PERIOD(60000), // 30s default

		/**
		 * Refer to documentation in {@link AbstractReconfiguratorDB}.
		 */
		MAX_FINAL_STATE_AGE(3600 * 1000),
		/**
		 * Whether request batching is enabled.
		 */
		BATCHING_ENABLED(true),

		/**
		 * Wait period for forcibly killing a lower paxos instance version in
		 * order to start a higher version.
		 * <p>
		 * FIXME: Unclear what a good default is for good liveness. It doesn't
		 * really matter for safety of reconfiguration.
		 */
		CAN_CREATE_TIMEOUT(5000),
		/**
		 * Wait period before going forth with a missed birthing paxos instance
		 * creation to see if the instance gets normally created anyway.
		 */
		WAIT_TO_GET_CREATED_TIMEOUT(2000),

		/**
		 * The maximum log message size. The higher the batching, the higher
		 * this value needs to be.
		 */
		MAX_LOG_MESSAGE_SIZE(1024 * 512),

		/**
		 * The maximum checkpoint size. The default below is the maximum size of
		 * varchar in embedded derby, which is probably somewhat faster than
		 * clobs, which would be automatically used with bigger checkpoint
		 * sizes.
		 */
		MAX_CHECKPOINT_SIZE(32672),
		
		/**
		 * Number of checkpoints after which log messages will be garbage
		 * collected.
		 */
		LOG_GC_FREQUENCY (1),

		/**
		 * FIXME: A sleep of a millisecond seems to improve latency because
		 * of more batching.
		 */
		BATCH_SLEEP_DURATION(1),

		/**
		 * Inverse of the percentage overhead of agreement latency added to the
		 * sleep duration used for increasing batching gains.
		 */
		BATCH_OVERHEAD(0.01),

		/**
		 * Maximum number of batched requests.
		 */
		MAX_BATCH_SIZE(1000),

		/**
		 * Checkpoint interval. A larger value means slower recovery, slower
		 * coordinator changes, and less frequent garbage collection, but it
		 * also means less frequent IO or higher request throughput.
		 */
		CHECKPOINT_INTERVAL(100),

		/**
		 * Number of threads in packet demultiplexer. More than 0 means that we
		 * may not preserve the order of client requests while processing them.
		 */
		PACKET_DEMULTIPLEXER_THREADS(4),
		
		/**
		 * Whether request order is preserved for requests sent by the same
		 * replica and committed by the same coordinator.
		 */
		ORDER_PRESERVING_REQUESTS (true),

		/**
		 * The replica receiving the request will simply send the request to the
		 * local application replica, i.e., this essentially disables all paxos
		 * coordination. This is used only for testing.
		 */
		EMULATE_UNREPLICATED(false),

		/**
		 * Only for performance instrumentation. If true, replicas will simply
		 * execute the request upon receiving an ACCEPT. This option will break
		 * RSM safety.
		 */
		EXECUTE_UPON_ACCEPT (false),

		/**
		 * Also used for testing. Lazily propagates requests to other replicas
		 * when emulating unreplicated execution mode.
		 */
		LAZY_PROPAGATION(false),

		/**
		 * Enables coalescing of accept replies.
		 */
		BATCHED_ACCEPT_REPLIES(true),

		/**
		 * Enables coalescing of commits. For coalesced commits, a replica must
		 * have the corresponding accept logged, otherwise it will end up
		 * sync'ing and increasing overhead. Enabling this option but not
		 * persistent logging can cause liveness problems.
		 */
		BATCHED_COMMITS(true),
		
		/**
		 * Whether to store compressed logs in the DB.
		 */
		DB_COMPRESSION (true),

		/**
		 * Instrumentation at various places. Should be enabled only during
		 * testing and disabled during production use.
		 */
		ENABLE_INSTRUMENTATION(true),

		/**
		 * 
		 */
		JSON_LIBRARY("org.json"),

		/**
		 * Default location for paxos logs when an embedded DB is used.
		 */
		PAXOS_LOGS_DIR("paxos_logs"),
		
		/**
		 * Prefix of the paxos DB's name. The whole name is obtained
		 * by concatenating this prefix with the node ID.
		 */
		PAXOS_DB_PREFIX ("paxos_logs"),
		
		/**
		 * Maximum length in characters of a paxos group name.
		 */
		MAX_PAXOS_ID_SIZE (40),
		
		/**
		 * {@link edu.umass.cs.gigapaxos.paxosutil.SQL.SQLType} type. Currently,
		 * the only other alternative is "MYSQL". Refer the above class to 
		 * specify the user name and password. 
		 */
		SQL_TYPE ("EMBEDDED_DERBY"),
		
		/**
		 * Maximum size of a paxos replica group.
		 */
		MAX_GROUP_SIZE (16),

		;

		final Object defaultValue;

		PC(Object defaultValue) {
			this.defaultValue = defaultValue;
		}

		@Override
		public Object getDefaultValue() {
			return this.defaultValue;
		}
	}

	/**
	 * @return Default node config.
	 */
	public static InterfaceNodeConfig<String> getDefaultNodeConfig() {
		final Map<String, InetSocketAddress> actives = PaxosConfig.getActives();

		return new InterfaceReconfigurableNodeConfig<String>() {

			@Override
			public boolean nodeExists(String id) {
				return actives.containsKey(id);
			}

			@Override
			public InetAddress getNodeAddress(String id) {
				return actives.containsKey(id) ? actives.get(id).getAddress()
						: null;
			}

			/**
			 * Bind address is returned as the same as the regular address coz
			 * we don't really need a bind address after all.
			 * 
			 * @param id
			 * @return Bind address.
			 */
			@Override
			public InetAddress getBindAddress(String id) {
				return actives.containsKey(id) ? actives.get(id).getAddress()
						: null;
			}

			@Override
			public int getNodePort(String id) {
				return actives.containsKey(id) ? actives.get(id).getPort()
						: null;
			}

			@Override
			public Set<String> getNodeIDs() {
				return new HashSet<String>(actives.keySet());
			}

			@Override
			public String valueOf(String strValue) {
				return this.nodeExists(strValue) ? strValue : null;
			}

			@Override
			public Set<String> getValuesFromStringSet(Set<String> strNodes) {
				throw new RuntimeException("Method not yet implemented");
			}

			@Override
			public Set<String> getValuesFromJSONArray(JSONArray array)
					throws JSONException {
				throw new RuntimeException("Method not yet implemented");
			}

			@Override
			public Set<String> getActiveReplicas() {
				return new HashSet<String>(actives.keySet());
			}

			@Override
			public Set<String> getReconfigurators() {
				return new HashSet<String>(actives.keySet());
			}
		};
	}
}
