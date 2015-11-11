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
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;

import org.json.JSONArray;
import org.json.JSONException;

import edu.umass.cs.gigapaxos.examples.noop.NoopPaxosApp;
import edu.umass.cs.nio.NIOTransport;
import edu.umass.cs.nio.SSLDataProcessingWorker;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.reconfiguration.AbstractReconfiguratorDB;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableNodeConfig;
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
                      if (className != null && !"null".equals(className)) {
			  clazz = Class.forName(className);
                      }
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
		 * Default offset for the client facing port.
		 */
		CLIENT_PORT_OFFSET(00),

		/**
		 * Verbose debugging and request instrumentation
		 */
		DEBUG(false),
		/**
		 * True means no persistent logging
		 */
		DISABLE_LOGGING(false),

		/**
		 * FIXME: Journaling based log for efficiency. Needs periodic compaction
		 * that is not yet implemented, so this log exists only for performance
		 * instrumentation.
		 */
		ENABLE_JOURNALING(true),

		/**
		 * True means no checkpointing. If logging is enabled (as is the
		 * default), the logs will grow unbounded. So either both should be true
		 * or both should be false.
		 */
		DISABLE_CHECKPOINTING(false),

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
		 * Fraction of capacity to be reached in order for pausing to get
		 * enabled.
		 */
		PAUSE_SIZE_THRESHOLD(0),

		/**
		 * The time after which the deactivation thread will attempt to pause
		 * idle paxos instances by making a pass over all currently unpaused
		 * instances. This is also the period for which a paxos instance must be
		 * idle in order to be paused.
		 */
		DEACTIVATION_PERIOD(60000), // 30s default

		/**
		 * Limits the rate of pausing to not interfere with request processing.
		 * But it has the downside of increasing the total pause time as well as
		 * limiting the paxos instance creation rate. For example, pausing a
		 * million instances will take 1000s with the default rate limit of
		 * 1000/s.
		 */
		PAUSE_RATE_LIMIT(1000), // /s

		/**
		 * Refer to documentation in {@link AbstractReconfiguratorDB}.
		 */
		MAX_FINAL_STATE_AGE(3600 * 1000),
		/**
		 * Whether request batching is enabled.
		 */
		BATCHING_ENABLED(true),

		/**
		 * 
		 */
		MAX_LOG_FILE_SIZE(64 * 1024 * 1024),

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
		MAX_LOG_MESSAGE_SIZE(1024 * 1024 * 5),

		/**
		 * The maximum checkpoint size. The default below is the maximum size of
		 * varchar in embedded derby, which is probably somewhat faster than
		 * clobs, which would be automatically used with bigger checkpoint
		 * sizes.
		 */
		MAX_CHECKPOINT_SIZE(32672),

		/**
		 * Number of checkpoints after which log messages will be garbage
		 * collected for a paxos group. We really don't need to do garbage
		 * collection at all until the size of the table starts affecting log
		 * message retrieval time or the size of the table starts causing the
		 * indexing overhead to become high at insertion time; the latter is
		 * unlikely as we maintain an index on the paxosID key.
		 */
		LOG_GC_FREQUENCY(10),

		/**
		 * Number of log files after which GC will be attempted.
		 */
		JOURNAL_GC_FREQUENCY(10),

		/**
		 * Number of log files after which compaction will be attempted.
		 */
		COMPACTION_FREQUENCY(10),

		/**
		 * The number of log messages after which they are indexed into the DB.
		 * Indexing every log message doubles the logging overhead and doesn't
		 * have to be done (unless #SYNC_INDEX_JOURNAL is enabled). Upon
		 * recovery however, we need to do slightly more work to ensure that all
		 * uncommitted log messages are indexed into the DB.
		 */
		LOG_INDEX_FREQUENCY(100),

		/**
		 * Whether fields in the log messages table in the DB should be indexed.
		 * This index refers to the DB's internal index as opposed to
		 * {@link #DB_INDEX_JOURNAL} that is an explicit index of log messages
		 * journaled on the file system.
		 */
		INDEX_LOG_TABLE(true),

		/**
		 * A tiny amount of minimum sleep imposed on every request in order to
		 * improve batching benefits. Also refer to {@link #BATCH_OVERHEAD}.
		 */
		BATCH_SLEEP_DURATION(0),

		/**
		 * Inverse of the percentage overhead of agreement latency added to the
		 * sleep duration used for increasing batching gains. Also refer to
		 * {@value #BATCH_SLEEP_DURATION}.
		 */
		BATCH_OVERHEAD(0.01),

		/**
		 * 
		 */
		DISABLE_SYNC_DECISIONS(false),

		/**
		 * Maximum number of batched requests. Setting it to infinity means that
		 * the log message size will still limit it.
		 */
		MAX_BATCH_SIZE(2000),

		/**
		 * Checkpoint interval. A larger value means slower recovery, slower
		 * coordinator changes, and less frequent garbage collection, but it
		 * also means less frequent IO or higher request throughput.
		 */
		CHECKPOINT_INTERVAL(400),

		/**
		 * Number of threads in packet demultiplexer. More than 0 means that we
		 * may not preserve the order of client requests while processing them.
		 */
		PACKET_DEMULTIPLEXER_THREADS(4),

		/**
		 * Whether request order is preserved for requests sent by the same
		 * replica and committed by the same coordinator.
		 */
		ORDER_PRESERVING_REQUESTS(true),

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
		EXECUTE_UPON_ACCEPT(false),

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
		DB_COMPRESSION(true),

		/**
		 * Instrumentation at various places. Should be enabled only during
		 * testing and disabled during production use.
		 */
		ENABLE_INSTRUMENTATION(true),

		/**
		 * 
		 */
		JSON_LIBRARY("json.smart"),

		/**
		 * Default location for paxos logs when an embedded DB is used.
		 */
		PAXOS_LOGS_DIR("./paxos_logs"),

		/**
		 * Prefix of the paxos DB's name. The whole name is obtained by
		 * concatenating this prefix with the node ID.
		 */
		PAXOS_DB_PREFIX("paxos_logs"),

		/**
		 * Maximum length in characters of a paxos group name.
		 */
		MAX_PAXOS_ID_SIZE(40),

		/**
		 * {@link edu.umass.cs.gigapaxos.paxosutil.SQL.SQLType} type. Currently,
		 * the only other alternative is "MYSQL". Refer the above class to
		 * specify the user name and password.
		 */
		SQL_TYPE("EMBEDDED_DERBY"),

		/**
		 * Maximum size of a paxos replica group.
		 */
		MAX_GROUP_SIZE(16),

		/**
		 * Threshold for throttling client request load.
		 */
		MAX_OUTSTANDING_REQUESTS(8000),

		/**
		 * Sleep millis to throttle client requests if overloaded. Used only for
		 * testing.
		 */
		THROTTLE_SLEEP(0),

		/**
		 * Client-server SSL mode.
		 */
		CLIENT_SSL_MODE(SSLDataProcessingWorker.SSL_MODES.CLEAR),

		/**
		 * Server-server SSL mode.
		 */
		SERVER_SSL_MODE(SSLDataProcessingWorker.SSL_MODES.CLEAR),

		/**
		 * Number of additional sending connections used by paxos. We need this
		 * because the sending throughput of a single TCP connection is limited
		 * and can become a bottleneck at the coordinator.
		 */
		NUM_MESSENGER_WORKERS(0),

		/**
		 * 
		 */
		USE_NIO_SENDER_TASK(false),

		/**
		 * Disable congestion pushback.
		 */
		DISABLE_CC(false),

		/**
		 * If true, we just approximately count outstanding instead of
		 * maintaining an exact map with callbacks. Flag used for testing
		 * overhead.
		 */
		COUNT_OUTSTANDING(false),

		/**
		 * If true, we use a garbage collected map that has higher overhead than
		 * a regular map, but is still not a bottleneck.
		 */
		USE_GC_MAP(true),

		/**
		 * Only log meta decisions if corresponding accept was previously
		 * received.
		 */
		LOG_META_DECISIONS(true),

		/**
		 * FIXME: The options below only exist for testing stringification
		 * overhead. They should probably be moved to {@link TESTPaxosConfig}.
		 * Most of these will compromise safety.
		 */

		/******************* Start of testing options *******************/
		/**
		 * Testing option.
		 */
		JOURNAL_COMPRESSION(false),

		/**
		 * Testing option.
		 */
		STRINGIFY_WO_JOURNALING(false),

		/**
		 * Testing option.
		 */
		NON_COORD_ONLY(false),

		/**
		 * Testing option.
		 */
		COORD_ONLY(false),

		/**
		 * Testing option.
		 */
		NO_STRINGIFY_JOURNALING(false),

		/**
		 * Testing option.
		 */
		MESSENGER_CACHES_ACCEPT(false),

		/**
		 * Testing option.
		 */
		COORD_STRINGIFIES_WO_JOURNALING(false),

		/**
		 * Testing option.
		 */
		DONT_LOG_DECISIONS(false),

		/**
		 * Testing option.
		 */
		NON_COORD_DONT_LOG_DECISIONS(false),

		/**
		 * Testing option.
		 */
		COORD_DONT_LOG_DECISIONS(false),

		/**
		 * Testing option.
		 */
		COORD_JOURNALS_WO_STRINGIFYING(false),

		/**
		 * Testing option
		 */
		ALL_BUT_APPEND(false),

		/**
		 * Testing option. Implies no coordinator fault-tolerance.
		 */
		DISABLE_GET_LOGGED_MESSAGES(false),

		/*********** End of testing options *****************/

		/**
		 * Whether journal entries should be synchronously indexed in the DB.
		 * Makes journaling (and overall throughput) slower but makes safe
		 * retrieval of logged messages easier both during normal operations and
		 * upon recovery. During normal operations, we just have to check the DB
		 * and also check the pending log in memory to ensure that we don't miss
		 * any log messages. Upon recovery however, there is no pending log in
		 * memory and the system may have crashed with some pending log messages
		 * before they could be inserted into the DB, so we just have to find
		 * the last message logged in the DB and put the rest in a pending queue
		 * for logging into the DB.
		 */
		PAUSABLE_INDEX_JOURNAL(true),

		/**
		 * Whether more than one thread is used to log messages.
		 */
		MULTITHREAD_LOGGER(false),

		/**
		 * False for testing only. We do need to index the journal files in the
		 * DB. But this option for now emulates the (unimplemented) strategy of
		 * maintaining a memory log and only infrequently inserting the index
		 * entries into the DB.
		 */
		DB_INDEX_JOURNAL(false),

		/**
		 * Failure detection timeout in seconds after which a node will be
		 * considered dead if no keepalives have been received from it. Used to
		 * detect coordinator failures.
		 */
		FAILURE_DETECTION_TIMEOUT(6),

		/**
		 * Request timeout in seconds after which the request will be deleted
		 * from the outstanding queue. Currently, there is no effort to remove
		 * any other state for that request from the system.
		 */
		REQUEST_TIMEOUT(10),

		/**
		 * Whether the mapdb package should be used. It seems too slow for our
		 * purposes, so we don
		 */
		USE_MAP_DB(false),

		/**
		 * If true, the checkpoints table will be used to also store paused
		 * state as opposed to storing paused state in its own table. The reason
		 * to store paused state in the same table as checkpoints is that we can
		 * easily compute FIXME:
		 */
		USE_CHECKPOINTS_AS_PAUSE_TABLE(true),

		/**
		 * 
		 */
		USE_DISK_MAP(true),

		/**
		 * 
		 */
		USE_HEX_TIMESTAMP(true),

		/**
		 * 
		 */
		LAZY_COMPACTION(true),

		/**
		 * 
		 */
		PAUSE_BATCH_SIZE(100),

		/**
		 * 
		 */
		SYNC(false),

		/**
		 * 
		 */
		FLUSH(true),

		/**
		 * 
		 */
		SYNC_FCLOSE(true),

		/**
		 * 
		 */
		FLUSH_FCLOSE(true),

		/**
		 * Minimum seconds after last modification when a compaction attempt can
		 * be made.
		 */
		LOGFILE_AGE_THRESHOLD(10 * 60),

		/**
		 * Percentage variation around mean in checkpoint interval.
		 */
		CPI_NOISE(0),

		/**
		 * 
		 */
		BATCH_CHECKPOINTS(true),

		/**
		 * 
		 */
		MAX_DB_BATCH_SIZE(10000),

		/**
		 * Number of milliseconds after which
		 * {@link edu.umass.cs.gigapaxos.interfaces.Replicable#execute(edu.umass.cs.gigapaxos.interfaces.Request)}
		 * will be re-attempted until it returns true.
		 */
		HANDLE_REQUEST_RETRY_INTERVAL(100),

		/**
		 * Maximum number of retry attempts for
		 * {@link edu.umass.cs.gigapaxos.interfaces.Replicable#execute(edu.umass.cs.gigapaxos.interfaces.Request)}
		 * . After these many unsuccessful attempts, the paxos instance will be
		 * killed.
		 */
		HANDLE_REQUEST_RETRY_LIMIT(10),

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
	 * @param level
	 */
	public static void setConsoleHandler(Level level) {
		ConsoleHandler handler = new ConsoleHandler();
		handler.setLevel(level);
		PaxosManager.getLogger().setLevel(level);
		PaxosManager.getLogger().addHandler(handler);
		PaxosManager.getLogger().setUseParentHandlers(false);
		// NIOTransport.getLogger().setLevel(level);
		// NIOTransport.getLogger().addHandler(handler);
		// NIOTransport.getLogger().setUseParentHandlers(false);

	}

	protected static void setConsoleHandler() {
		setConsoleHandler(Level.INFO);
	}

	/**
	 * @return Default node config.
	 */
	public static NodeConfig<String> getDefaultNodeConfig() {
		final Map<String, InetSocketAddress> actives = PaxosConfig.getActives();

		return new ReconfigurableNodeConfig<String>() {

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
