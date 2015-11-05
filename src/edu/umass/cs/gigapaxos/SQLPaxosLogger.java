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

import com.mchange.v2.c3p0.ComboPooledDataSource;

import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.gigapaxos.paxospackets.AcceptPacket;
import edu.umass.cs.gigapaxos.paxospackets.PValuePacket;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket;
import edu.umass.cs.gigapaxos.paxospackets.PreparePacket;
import edu.umass.cs.gigapaxos.paxospackets.ProposalPacket;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.gigapaxos.paxospackets.StatePacket;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket.PaxosPacketType;
import edu.umass.cs.gigapaxos.paxosutil.Ballot;
import edu.umass.cs.gigapaxos.paxosutil.HotRestoreInfo;
import edu.umass.cs.gigapaxos.paxosutil.LogIndex;
import edu.umass.cs.gigapaxos.paxosutil.LogIndex.LogIndexEntry;
import edu.umass.cs.gigapaxos.paxosutil.LogMessagingTask;
import edu.umass.cs.gigapaxos.paxosutil.PaxosMessenger;
import edu.umass.cs.gigapaxos.paxosutil.PaxosInstanceCreationException;
import edu.umass.cs.gigapaxos.paxosutil.RecoveryInfo;
import edu.umass.cs.gigapaxos.paxosutil.SQL;
import edu.umass.cs.gigapaxos.paxosutil.SlotBallotState;
import edu.umass.cs.gigapaxos.paxosutil.StringContainer;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DiskMap;
import edu.umass.cs.utils.Diskable;
import edu.umass.cs.utils.MultiArrayMap;
import edu.umass.cs.utils.Util;
import edu.umass.cs.utils.DelayProfiler;

import org.json.JSONArray;
import org.json.JSONException;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import javax.sql.DataSource;

import java.beans.PropertyVetoException;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * @author V. Arun
 * 
 *         <p>
 * 
 *         This logger uses an embedded database for persistent storage. It is
 *         easily scalable to a very large number of paxos instances as the
 *         scale is only limited by the available disk space and the disk space
 *         needed increases as the number of paxos instances, the size of
 *         application state, and the inter-checkpointing interval.
 * 
 *         Concurrency: There is very little concurrency support here. All
 *         methods that touch the database are synchronized. We need to have
 *         connection pooling for better performance. The methods are
 *         synchronized coz we are mostly reusing a single connection, so we can
 *         not have prepared statements overlap while they are executing.
 * 
 *         Testing: Can be unit-tested using main.
 */
public class SQLPaxosLogger extends AbstractPaxosLogger {

	static {
		PaxosConfig.load();
	}

	/* ****************************************************************
	 * DB related parameters to be changed to use a different database service.
	 * Refer also to constants in paxosutil.SQL to update any constants.
	 */
	private static final SQL.SQLType SQL_TYPE = SQL.SQLType.valueOf(Config
			.getGlobalString(PC.SQL_TYPE)); // SQL.SQLType.MYSQL;
	private static final String DATABASE = Config
			.getGlobalString(PC.PAXOS_DB_PREFIX);// "paxos_logs";
	/* ************ End of DB service related parameters ************** */

	protected static final String LOG_DIRECTORY = Config
			.getGlobalString(PC.PAXOS_LOGS_DIR);// "paxos_logs";
	private static final boolean CONN_POOLING = true; // should stay true
	private static final int MAX_POOL_SIZE = 100; // no point fiddling

	/**
	 * Don't change any of the table names below, otherwise it will break
	 * recovery.
	 */
	private static final String CHECKPOINT_TABLE = "checkpoint";
	private static final String PREV_CHECKPOINT_TABLE = "prev_checkpoint";
	private static final String PAUSE_TABLE = "pause";
	private static final String MESSAGES_TABLE = "messages";

	/**
	 * Disable persistent logging altogether
	 */
	private static final boolean DISABLE_LOGGING = Config
			.getGlobalBoolean(PaxosConfig.PC.DISABLE_LOGGING);

	/**
	 * Maximum size of a log message; depends on RequestBatcher.MAX_BATCH_SIZE
	 */
	public static final int MAX_LOG_MESSAGE_SIZE = Config
			.getGlobalInt(PaxosConfig.PC.MAX_LOG_MESSAGE_SIZE); // 32672;
	/**
	 * Maximum size of checkpoint state.
	 */
	private static final int MAX_CHECKPOINT_SIZE = Config
			.getGlobalInt(PaxosConfig.PC.MAX_CHECKPOINT_SIZE);// 32672;

	/**
	 * Maximum character length of a paxos group name.
	 */
	public static final int MAX_PAXOS_ID_SIZE = Config
			.getGlobalInt(PC.MAX_PAXOS_ID_SIZE);
	private static final int MAX_GROUP_SIZE = Config
			.getGlobalInt(PC.MAX_GROUP_SIZE);
	/**
	 * Maximum length of a comma separated set of int members of a paxos group.
	 */
	public static final int MAX_GROUP_STR_LENGTH = MAX_GROUP_SIZE * 16;
	/**
	 * Pause state is just the group members plus a few other scalar fields.
	 */
	private static final int PAUSE_STATE_SIZE = MAX_GROUP_STR_LENGTH * 4;
	private static final int LOG_INDEX_SIZE = Config
			.getGlobalInt(PC.CHECKPOINT_INTERVAL) * 128;

	/**
	 * Truncated checkpoint state size for java logging purposes
	 */
	private static final int TRUNCATED_STATE_SIZE = 20;
	private static final int MAX_OLD_DECISIONS = Config
			.getGlobalInt(PC.CHECKPOINT_INTERVAL);
	/**
	 * Needed for testing with recovery in the same JVM
	 */
	private static final boolean DONT_SHUTDOWN_EMBEDDED = true;

	private static final int MAX_FILENAME_SIZE = 512;

	/**
	 * Batching can make log messages really big, so we need a maximum size here
	 * to ensure that we don't try to batch more than we can chew.
	 */
	private static int maxLogMessageSize = MAX_LOG_MESSAGE_SIZE;

	private static boolean getLogMessageBlobOption() {
		return (maxLogMessageSize > SQL.getVarcharSize(SQL_TYPE))
				|| Config.getGlobalBoolean(PC.BATCHING_ENABLED);
	}

	private static int maxCheckpointSize = MAX_CHECKPOINT_SIZE;

	private static boolean getCheckpointBlobOption() {
		return (maxCheckpointSize > SQL.getVarcharSize(SQL_TYPE));
	}

	private static enum C {
		PAXOS_ID, VERSION, SLOT, BALLOTNUM, COORDINATOR, PACKET_TYPE, MIN_LOGFILE, STATE, LOGFILE, OFFSET, LENGTH, MESSAGE, CREATE_TIME
	};

	private static final ArrayList<SQLPaxosLogger> instances = new ArrayList<SQLPaxosLogger>();

	private ComboPooledDataSource dataSource = null;
	private Connection defaultConn = null;
	private Connection cursorConn = null;

	private final Journaler journaler;
	private final MapDBContainer mapDB;

	private boolean closed = true;

	// disables message logging overriding ENABLE_JOURNALING
	protected static boolean isLoggingEnabled() {
		return !DISABLE_LOGGING;
	}

	protected static boolean isJournalingEnabled() {
		return ENABLE_JOURNALING;
	}

	/*
	 * The global statements are not really need and can be replaced by local
	 * variables in log(.) and duplicateOrOutdated(.) but are supposedly more
	 * efficient. But they don't seem to speed it up much. But at some point,
	 * they did, so these are still being used.
	 */
	private PreparedStatement logMsgStmt = null;
	private PreparedStatement checkpointStmt = null;
	private PreparedStatement cursorPstmt = null;
	private ResultSet cursorRset = null;

	private final String strID;

	private final ScheduledExecutorService GC;
	private final MessageLogDiskMap messageLog;

	private static Logger log = PaxosManager.getLogger();

	SQLPaxosLogger(int id, String strID, String dbPath, PaxosMessenger<?> messenger) {
		super(id, dbPath, messenger);
		this.strID = strID;
		GC = Executors.newScheduledThreadPool(2, new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				Thread thread = Executors.defaultThreadFactory().newThread(r);
				thread.setName(strID);
				return thread;
			}
		}); // new Timer(strID);
		addDerbyLogger(this);
		this.journaler = new Journaler(this.logDirectory, this.myID);
		this.deleteTmpJournalFiles();

		this.mapDB = USE_MAP_DB ? new MapDBContainer(DBMaker.fileDB(
				new File(this.getLogIndexDBPrefix())).make(), DBMaker
				.memoryDB().transactionDisable().make()) : null;
				
		Diskable<String,LogIndex> disk = new Diskable<String,LogIndex>() {

			@Override
			public Set<String> commit(Map<String,LogIndex> toCommit) throws IOException {
				return SQLPaxosLogger.this.pauseLogIndex(toCommit);
			}

			@Override
			public LogIndex restore(String key) throws IOException {
				return SQLPaxosLogger.this.unpauseLogIndex(key);
			}
			
			public String toString() {
				return MessageLogDiskMap.class.getSimpleName()+SQLPaxosLogger.this.myID;
			}
			
		};
		this.messageLog = USE_MAP_DB ? new MessageLogMapDB(this.mapDB.inMemory,
				this.mapDB.onDisk, disk)
				: USE_DISK_MAP ? new MessageLogDiskMap(disk)
						: new MessageLogPausable(disk);
				
		initialize(); // will set up db, connection, tables, etc. as needed
	}

	private String getLogIndexDBPrefix() {
		return this.logDirectory + "logIndex" + this.myID;
	}

	/**
	 * @param id
	 * @param dbPath
	 * @param messenger
	 */
	public SQLPaxosLogger(int id, String dbPath, PaxosMessenger<?> messenger) {
		this(id, "" + id, dbPath, messenger);

	}
	

	/*
	 * This is currently the default MessageLog and is based on DiskMap that is
	 * a hash map whose infrequently used entries automatically get paused to
	 * disk.
	 */
	static class MessageLogDiskMap extends DiskMap<String,LogIndex> {
		
		final Diskable<String,LogIndex> disk;

		MessageLogDiskMap(Diskable<String,LogIndex> disk) {
			//super(new MultiArrayMap<String, LogIndex>(Config.getGlobalInt(PC.PINSTANCES_CAPACITY)));
			super(128*1024);
			this.disk = disk;
		}
		
		synchronized LogIndex getOrCreateIfNotExistsOrLower(String paxosID,
				int version) {
			LogIndex logIndex = null;
			if ((logIndex = this.get(paxosID)) == null
					|| (logIndex.version - version < 0)) {
				LogIndex prev = this.put(paxosID,
						logIndex = new LogIndex(paxosID, version));
				log.log(Level.INFO, "{0} created logIndex {1}:{2} {3}",
						new Object[] {
								this.disk,
								paxosID,
								version,
								prev != null ? " replacing " + prev.paxosID
										+ ":" + prev.version : "" });
			}
			if (logIndex != null && logIndex.version != version)
				log.log(Level.INFO,
						"{0} found higher logIndex {1}:{2}:{3} when looking for version {4}",
						new Object[] { this.disk, paxosID, logIndex.version,
								logIndex, version });

			return logIndex!=null && logIndex.version == version ? logIndex : null;
		}

		synchronized void add(PaxosPacket msg, String logfile, long offset,
				int length) {
			//long t = System.nanoTime();
			LogIndex logIndex = this.getOrCreateIfNotExistsOrLower(
					msg.getPaxosID(), msg.getVersion());
			if(logIndex==null) return;

			boolean isPValue = msg instanceof PValuePacket;
			logIndex.add(isPValue ? ((PValuePacket) msg).slot
					: ((PreparePacket) msg).firstUndecidedSlot,
					isPValue ? ((PValuePacket) msg).ballot.ballotNumber
							: ((PreparePacket) msg).ballot.ballotNumber,
					isPValue ? ((PValuePacket) msg).ballot.coordinatorID
							: ((PreparePacket) msg).ballot.coordinatorID, msg
							.getType().getInt(), logfile, offset, length);
			this.put(msg.getPaxosID(), logIndex);
			//if (Util.oneIn(10)) DelayProfiler.updateDelayNano("logAddDelay", t);
		}

		synchronized void setGCSlot(String paxosID, int version, int gcSlot) {
			LogIndex logIndex = this.getOrCreateIfNotExistsOrLower(paxosID, version);
			if(logIndex==null) return;

			logIndex.setGCSlot(gcSlot);
			this.put(paxosID, logIndex);
			
		}

		 LogIndex getLogIndex(String paxosID, int version) {
			LogIndex logIndex = this.get(paxosID);
			if (logIndex != null && logIndex.version != version)
				log.log(Level.INFO,
						"{0} has conflicting logIndex {1}:{2}:{3} when looking for version {3}",
						new Object[] { disk, paxosID, logIndex.version, logIndex, version });
			return logIndex != null && logIndex.version == version ? logIndex
					: null;
		}

		 String toString(String paxosID) {
			LogIndex logIndex = this.get(paxosID);
			return logIndex != null ? logIndex.toString() : null;
		}

		 LogIndex getLogIndex(String paxosID) {
			return this.get(paxosID);
		}

		 String getMinLogfile(String paxosID) {
			LogIndex logIndex = this.get(paxosID);
			return logIndex != null ? logIndex.getMinLogfile() : null;
		}

		 void uncache(String paxosID) {
			// do nothing
		}

		 void restore(LogIndex logIndex) throws IOException {
			// do nothing
			 this.hintRestore(logIndex.paxosID, logIndex);
		}

		@Override
		public Set<String> commit(Map<String, LogIndex> toCommit)
				throws IOException {
			return this.disk.commit(toCommit);
		}

		@Override
		public LogIndex restore(String key) throws IOException {
			return this.disk.restore(key);
		}

		public synchronized void modifyLogIndexEntry(String paxosID, LogIndexEntry entry) {
			LogIndex logIndex = this.get(paxosID);
			assert (logIndex != null) : paxosID
					+ " logIndex not found while trying to replace it with ["
					+ entry.getLogfile() + ", " + entry.getOffset() + ", "
					+ entry.getLength();
			if(logIndex.modify(entry))
				this.put(paxosID, logIndex);
		}
	}
	
	private static int getSlot(PaxosPacket logMsg) {
		assert(logMsg instanceof PreparePacket || logMsg instanceof PValuePacket);
		return logMsg instanceof PreparePacket ? ((PreparePacket)logMsg).firstUndecidedSlot : 
			((PValuePacket)logMsg).ballot.ballotNumber;
	}
	private static Ballot getBallot(PaxosPacket logMsg) {
		assert(logMsg instanceof PreparePacket || logMsg instanceof PValuePacket);
		return logMsg instanceof PreparePacket ? ((PreparePacket)logMsg).ballot : 
			((PValuePacket)logMsg).ballot;
	}

	private synchronized Connection getDefaultConn() throws SQLException {
		return dataSource.getConnection();
	}

	private synchronized Connection getCursorConn() throws SQLException {
		return (this.cursorConn = this.dataSource.getConnection());
	}

	// testing
	private void putCheckpointState(String paxosID, int version, int[] group,
			int slot, Ballot ballot, String state, int acceptedGCSlot) {
		this.putCheckpointState(paxosID, version,
				Util.arrayOfIntToStringSet(group), slot, ballot, state,
				acceptedGCSlot, System.currentTimeMillis());
	}

	@Override
	public void putCheckpointState(String paxosID, int version,
			Set<String> group, int slot, Ballot ballot, String state,
			int acceptedGCSlot) {
		this.putCheckpointState(paxosID, version, (group), slot, ballot, state,
				acceptedGCSlot, System.currentTimeMillis());

	}

	public boolean copyEpochFinalCheckpointState(String paxosID, int version) {
		if (isClosed() /* || !isLoggingEnabled() */)
			return true;

		boolean copied = false;
		// Stupid derby doesn't have an insert if not exist command
		String insertCmd = "insert into "
				+ getPCTable()
				+ " (version,members,slot,ballotnum,coordinator,state,create_time, paxos_id) values (?,?,?,?,?,?,?,?)";
		String updateCmd = "update "
				+ getPCTable()
				+ " set version=?,members=?, slot=?, ballotnum=?, coordinator=?, state=?, create_time=? where paxos_id=?";
		String cmd = this.existsRecord(getPCTable(), paxosID) ? updateCmd
				: insertCmd;
		String readCmd = "select version, members, slot, ballotnum, coordinator, state, create_time from "
				+ getCTable() + " where paxos_id=?";
		PreparedStatement readCP = null;
		ResultSet cpRecord = null;
		Connection conn = null;
		PreparedStatement insertCP = null;
		try {
			conn = this.getDefaultConn();

			readCP = conn.prepareStatement(readCmd);
			readCP.setString(1, paxosID);
			cpRecord = readCP.executeQuery();

			while (cpRecord.next()) {
				if (version != cpRecord.getInt("version"))
					break;

				insertCP = conn.prepareStatement(cmd);
				insertCP.setInt(1, version);
				insertCP.setString(2, cpRecord.getString("members"));
				insertCP.setInt(3, cpRecord.getInt("slot"));
				insertCP.setInt(4, cpRecord.getInt("ballotnum"));
				insertCP.setInt(5, cpRecord.getInt("coordinator"));
				if (getCheckpointBlobOption()) {
					insertCP.setBlob(7, cpRecord.getBlob("state"));
				} else
					insertCP.setString(6, cpRecord.getString("state"));
				insertCP.setLong(7, cpRecord.getLong("create_time"));
				insertCP.setString(8, paxosID);
				copied = (insertCP.executeUpdate() > 0);
				// conn.commit();
				log.log(Level.INFO,
						"{0} copied epoch final state for {1}:{2}: [{3}]",
						new Object[] {
								this,
								paxosID,
								version,
								Util.truncate(
										(getCheckpointBlobOption() ? new String(
												cpRecord.getBytes("state"),
												CHARSET) : cpRecord
												.getString("state")), 32, 32) });
			}
		} catch (SQLException | UnsupportedEncodingException sqle) {
			log.severe("SQLException while copying epoch final state for "
					+ paxosID
					+ ":"
					+ version
					+ " using ["
					+ cmd
					+ "]. This node may be unable to participate in future epochs for "
					+ paxosID);
			sqle.printStackTrace();
		} finally {
			cleanup(readCP, cpRecord);
			cleanup(insertCP);
			cleanup(conn);
		}
		return copied;
	}

	/**
	 * The epoch final state checkpoint should have been taken not too long back
	 * as it should roughly reflect the time to take the checkpoint itself. If
	 * we allow for arbitrarily old epoch final state, then we can not safely
	 * allow name re-creation, i.e., a deletion followed by a creation of the
	 * same name (as there is no way to distinguish between the current
	 * incarnation and the previous incarnation before deletion of the name)
	 * unless we ensure that deletions are possible only when all final state
	 * for that name has been completely deleted from all active replicas (a
	 * sufficient condition for which is that all previous epoch final state is
	 * dropped before further reconfigurations can happen for *all* (including
	 * non-deletion) reconfigurations.
	 * 
	 * Assumption for safety: (1) Epoch final state older than
	 * MAX_FINAL_STATE_AGE is never used. (2) Name deletions are not committed
	 * as complete unless either MAX_FINAL_STATE_AGE time has passed in a
	 * pending delete state or *all* active replica nodes in the system have
	 * confirmed deletion of any state for the name.
	 * 
	 * Progress implications: If a reconfiguration is interrupted after stopping
	 * the previous epoch and before starting the next epoch for a duration
	 * longer than MAX_FINAL_STATE_AGE, the reconfiguration will be stuck as
	 * there is no safe way to complete it. The only straightforward way to
	 * alleviate this problem seems to be to prevent reconfigurations in the
	 * first place from making further progress until all previous epoch
	 * replicas have dropped their final state; if so, MAX_FINAL_STATE_AGE can
	 * be infinity, but the flip side is that typical reconfigurations can get
	 * stuck because of the failure of even a single active replica, which is
	 * even more undesirable. So we go with a finite MAX_FINAL_STATE_AGE.
	 */
	public static final long MAX_FINAL_STATE_AGE = Config
			.getGlobalInt(PC.MAX_FINAL_STATE_AGE);

	@Override
	public StringContainer getEpochFinalCheckpointState(String paxosID,
			int version) {
		SlotBallotState sbs = this.getSlotBallotState(getPCTable(), paxosID,
				version, true);
		if (sbs == null)
			log.log(Level.INFO,
					"{0} did not find any epoch final state for {1}:{2}; last version = {3}",
					new Object[] { this, paxosID, version,
							this.getEpochFinalCheckpointVersion(paxosID) });
		return sbs != null
				&& (System.currentTimeMillis() - sbs.getCreateTime() < MAX_FINAL_STATE_AGE) ? new StringContainer(
				sbs.state) : null;
	}

	private boolean garbageCollectEpochFinalCheckpointState(String paxosID,
			int version) {
		SlotBallotState sbs = this.getSlotBallotState(getPCTable(), paxosID,
				version, true);
		if (sbs != null
				&& (System.currentTimeMillis() - sbs.getCreateTime() > MAX_FINAL_STATE_AGE))
			return this.deleteEpochFinalCheckpointState(paxosID, version);
		return false;
	}

	// can reuse getSlotBallotState here
	@Override
	public Integer getEpochFinalCheckpointVersion(String paxosID) {
		SlotBallotState sbs = this.getSlotBallotState(getPCTable(), paxosID, 0,
				false);
		if (sbs != null)
			if (System.currentTimeMillis() - sbs.getCreateTime() < MAX_FINAL_STATE_AGE)
				return sbs.getVersion();
			else {
				log.log(Level.INFO,
						"{0} garbage collecting expired epoch final checkpoint state for {1}:{2}",
						new Object[] { paxosID, sbs.getVersion() });
				this.garbageCollectEpochFinalCheckpointState(paxosID,
						sbs.getVersion());
			}
		;
		return null;
	}

	static class Journaler {
		private static final String PREFIX = "log.";
		private static final String POSTPREFIX = ".";
		private final int myID;
		private final String logdir;
		private final String logfilePrefix;
		private String curLogfile = null;
		private FileOutputStream fos;
		private long curLogfileSize = 0;
		private int numLogfiles = 0;
		private int numOngoingGCs = 0;
		private Object fosLock = new Object();

		Journaler(String logdir, int myID) {
			this.myID = myID;
			this.logdir = logdir;
			this.logfilePrefix = PREFIX + myID + POSTPREFIX;
			assert (this.logdir != null && this.logfilePrefix != null);
			this.curLogfile = generateLogfileName();
			this.fos = createLogfile(curLogfile, true);
		}

		private String getLogfilePrefix() {
			return this.logdir + PREFIX + this.myID + POSTPREFIX;
		}

		private FileOutputStream createLogfile(String filename) {
			return this.createLogfile(filename, false);
		}

		private String generateLogfileName() {
			return this.logdir
					+ this.logfilePrefix
					+ (USE_HEX_TIMESTAMP ? Long.toHexString(System
							.currentTimeMillis()) : System.currentTimeMillis());
		}

		private FileOutputStream createLogfile(String filename,
				boolean deleteEmpty) {
			assert (this.logdir != null && this.logfilePrefix != null);
			if (deleteEmpty)
				this.deleteEmptyLogfiles();
			try {
				new File(filename).getParentFile().mkdirs();
				(new FileWriter(filename, false)).close();
				this.fos = new FileOutputStream(new File(filename));
				this.curLogfileSize = 0;
				this.numLogfiles++;
				log.log(Level.INFO, "{0} created new log file {1}",
						new Object[] { this, this.curLogfile });
				return this.fos;
			} catch (IOException e) {
				if (ENABLE_JOURNALING) {
					log.severe("Unable to create log file " + filename
							+ "; exiting");
					e.printStackTrace();
					System.exit(1);
				} // else ignore
			}
			return null;
		}
		
		boolean shouldGC() {
			if(this.numLogfiles>0 && this.numLogfiles%JOURNAL_GC_FREQUENCY==0)
				return true;
			return false;
		}

		public String toString() {
			return this.getClass().getSimpleName() + this.myID;
		}

		private void deleteEmptyLogfiles() {
			File[] emptyFiles = new File(this.logdir)
					.listFiles(new FileFilter() {

						@Override
						public boolean accept(File pathname) {
							return pathname.isFile()
									&& pathname.length() == 0
									&& pathname.toString().startsWith(
											Journaler.this.getLogfilePrefix());
						}

					});
			if (emptyFiles != null)
				for (File f : emptyFiles)
					f.delete();
		}

		private void rollLogFile() {
			synchronized (fosLock) {
				// check again here
				if (curLogfileSize > MAX_LOG_FILE_SIZE) {
					try {
						if(FLUSH_FCLOSE) fos.flush();
						if(SYNC_FCLOSE)
							fos.getFD().sync();
						fos.close();
						fos = createLogfile(curLogfile = generateLogfileName());
						curLogfileSize = 0;
					} catch (IOException e) {
						log.severe(this + " unable to close existing log file "
								+ this.curLogfile);
						e.printStackTrace();
					} finally {
						if (fos == null)
							Util.suicide(this + " unable to open log file "
									+ this.curLogfile + "; exiting");
					}
				}
			}
		}

		private void appendToLogFile(byte[] bytes) throws IOException {
			synchronized (fosLock) {
				fos.write(bytes);
				if(FLUSH) fos.flush();
				// will sync to disk but will be slow as hell
				if(SYNC) fos.getFD().sync();
				curLogfileSize += bytes.length;
			}
		}

		private TreeSet<Filename> getGCCandidates() {
			synchronized (fosLock) {
				File[] dirFiles = (new File(this.logdir))
						.listFiles(new FileFilter() {
							@Override
							public boolean accept(File pathname) {
								return pathname.toString().startsWith(
										Journaler.this.getLogfilePrefix());
							}
						});
				TreeSet<Filename> candidates = new TreeSet<Filename>();
				for (File f : dirFiles)
					if (!f.toString().equals(curLogfile))
						candidates.add(new Filename(f));
				return candidates;
			}
		}
	}

	private static final int MAX_LOG_FILE_SIZE = Config.getGlobalInt(PC.MAX_LOG_FILE_SIZE);

	/*
	 * Deletes all but the most recent checkpoint for the RC group name. We
	 * could track recency based on timestamps using either the timestamp in the
	 * filename or the OS file creation time. Here, we just supply the latest
	 * checkpoint filename explicitly as we know it when this method is called
	 * anyway.
	 */
	private static boolean deleteOldCheckpoints(final String cpDir,
			final String rcGroupName, int keep, Object lockMe) {
		File dir = new File(cpDir);
		assert (dir.exists());
		// get files matching the prefix for this rcGroupName's checkpoints
		File[] foundFiles = dir.listFiles(new FilenameFilter() {
			public boolean accept(File dir, String name) {
				return name.startsWith(rcGroupName);
			}
		});
		if (foundFiles.length == 0)
			log.fine(" no file in " + cpDir + " starting with " + rcGroupName);

		// delete all but the most recent
		boolean allDeleted = true;
		for (Filename f : getAllButLatest(foundFiles, keep))
			allDeleted = allDeleted && deleteFile(f.file, lockMe);
		return allDeleted;
	}

	private static boolean deleteFile(File f, Object lockMe) {
		synchronized (lockMe) {
			return f.delete();
		}
	}

	private static Set<Filename> getAllButLatest(File[] files, int keep) {
		TreeSet<Filename> allFiles = new TreeSet<Filename>();
		TreeSet<Filename> oldFiles = new TreeSet<Filename>();
		for (File file : files)
			allFiles.add(new Filename(file));
		if (allFiles.size() <= keep)
			return oldFiles;
		Iterator<Filename> iter = allFiles.iterator();
		for (int i = 0; i < allFiles.size() - keep; i++)
			oldFiles.add(iter.next());

		return oldFiles;
	}

	private static SortedSet<Filename> getLatest(File[] files, int numLatest) {
		TreeSet<Filename> allFiles = new TreeSet<Filename>();
		TreeSet<Filename> oldFiles = new TreeSet<Filename>();
		for (File file : files)
			allFiles.add(new Filename(file));
		if (allFiles.size() <= numLatest)
			return allFiles;
		Iterator<Filename> iter = allFiles.descendingIterator();
		for (int i = 0; i < numLatest; i++)
			oldFiles.add(iter.next());

		return oldFiles;
	}

	private static File[] toFiles(Filename[] filenames) {
		File[] files = new File[filenames.length];
		for (int i = 0; i < files.length; i++)
			files[i] = filenames[i].file;
		return files;
	}

	private static class Filename implements Comparable<Filename> {
		final File file;

		Filename(File f) {
			this.file = f;
		}

		// FIXME: should use the logical timestamp in filename
		@Override
		public int compareTo(SQLPaxosLogger.Filename o) {
			long t1 = getLTS(file);
			long t2 = getLTS(o.file);

			if (t1 < t2)
				return -1;
			else if (t1 == t2)
				return 0;
			else
				return 1;
		}
		
		private static long getLTS(File file) {
			String[] tokens = file.toString().split("\\.");
			assert (tokens[tokens.length - 1].matches("[0-9a-fA-F]*$")) : file;
			try {
				return USE_HEX_TIMESTAMP ? Long.parseLong(
						tokens[tokens.length - 1], 16) : Long
						.valueOf(tokens[tokens.length - 1]);
			} catch (NumberFormatException nfe) {
				nfe.printStackTrace();
			}
			return file.lastModified();
		}
	}

	private static final byte[] testBytes = new byte[2000 * 1000];
	static {
		for (int i = 0; i < testBytes.length; i++)
			testBytes[i] = (byte) (-256 + (int) (Math.random() * 256));
	}

	private PendingLogTask[] journal(LogMessagingTask[] packets) {
		if (!ENABLE_JOURNALING)
			return new PendingLogTask[0]; // no error
		if (this.journaler.fos == null)
			return null; // error
		boolean amCoordinator = false, isAccept = false;
		PendingLogTask[] pending = new PendingLogTask[packets.length];
		for (int i = 0; i < packets.length; i++) {
			LogMessagingTask pkt = packets[i];
			amCoordinator = pkt.logMsg instanceof PValuePacket ? ((PValuePacket) pkt.logMsg).ballot.coordinatorID == myID
					: pkt.logMsg instanceof PreparePacket ? ((PreparePacket) pkt.logMsg).ballot.coordinatorID == myID
							: false;
			isAccept = pkt.logMsg.getType() == PaxosPacketType.ACCEPT; 
			if (DONT_LOG_DECISIONS && !isAccept)
				continue;
			if (NON_COORD_ONLY && amCoordinator
					&& !COORD_STRINGIFIES_WO_JOURNALING)
				continue;
			if (COORD_ONLY && !amCoordinator)
				continue;
			if (NON_COORD_DONT_LOG_DECISIONS && !amCoordinator && !isAccept)
				continue;
			if (COORD_DONT_LOG_DECISIONS && amCoordinator && !isAccept)
				continue;

			try {
				{
					byte[] bytes = !NO_STRINGIFY_JOURNALING
							&& !(COORD_JOURNALS_WO_STRINGIFYING && amCoordinator) ? toBytes(pkt.logMsg)
							: Arrays.copyOf(testBytes,
									((RequestPacket) pkt.logMsg)
											.lengthEstimate());
					if (JOURNAL_COMPRESSION)
						bytes = deflate(bytes);

					// format: <size><message>*
					ByteBuffer bbuf = ByteBuffer.allocate(4 + bytes.length);
					bbuf.putInt(bytes.length);
					bbuf.put(bytes);

					if (ALL_BUT_APPEND)
						continue;

					if (STRINGIFY_WO_JOURNALING
							|| (COORD_STRINGIFIES_WO_JOURNALING && amCoordinator))
						continue;

					// else append to log file *after* creating pending task
					if (DB_INDEX_JOURNAL)
						synchronized (this) {
							SQLPaxosLogger.this.pendingLogMessages
									.add(pending[i] = new PendingLogTask(
											packets[i],
											this.journaler.curLogfile,
											this.journaler.curLogfileSize,
											bytes.length));
						}
					else if (PAUSABLE_INDEX_JOURNAL)
						this.messageLog.add(packets[i].logMsg,
								this.journaler.curLogfile,
								this.journaler.curLogfileSize, bytes.length);
					if (USE_MAP_DB && Util.oneIn(1000))
						this.mapDB.dbMemory.commit();
					SQLPaxosLogger.this.journaler.appendToLogFile(bbuf.array());
					assert (pending[i] == null || this.journaler.curLogfileSize == pending[i].logfileOffset
							+ bbuf.capacity());
				}

			} catch (IOException ioe) {
				ioe.printStackTrace();
				return null;
			}
		}

		if (this.journaler.curLogfileSize > MAX_LOG_FILE_SIZE) {
			// always commit pending before rolling log file
			log.log(Level.INFO, "{0} rolling log file {1}", new Object[] {
					SQLPaxosLogger.this.journaler,
					SQLPaxosLogger.this.journaler.curLogfile });
			//DelayProfiler.updateMovAvg("#fgsync", this.pendingLogMessages.size());
			// first sync, then roll log file
			SQLPaxosLogger.this.syncLogMessagesIndex();
			long t = System.currentTimeMillis();
			SQLPaxosLogger.this.journaler.rollLogFile();
			DelayProfiler.updateDelay("rolllog", t, 1.0);

			
			if (this.journaler.shouldGC()) {
				this.GC.submit(new TimerTask() {
					@Override
					public void run() {
						try {
							Thread.currentThread().setPriority(Thread.MIN_PRIORITY);
							SQLPaxosLogger.this
									.garbageCollectJournal(SQLPaxosLogger.this.journaler
											.getGCCandidates());
						} catch (Exception | Error e) {
							log.severe(this + " incurred exception "
									+ e.getMessage()
									+ " while garbage collecting logfiles");
							e.printStackTrace();
						}
					}
				}, 0);
			}
		}
		if (!DB_INDEX_JOURNAL && Util.oneIn(Integer.MAX_VALUE))
			// used only for testing
			SQLPaxosLogger.deleteOldCheckpoints(logDirectory,
					SQLPaxosLogger.this.journaler.logfilePrefix, 5, this);

		return pending;
	}

	private byte[] toBytes(PaxosPacket packet)
			throws UnsupportedEncodingException {
		return toString(packet).getBytes(CHARSET);
	}

	private String toString(PaxosPacket packet) {
		return packet.toString();
	}

	// various options for performance testng below
	private static final boolean ENABLE_JOURNALING = Config
			.getGlobalBoolean(PC.ENABLE_JOURNALING);
	private static final boolean STRINGIFY_WO_JOURNALING = Config
			.getGlobalBoolean(PC.STRINGIFY_WO_JOURNALING);
	private static final boolean NON_COORD_ONLY = Config
			.getGlobalBoolean(PC.NON_COORD_ONLY);
	private static final boolean COORD_ONLY = Config
			.getGlobalBoolean(PC.NON_COORD_ONLY);
	private static final boolean NO_STRINGIFY_JOURNALING = Config
			.getGlobalBoolean(PC.NO_STRINGIFY_JOURNALING);
	private static final boolean COORD_STRINGIFIES_WO_JOURNALING = Config
			.getGlobalBoolean(PC.COORD_STRINGIFIES_WO_JOURNALING);
	private static final boolean COORD_JOURNALS_WO_STRINGIFYING = Config
			.getGlobalBoolean(PC.COORD_JOURNALS_WO_STRINGIFYING);
	private static final boolean DONT_LOG_DECISIONS = Config
			.getGlobalBoolean(PC.DONT_LOG_DECISIONS);
	private static final boolean NON_COORD_DONT_LOG_DECISIONS = Config
			.getGlobalBoolean(PC.NON_COORD_DONT_LOG_DECISIONS);
	private static final boolean COORD_DONT_LOG_DECISIONS = Config
			.getGlobalBoolean(PC.COORD_DONT_LOG_DECISIONS);
	private static final boolean JOURNAL_COMPRESSION = Config
			.getGlobalBoolean(PC.JOURNAL_COMPRESSION);
	private static final boolean PAUSABLE_INDEX_JOURNAL = Config
			.getGlobalBoolean(PC.PAUSABLE_INDEX_JOURNAL);
	private static final boolean DB_INDEX_JOURNAL = Config
			.getGlobalBoolean(PC.DB_INDEX_JOURNAL);
	private static final boolean SYNC = Config
			.getGlobalBoolean(PC.SYNC);
	private static final boolean SYNC_FCLOSE = Config
			.getGlobalBoolean(PC.SYNC_FCLOSE);
	private static final boolean FLUSH_FCLOSE = Config
			.getGlobalBoolean(PC.FLUSH_FCLOSE);
	private static final boolean FLUSH = Config
			.getGlobalBoolean(PC.FLUSH);

	private static final int LOG_INDEX_FREQUENCY = Config
			.getGlobalInt(PC.LOG_INDEX_FREQUENCY);
	private static final int JOURNAL_GC_FREQUENCY = Config
			.getGlobalInt(PC.JOURNAL_GC_FREQUENCY);
	private static final int COMPACTION_FREQUENCY = Config
			.getGlobalInt(PC.COMPACTION_FREQUENCY);
	private static final boolean ALL_BUT_APPEND = Config
			.getGlobalBoolean(PC.ALL_BUT_APPEND);
	private static final boolean DISABLE_GET_LOGGED_MESSAGES = Config
			.getGlobalBoolean(PC.DISABLE_GET_LOGGED_MESSAGES);
	private static final boolean USE_MAP_DB = Config
			.getGlobalBoolean(PC.USE_MAP_DB);
	private static final boolean USE_DISK_MAP = Config
			.getGlobalBoolean(PC.USE_DISK_MAP);
	private static final boolean DISABLE_CHECKPOINTING = Config
			.getGlobalBoolean(PC.DISABLE_CHECKPOINTING);
	private static final boolean USE_HEX_TIMESTAMP = Config
			.getGlobalBoolean(PC.USE_HEX_TIMESTAMP);
	private static final boolean LAZY_COMPACTION = Config
			.getGlobalBoolean(PC.LAZY_COMPACTION);	

	private static final boolean USE_CHECKPOINTS_AS_PAUSE_TABLE = Config.getGlobalBoolean(PC.USE_CHECKPOINTS_AS_PAUSE_TABLE);

	private static final int MAX_DB_BATCH_SIZE = Config.getGlobalInt(PC.MAX_DB_BATCH_SIZE);

	/*
	 * A wrapper to select between the purely DB-based logger and the
	 * work-in-progress journaling logger.
	 */
	@Override
	public boolean logBatch(final LogMessagingTask[] packets) {
		if (isClosed())
			return false;
		if (!isLoggingEnabled())
			return true;
		if (!isJournalingEnabled())
			// no need to journal and the file, offset have no meaning here
			return this.logBatchDB(packets);

		// else journaling with just indexes in DB
		String journalFile = this.journaler.curLogfile;
		PendingLogTask[] pending = null;
		boolean journaled = (ENABLE_JOURNALING && (pending = this
				.journal(packets)) != null);
		assert (pending != null);
		if (!journaled || !DB_INDEX_JOURNAL)
			return journaled;

		String[] journalFiles = new String[packets.length];
		for (int i = 0; i < packets.length; i++)
			journalFiles[i] = journalFile;
		// synchronous indexing
		if (LOG_INDEX_FREQUENCY == 0)
			return this.syncLogMessagesIndex(); 
		// asynchronous indexing
		log.log(Level.FINER, "{0} has {1} pending log messages", new Object[] {
				this, this.pendingLogMessages.size() });
		// not strictly necessary coz we index upon rolling logfile anyway
		if (Util.oneIn(LOG_INDEX_FREQUENCY))
			SQLPaxosLogger.this.syncLogMessagesIndexBackground();
		// else no indexing of journal
		return journaled;
	}

	private LinkedList<PendingLogTask> pendingLogMessages = new LinkedList<PendingLogTask>();

	// latches meaningless journal files and offsets
	private boolean logBatchDB(LogMessagingTask[] packets) {
		PendingLogTask[] pending = new PendingLogTask[packets.length];
		for (int i = 0; i < packets.length; i++)
			pending[i] = new PendingLogTask(packets[i],
					this.journaler.curLogfile, this.journaler.curLogfileSize, 0);
		return this.logBatchDB(pending);
	}

	/*
	 * The main method to log to the DB. If journaling is enabled, this method
	 * is always called after journaling; in that case, this method performs
	 * indexing.
	 */
	private synchronized boolean logBatchDB(PendingLogTask[] packets) {
		if (isClosed())
			return false;
		if (!isLoggingEnabled() /* && !ENABLE_JOURNALING */)
			return true;
		
		boolean logged = true;
		PreparedStatement pstmt = null;
		Connection conn = null;
		String cmd = "insert into " + getMTable()
				+ " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		long t0 = System.nanoTime(), t0Millis = System.currentTimeMillis(), t1 = t0;
		int i = 0;
		try {
			for (i = 0; i < packets.length; i++) {
				if (conn == null) {
					conn = this.getDefaultConn();
					conn.setAutoCommit(false);
					pstmt = conn.prepareStatement(cmd);
				}
				PaxosPacket packet = packets[i].lmTask.logMsg;
				// accept and decision use a faster implementation
				int[] sb = AbstractPaxosLogger.getSlotBallot(packet);

				pstmt.setString(1, packet.getPaxosID());
				pstmt.setInt(2, packet.getVersion());
				pstmt.setInt(3, sb[0]);
				pstmt.setInt(4, sb[1]);
				pstmt.setInt(5, sb[2]);
				pstmt.setInt(6, packet.getType().getInt());
				pstmt.setString(7, packets[i].logfile);
				pstmt.setLong(8, packets[i].logfileOffset);

				byte[] msgBytes = isJournalingEnabled() ? new byte[0]
						: deflate(toBytes(packet));

				if (getLogMessageBlobOption()) {
					pstmt.setInt(9, packets[i].length);// msgBytes.length);
					Blob blob = conn.createBlob();
					blob.setBytes(1, msgBytes);
					pstmt.setBlob(10, blob);
				} else {
					String packetString = packet.toString();
					pstmt.setInt(9, packetString.length());
					pstmt.setString(10, packetString);
				}

				pstmt.addBatch();
				if ((i + 1) % MAX_DB_BATCH_SIZE == 0 || (i + 1) == packets.length) {
					int[] executed = pstmt.executeBatch();
					conn.commit();
					pstmt.clearBatch();
					for (int j : executed)
						logged = logged && (j > 0);
					if (logged)
						log.log(Level.FINE, "{0}{1}{2}{3}{4}{5}", new Object[] {
								this,
								" successfully logged the " + "last ",
								(i + 1) % MAX_DB_BATCH_SIZE == 0 ? MAX_DB_BATCH_SIZE
										: (i + 1) % MAX_DB_BATCH_SIZE,
								" messages in ",
								(System.nanoTime() - t1) / 1000, " us" });
					t1 = System.nanoTime();
				}
			}

		} catch (Exception sqle) {
			/*
			 * If any exception happens, we must return false to preserve
			 * safety. We return true only if every message is logged
			 * successfully.
			 */
			sqle.printStackTrace();
			log.severe(this + " incurred " + sqle
					+ " while logging batch of size:" + packets.length
					+ "; packet_length = " + packets[i].toString().length());
			assert (packets[i].toString().length() < MAX_LOG_MESSAGE_SIZE);
			logged = false;
		} finally {
			cleanup(pstmt);
			cleanup(conn);
		}
		if(ENABLE_JOURNALING)
			DelayProfiler.updateDelayNano("index", t0, packets.length);
		else
			DelayProfiler.updateDelay("logBatchDB", t0Millis);
		//DelayProfiler.updateCount("#logged", packets.length);
		DelayProfiler.updateMovAvg("#potential_batched", packets.length);
		
		return logged;
	}

	/**
	 * Encoding used by the logger.
	 */
	public static final String CHARSET = "ISO-8859-1";

	private static final boolean DB_COMPRESSION = Config
			.getGlobalBoolean(PC.DB_COMPRESSION);

	/**
	 * @param data
	 * @return Compressed form.
	 * @throws IOException
	 */
	public static byte[] deflate(byte[] data) throws IOException {
		if (!DB_COMPRESSION)
			return data;
		byte[] compressed = null;
		double inflation = 1;
		int compressedLength = data.length;
		do {
			Deflater deflator = new Deflater();
			compressed = new byte[(int)((inflation *= 1.1)*data.length+16)];
			deflator.setInput(data);
			deflator.finish();
			compressedLength = deflator.deflate(compressed);
			deflator.end();
		} while (compressedLength == compressed.length);
		return Arrays.copyOf(compressed, compressedLength);
	}

	/**
	 * @param buf
	 * @return Uncompressed form.
	 * @throws IOException
	 */
	public static byte[] inflate(byte[] buf) throws IOException {
		if (!DB_COMPRESSION)
			return buf;
		Inflater inflator = new Inflater();
		inflator.setInput(buf);
		byte[] decompressed = new byte[buf.length];
		ByteArrayOutputStream baos = new ByteArrayOutputStream(buf.length);
		try {
			while (!inflator.finished()) {
				int count = inflator.inflate(decompressed);
				if (count == 0)
					break;
				baos.write(decompressed, 0, count);
			}
			baos.close();
			inflator.end();
		} catch (DataFormatException e) {
			PaxosManager.getLogger().severe(
					"DataFormatException while decompressing buffer of length "
							+ buf.length);
			e.printStackTrace();
			return buf;
		}
		return baos.toByteArray();
	}

	/*
	 * The entry point for checkpointing. Puts given checkpoint state for
	 * paxosID. 'state' could be anything that allows PaxosInterface to later
	 * restore the corresponding state. For example, 'state' could be the name
	 * of a file where the app maintains a checkpoint of all of its state. It
	 * could of course be the stringified form of the actual state if the state
	 * is at most MAX_STATE_SIZE.
	 */
	private void putCheckpointState(final String paxosID, final int version,
			final Set<String> group, final int slot, final Ballot ballot,
			final String state, final int acceptedGCSlot, final long createTime) {
		if (isClosed() || DISABLE_CHECKPOINTING)
			return;

		long t1 = System.currentTimeMillis();
		// stupid derby doesn't have an insert if not exist command
		String insertCmd = "insert into "
				+ getCTable()
				+ " (version,members,slot,ballotnum,coordinator,state,create_time, min_logfile, paxos_id) values (?,?,?,?,?,?,?,?,?)";
		String updateCmd = "update "
				+ getCTable()
				+ " set version=?,members=?, slot=?, ballotnum=?, coordinator=?, state=?, create_time=?, min_logfile=? where paxos_id=?";
		boolean existingCP = this.existsRecord(getCTable(), paxosID);
		// FIXME: concurrency issue; should explicitly process insert exception
		String cmd = existingCP ? updateCmd : insertCmd;
		PreparedStatement insertCP = null;
		Connection conn = null;
		String minLogfile = null;
		try {
			conn = this.getDefaultConn();
			insertCP = conn.prepareStatement(cmd);
			insertCP.setInt(1, version);
			insertCP.setString(2, Util.toJSONString(group));
			insertCP.setInt(3, slot);
			insertCP.setInt(4, ballot.ballotNumber);
			insertCP.setInt(5, ballot.coordinatorID);
			if (getCheckpointBlobOption()) {
				Blob blob = conn.createBlob();
				blob.setBytes(1, state.getBytes(CHARSET));
				insertCP.setBlob(6, blob);
			} else
				insertCP.setString(6, state);
			insertCP.setLong(7, createTime);
			insertCP.setString(8, minLogfile = this.getMinLogfile(paxosID));
			insertCP.setString(9, paxosID);
			insertCP.executeUpdate();
			// conn.commit();
			incrTotalCheckpoints();

			DelayProfiler.updateDelay("checkpoint", t1);
			// why can't insertCP.toString() return the query string? :/
			log.log(shouldLogCheckpoint() ? Level.INFO : Level.FINE,
					"{0} checkpointed ({1}:{2}, {3}{4}, {5}, ({6}, {7}) [{8}]) in {9} ms",
					new Object[] {
							this,
							paxosID,
							version,
							Util.toJSONString(group).substring(0, 0),
							slot,
							ballot,
							acceptedGCSlot,
							minLogfile,
							Util.truncate(state, TRUNCATED_STATE_SIZE,
									TRUNCATED_STATE_SIZE),
							(System.currentTimeMillis() - t1), });
		} catch (SQLException | UnsupportedEncodingException sqle) {
			log.log(Level.SEVERE,
					"{0} SQLException while checkpointing using command {1} with values "
							+ " {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9} "
							+ "; previous checkpoint state = {10}",
					new Object[] { this, cmd, version, group, slot,
							ballot.ballotNumber, ballot.coordinatorID, state,
							createTime, paxosID, existingCP });
			sqle.printStackTrace();
		} finally {
			cleanup(insertCP);
			cleanup(conn);
		}
		
		this.deleteOutdatedMessages(paxosID, version, ballot, acceptedGCSlot,
				ballot.ballotNumber, ballot.coordinatorID, acceptedGCSlot);

	}
		
	private void deleteOutdatedMessages(String paxosID, int version, Ballot ballot, int slot,
			int ballotnum, int coordinator, int acceptedGCSlot) {
		/*
		 * Delete logged messages from before the checkpoint. Note: Putting this
		 * before cleanup(conn) above can cause deadlock if we don't have at
		 * least 2x the number of connections as concurrently active paxosIDs.
		 * Realized this the hard way. :)
		 */
		if(ENABLE_JOURNALING && PAUSABLE_INDEX_JOURNAL) 
			this.messageLog.setGCSlot(paxosID, version, acceptedGCSlot);
		else if (Util.oneIn(getLogGCFrequency()) && this.incrNumGCs()==0) {
			Runnable gcTask = new TimerTask() {
				@Override
				public void run() {
					try {
						int priority = Thread.currentThread().getPriority();
						Thread.currentThread().setPriority(Thread.MIN_PRIORITY);
						long t = System.currentTimeMillis();
						SQLPaxosLogger.this.deleteOutdatedMessagesDB(paxosID,
								slot, ballot.ballotNumber,
								ballot.coordinatorID, acceptedGCSlot);
						Thread.currentThread().setPriority(priority);
						DelayProfiler.updateDelay("DBGC", t);
					} catch (Exception | Error e) {
						log.severe(this + " incurred exception "
								+ e.getMessage());
						e.printStackTrace();
					}
				}
			};
			if (getLogGCFrequency() == 0) {
				gcTask.run();
			}
			else {
				this.GC.submit(gcTask, 0);
			}
			assert (this.decrNumGCs() == 1);
		}		
	}
	
	private static int logGCFrequency = Config
			.getGlobalInt(PC.LOG_GC_FREQUENCY);

	private static int getLogGCFrequency() {
		return logGCFrequency;
	}

	private static void setLogGCFrequency(int f) {
		logGCFrequency = f;
	}

	private int numGCs = 0;
	private synchronized int incrNumGCs() {
		return this.numGCs++;
	}
	private synchronized int decrNumGCs() {
		return this.numGCs--;
	}

	public void putCheckpointState(CheckpointTask[] tasks) {
		this.putCheckpointState(tasks, true);
	}
	
	/**
	 * Batched version of putCheckpointState. This is a complicated method with
	 * very different behaviors for updates and inserts. If update is true, it
	 * attempts to batch-update all the checkpoints and for those
	 * updates/inserts that failed, it attempts to individually update/insert
	 * them through
	 * {@link #putCheckpointState(String, int, Set, int, Ballot, String, int)}.
	 * It is still possible that only a subset of the updates succeed, but that
	 * is okay as checkpoint failure is not fatal except in the case of initial
	 * checkpoint insertion.
	 * 
	 * If update is false, it means that this is a batch-insertion of initial
	 * checkpoints, and it is critical that this batch operation is atomic. If
	 * the batch operation only partly succeeds, it should throw an exception so
	 * that the caller can not proceed any further with the batch insertion but
	 * it should also rollback the changes.
	 * 
	 * The reason batched creation of initial checkpoints should be atomic is
	 * that otherwise, the checkpoints that did get written essentially are
	 * created paxos instances, but there is no easy way for the caller to know
	 * that they got created and this could lead to nasty surprises later. If
	 * the caller always follows up failed batch creations with sequential
	 * creation, then the rollback is not critical as the sequential creation
	 * will simply "recover" from the checkpoint if any left behind during a
	 * previous failed batch creation. If the caller chooses to keep re-issuing
	 * the batch creation and expects to eventually succeed (assuming that the
	 * instances in the batch didn't actually exist a priori), then rolling back
	 * failed batch creations like below will not help in the event of crashes.
	 * So, the caller should really just issue sequential creation requests if a
	 * batch creation fails or times out for any reason.
	 * 
	 * @param tasks
	 * @param update
	 */
	@Override
	public synchronized boolean putCheckpointState(CheckpointTask[] tasks, boolean update) {
		if (isClosed() || DISABLE_CHECKPOINTING)
			return false;

		boolean batchSuccess = true;
		boolean[] committed = new boolean[tasks.length];
		long t1 = System.currentTimeMillis();
		String insertCmd = "insert into "
				+ getCTable()
				+ " (version,members,slot,ballotnum,coordinator,state,create_time, min_logfile, paxos_id) values (?,?,?,?,?,?,?,?,?)";

		String updateCmd = "update "
				+ getCTable()
				+ " set version=?,members=?, slot=?, ballotnum=?, coordinator=?, state=?, create_time=?, min_logfile=? where paxos_id=?";
		String cmd = update ? updateCmd : insertCmd;
		PreparedStatement insertCP = null;
		Connection conn = null;
		String minLogfile = null;
		ArrayList<Integer> batch = new ArrayList<Integer>();
		try {
			for (int i = 0; i < tasks.length; i++) {
				CheckpointTask task = tasks[i];
				assert(task!=null);
				assert(update || task.slot==0);
				if ((task.slot == 0) == update) {
					this.putCheckpointState(task.paxosID, task.version,
							(task.members), task.slot, task.ballot, task.state,
							task.gcSlot, task.createTime);
					committed[i] = true;
					continue;
				}
				if (conn == null) {
					conn = this.getDefaultConn();
					conn.setAutoCommit(false);
					insertCP = conn.prepareStatement(cmd);
				}
				insertCP.setInt(1, task.version);
				insertCP.setString(2, Util.toJSONString(task.members));
				insertCP.setInt(3, task.slot);
				insertCP.setInt(4, task.ballot.ballotNumber);
				insertCP.setInt(5, task.ballot.coordinatorID);
				if (getCheckpointBlobOption()) {
					Blob blob = conn.createBlob();
					blob.setBytes(1, task.state.getBytes(CHARSET));
					insertCP.setBlob(6, blob);
				} else
					insertCP.setString(6, task.state);
				insertCP.setLong(7, task.createTime);
				insertCP.setString(8,
						minLogfile = this.getMinLogfile(task.paxosID));
				insertCP.setString(9, task.paxosID);
				insertCP.addBatch();
				batch.add(i);
				incrTotalCheckpoints();
				log.log(shouldLogCheckpoint() ? Level.INFO : Level.FINE,
						"{0} checkpointed> ({1}:{2}, {3}{4}, {5}, ({6}, {7}) [{8}]) {9}",
						new Object[] {
								this,
								task.paxosID,
								task.version,
								Util.toJSONString(task.members).substring(0, 0),
								task.slot,
								task.ballot,
								task.gcSlot,
								minLogfile,
								Util.truncate(task.state, TRUNCATED_STATE_SIZE,
										TRUNCATED_STATE_SIZE),
								(tasks.length > 1 ? "(batched=" + tasks.length
										+ ")" : "") });

				if ((i + 1) % MAX_DB_BATCH_SIZE == 0 || (i + 1) == tasks.length) {
					int[] executed = insertCP.executeBatch();
					conn.commit();
					insertCP.clearBatch();
					for (int j = 0; j < executed.length; j++)
						batchSuccess = batchSuccess
								&& (committed[batch.get(j)] = (executed[j] > 0));
					batch.clear();
				}
			}
			DelayProfiler.updateDelay("checkpoint", t1, tasks.length);
		} catch (SQLException | UnsupportedEncodingException sqle) {
			log.log(Level.SEVERE,
					"{0} SQLException while batched checkpointing",
					new Object[] { this });
			sqle.printStackTrace();
		} finally {
			cleanup(insertCP);
			cleanup(conn);
		}
		
		if (!batchSuccess) {
			if (update) {
				for (int i = 0; i < tasks.length; i++)
					if (!committed[i])
						this.putCheckpointState(tasks[i].paxosID,
								tasks[i].version, tasks[i].members,
								tasks[i].slot, tasks[i].ballot, tasks[i].state,
								tasks[i].gcSlot);
			} else {
				// rollback
				for (int i = 0; i < tasks.length; i++)
					if (committed[i])
						this.deleteCheckpoint(tasks[i].paxosID,
								tasks[i].version, tasks[i].members,
								tasks[i].slot, tasks[i].ballot, tasks[i].state,
								tasks[i].gcSlot);
				
				throw new PaxosInstanceCreationException(
						"Rolled back failed batch-creation of " + tasks.length
								+ " paxos instances");
			}
		}

		for (CheckpointTask task : tasks)
			this.deleteOutdatedMessages(task.paxosID, task.version,
					task.ballot, task.slot, task.ballot.ballotNumber,
					task.ballot.coordinatorID, task.gcSlot);
		return true;
	}

	private void deleteCheckpoint(String paxosID, int version,
			Set<String> members, int slot, Ballot ballot, String state,
			int gcSlot) {
		if (isClosed() || DISABLE_CHECKPOINTING)
			return;

		SlotBallotState sbs = this.getSlotBallotState(paxosID, version);
		if (!(sbs != null && sbs.slot == slot && sbs.members.equals(members)
				&& sbs.ballotnum == ballot.ballotNumber && sbs.state
					.equals(state))) {
			return;
		}
			
		PreparedStatement pstmt = null;
		Connection conn = null;
		try {
			conn = this.getDefaultConn();
			pstmt = conn.prepareStatement("delete from " + this.getCTable() + " where paxosID=?");
			pstmt.setString(1, paxosID);
			pstmt.execute();
		} catch(SQLException e) {
			log.severe(this + " unable to rollback failed batched-creation of " + paxosID);
			e.printStackTrace();
		}
	}

	private static int CHECKPOINT_LOG_THRESHOLD = 100000;
	private static int totalCheckpoints = 0;

	private synchronized static void incrTotalCheckpoints() {
		totalCheckpoints++;
	}

	private synchronized boolean shouldLogCheckpoint() {
		return totalCheckpoints < CHECKPOINT_LOG_THRESHOLD ? true : Util
				.oneIn(1000) ? true : false;
	}

	/*
	 * Forms the constraint field < limit while handling wraparounds. Looks like
	 * in SQL, we can not conveniently just check (field - limit < 0). SQL
	 * arithmetic stops at wraparound boundaries, e.g., Integer.MAX_VALUE + 1 is
	 * just Integer.MAX_VALUE.
	 */
	private static String getIntegerLTConstraint(String field, int limit) {
		return "("
				+ field
				+ " < "
				+ limit
				+ (limit < Integer.MIN_VALUE / 2 ? " or " + field + " > "
						+ Integer.MAX_VALUE / 2 : "") + ")";
	}

	// Forms the constraint field > limit while handling wraparounds
	private static String getIntegerGTEConstraint(String field, int limit) {
		return "("
				+ field
				+ " >= "
				+ limit
				+ (limit > Integer.MAX_VALUE / 2 ? " or " + field + " < "
						+ Integer.MIN_VALUE / 2 : "") + ")";
	}

	/*
	 * Called by putCheckpointState to delete logged messages from before the
	 * checkpoint.
	 */
	private void deleteOutdatedMessagesDB(String paxosID, int slot,
			int ballotnum, int coordinator, int acceptedGCSlot) {
		if (isClosed())
			return;
		if (slot == 0)
			return; // a hack to avoid GC at slot 0

		PreparedStatement pstmt = null;
		ResultSet rset = null;
		Connection conn = null;
		try {
			/*
			 * All accepts at or above the most recent checkpointed slot are
			 * retained. We retain the accept at the checkpoint slot to ensure
			 * that the accepted pvalues list is never empty unless there are
			 * truly no accepts beyond prepare.firstUndecidedSlot. If we don't
			 * ensure this property, we would have to maintain GC slot
			 * information in the database and send it along with prepare
			 * replies.
			 */
			int minLoggedAccept = (acceptedGCSlot - slot < 0 ? acceptedGCSlot + 1
					: slot);
			int minLoggedDecision = slot - MAX_OLD_DECISIONS;
			// The following are for handling integer wraparound arithmetic
			String decisionConstraint = getIntegerLTConstraint("slot",
					minLoggedDecision);
			String acceptConstraint = getIntegerLTConstraint("slot",
					minLoggedAccept);
			String ballotnumConstraint = getIntegerLTConstraint("ballotnum",
					ballotnum);

			String[] cmds = new String[3];
			// create delete command using the slot, ballot, and gcSlot
			cmds[0] = "delete from " + getMTable() + " where paxos_id='"
					+ paxosID + "' and " + "(packet_type="
					+ PaxosPacketType.ACCEPT.getInt() + " and "
					+ acceptConstraint + ")";
			cmds[1] = "delete from " + getMTable() + " where paxos_id='"
					+ paxosID + "' and " + "(packet_type="
					+ PaxosPacketType.DECISION.getInt() + " and "
					+ decisionConstraint + ") ";
			cmds[2] = "delete from " + getMTable() + " where paxos_id='"
					+ paxosID + "' and " + "(packet_type="
					+ PaxosPacketType.PREPARE.getInt() + " and ("
					+ ballotnumConstraint + " or (ballotnum=" + ballotnum
					+ " and coordinator<" + coordinator + ")))";

			conn = getDefaultConn();
			int deleted = 0;
			// have to literally break it down for derby :(
			for (int i = 0; i < 3; i++) {
				pstmt = conn.prepareStatement(cmds[i]);
				deleted += pstmt.executeUpdate();
				pstmt.close();
			}

			// conn.commit();
			log.log(Level.FINE, "{0}{1}{2}", new Object[] { this,
					" DB deleted up to slot ", acceptedGCSlot });
			if(deleted>0)
				//DelayProfiler.updateCount("#logged", -deleted)
				;
		} catch (SQLException sqle) {
			log.severe("SQLException while deleting outdated messages for "
					+ paxosID);
			sqle.printStackTrace();
		} finally {
			cleanup(pstmt, rset);
			cleanup(conn);
		}
	}

	/*
	 * Used to be the entry point for message logging. Replaced by batchLog and
	 * log(PaxosPacket) now.
	 */
	@Deprecated
	private boolean log(String paxosID, int version, int slot, int ballotnum,
			int coordinator, PaxosPacketType type, String message) {
		if (isClosed())
			return false;
		if (!isLoggingEnabled())
			return true;

		boolean logged = false;

		String cmd = "insert into " + getMTable()
				+ " values (?, ?, ?, ?, ?, ?, ?)";
		PreparedStatement localLogMsgStmt = null;
		Connection conn = null;
		try {
			conn = this.getDefaultConn();
			localLogMsgStmt = conn.prepareStatement(cmd); // no re-use option

			localLogMsgStmt.setString(1, paxosID);
			localLogMsgStmt.setInt(2, version);
			localLogMsgStmt.setInt(3, slot);
			localLogMsgStmt.setInt(4, ballotnum);
			localLogMsgStmt.setInt(5, coordinator);
			localLogMsgStmt.setInt(6, type.getInt());
			if (getLogMessageBlobOption()) {
				// localLogMsgStmt.setBlob(7, new StringReader(message));
				Blob blob = conn.createBlob();
				blob.setBytes(1, message.getBytes(CHARSET));
				localLogMsgStmt.setBlob(7, blob);
			} else
				localLogMsgStmt.setString(7, message);

			int rowcount = localLogMsgStmt.executeUpdate();
			assert (rowcount == 1);
			logged = true;
			log.log(Level.FINEST, "{0} inserted {1}, {2}, {3}, {4}, {5}",
					new Object[] { this, paxosID, slot, ballotnum, coordinator,
							message });
		} catch (SQLException sqle) {
			if (SQL.DUPLICATE_KEY.contains(sqle.getSQLState())) {
				log.log(Level.FINE, "{0} log message {1} previously logged",
						new Object[] { this, message });
				logged = true;
			} else {
				log.severe("SQLException while logging as " + cmd + " : "
						+ sqle);
				sqle.printStackTrace();
			}
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} finally {
			cleanup(localLogMsgStmt);
			cleanup(conn);
		} // no cleanup if statement is re-used
		return logged;
	}

	private synchronized Map<String, HotRestoreInfo> pauseBatchIndividually(Map<String, HotRestoreInfo> hriMap) {
		Map<String, HotRestoreInfo> paused = new HashMap<String,HotRestoreInfo>();
		for(HotRestoreInfo hri : hriMap.values()) {
			if(this.pause(hri.paxosID, hri.toString()))
				paused.put(hri.paxosID, hri);
		}
		return paused;
	}
	public synchronized Map<String, HotRestoreInfo> pause(Map<String, HotRestoreInfo> hriMap) {
		if (isClosed())
			return null;
		if(!USE_CHECKPOINTS_AS_PAUSE_TABLE) return pauseBatchIndividually(hriMap);

		String updateCmdNoLogIndex = "update " + (USE_CHECKPOINTS_AS_PAUSE_TABLE ? getCTable() : getPTable())
				+ " set serialized=?, has_serialized=true where  paxos_id=?";

		Map<String, HotRestoreInfo> paused = new HashMap<String, HotRestoreInfo>();
		HotRestoreInfo[] hris = hriMap.values().toArray(new HotRestoreInfo[0]);
		PreparedStatement pstmt = null;
		Connection conn = null;
		try {
			Map<String, HotRestoreInfo> batch = new HashMap<String, HotRestoreInfo>();
			for (int i = 0; i < hris.length; i++) {
				String paxosID = hris[i].paxosID;
				if (conn == null) {
					conn = this.getDefaultConn();
					conn.setAutoCommit(false);
					pstmt = conn.prepareStatement(updateCmdNoLogIndex);
				}
				pstmt.setString(1, hriMap.get(paxosID).toString());
				pstmt.setString(2, paxosID);

				pstmt.addBatch();
				batch.put(paxosID, hris[i]);
				if ((i + 1) % MAX_DB_BATCH_SIZE == 0 || (i + 1) == hriMap.size()) {
					pstmt.executeBatch();
					conn.commit();
					pstmt.clearBatch();
					paused.putAll(batch);

					log.log(Level.FINE,
							"{0} paused [{1}] ,[{2}]",
							new Object[] { this,
									Util.truncatedLog(batch.keySet(), 16) });
					batch.clear();
				}
			}
		} catch (SQLException e) {
			log.severe(this + " failed to pause batch "
					+ Util.truncatedLog(hriMap.keySet(), 10));
			e.printStackTrace();
		} finally {
			cleanup(pstmt);
			cleanup(conn);
		}
		paused.putAll(this.pauseBatchIndividually(this.diffHRI(hriMap, paused)));
		return paused;
	}
	
	private Map<String, HotRestoreInfo> diffHRI(
			Map<String, HotRestoreInfo> map1, Map<String, HotRestoreInfo> map2) {
		Map<String, HotRestoreInfo> diffEntries = new HashMap<String, HotRestoreInfo>();
		for (String key : map1.keySet())
			if (!map2.containsKey(key))
				diffEntries.put(key, map1.get(key));
		return diffEntries;
	}

	private Map<String, LogIndex> diffLI(Map<String, LogIndex> map1,
			Set<String> set2) {
		Map<String, LogIndex> diffEntries = new HashMap<String, LogIndex>();
		for (String key : map1.keySet())
			if (!set2.contains(key))
				diffEntries.put(key, map1.get(key));
		return diffEntries;
	}

	/*
	 * Can not start pause or unpause after close has been called. For other
	 * operations like checkpointing or logging, we need to be able to do them
	 * even after close has been called as waitToFinish needs that.
	 */
	@Override
	public synchronized boolean pause(String paxosID, String serializedState) {
		if (isClosed() /* || !isLoggingEnabled() */)
			return false;

		boolean paused = false;
		String insertCmd = "insert into " + (USE_CHECKPOINTS_AS_PAUSE_TABLE ? getCTable() : getPTable())
				+ " (serialized, has_serialized, logindex, paxos_id) values (?,true,?,?)";
		String insertCmdNoLogIndex = "insert into " + (USE_CHECKPOINTS_AS_PAUSE_TABLE ? getCTable() : getPTable())
				+ " (serialized, has_serialized, paxos_id) values (?,true,?)";

		String updateCmd = "update " + (USE_CHECKPOINTS_AS_PAUSE_TABLE ? getCTable() : getPTable())
				+ " set serialized=?, has_serialized=true, logindex=? where  paxos_id=?";
		String updateCmdNoLogIndex = "update " + (USE_CHECKPOINTS_AS_PAUSE_TABLE ? getCTable() : getPTable())
				+ " set serialized=?, has_serialized=true where  paxos_id=?";

		PreparedStatement pstmt = null;
		Connection conn = null;
		synchronized (this.messageLog) {
			try {
				LogIndex logIndex = this.messageLog.getLogIndex(paxosID);
				boolean pauseLogIndex = (logIndex != null);
				Blob blob = null;
				byte[] logIndexBytes = null;
				
				conn = this.getDefaultConn();
				// try update first; if exception, try insert
				pstmt = conn.prepareStatement(pauseLogIndex ? updateCmd : updateCmdNoLogIndex);
				pstmt.setString(1, serializedState);
				if(pauseLogIndex) {
					logIndexBytes = deflate(this.messageLog
							.getLogIndex(paxosID).toString().getBytes(CHARSET));
					blob = conn.createBlob();
					blob.setBytes(1, logIndexBytes);
					pstmt.setBlob(2, blob);
					assert (new String(inflate(logIndexBytes), CHARSET)
							.equals(this.messageLog.getLogIndex(paxosID)
									.toString()));
				}
				pstmt.setString(pauseLogIndex ? 3 : 2, paxosID);
				try {
					pstmt.executeUpdate();
				} catch (SQLException e) {
					pstmt.close();
					// try insert
					pstmt = conn.prepareStatement(pauseLogIndex ? insertCmd : insertCmdNoLogIndex);
					pstmt.setString(1, serializedState);
					if(pauseLogIndex) {
						blob = conn.createBlob();
						blob.setBytes(1, logIndexBytes);
						pstmt.setBlob(2, blob);
					}
					pstmt.setString(pauseLogIndex ? 3 : 2, paxosID);
					pstmt.executeUpdate();
				}
				log.log(Level.FINE, "{0} paused [{1}] ,[{2}]", new Object[] {
						this, serializedState, logIndex });
				paused = true;
			} catch (SQLException | IOException e) {
				log.severe(this + " failed to pause instance " + paxosID);
				this.deletePaused(paxosID);
				e.printStackTrace();
			} finally {
				cleanup(pstmt);
				cleanup(conn);
			}
			this.messageLog.uncache(paxosID);
		}
		return paused;
	}

	@Override
	public /*synchronized*/ HotRestoreInfo unpause(String paxosID) {
		if (isClosed() /* || !isLoggingEnabled() */)
			return null;

		HotRestoreInfo hri = null;
		PreparedStatement pstmt = null;
		ResultSet rset = null;
		Connection conn = null;
		String logIndexString = null;
		try {
			conn = this.getDefaultConn();
			pstmt = this.getPreparedStatement(conn, (USE_CHECKPOINTS_AS_PAUSE_TABLE ? getCTable() : getPTable()), paxosID,
					"serialized, logindex");
			rset = pstmt.executeQuery();
			while (rset.next()) {
				assert (hri == null); // exactly onece
				String serialized = rset.getString(1); // no clob option
				if (serialized != null)
					hri = new HotRestoreInfo(serialized);

				Blob logIndexBlob = rset.getBlob(2);
				logIndexString = lobToString(logIndexBlob);
				if (logIndexBlob != null)
					this.messageLog.restore(new LogIndex(new JSONArray(logIndexString)));
				
			}
		} catch (SQLException | JSONException | IOException e) {
			log.severe(this + " failed to unpause instance " + paxosID
					+ "; logIndex = " + logIndexString);
			e.printStackTrace();
		} finally {
			cleanup(pstmt, rset);
			cleanup(conn);
		}
		;
		if (hri != null)
			this.deletePaused(paxosID); // unpause will also delete paused state
		return hri;
	}

	private void deletePaused(String paxosID) {
		if (isClosed() /* || !isLoggingEnabled() */)
			return;
		PreparedStatement pstmt = null;
		ResultSet rset = null;
		Connection conn = null;
		try {
			conn = this.getDefaultConn();
			String cmd = "update "
					+ (USE_CHECKPOINTS_AS_PAUSE_TABLE ? getCTable()
							: getPTable())
					+ " set serialized=null"
					+ (USE_CHECKPOINTS_AS_PAUSE_TABLE ? ", has_serialized=false"
							: "") + " where paxos_id='" + paxosID + "'";
			pstmt = conn.prepareStatement(cmd);
			pstmt.executeUpdate();
			// conn.commit();
		} catch (SQLException sqle) {
			log.severe(this + " failed to delete paused state for " + paxosID);
			sqle.printStackTrace();
		} finally {
			cleanup(pstmt, rset);
			cleanup(conn);
		}
		;
	}

	private synchronized boolean pauseLogIndex(String paxosID, LogIndex logIndex) {
		if (isClosed() /* || !isLoggingEnabled() */)
			return false;
		boolean paused = false;
		// insert works because unpause always deletes on-disk copy
		String insertCmd = "insert into " + getPTable()
				+ " (null, false, logindex, paxos_id) values (?,?)";
		String updateCmd = "update " + (USE_CHECKPOINTS_AS_PAUSE_TABLE ? getCTable() : getPTable())
				+ " set logindex=? where  paxos_id=?";

		PreparedStatement pstmt = null;
		Connection conn = null;
		synchronized (this.messageLog) {
			try {
				conn = this.getDefaultConn();
				// try update first; if exception, try insert
				pstmt = conn.prepareStatement(updateCmd);

				byte[] logIndexBytes = logIndex!=null ? deflate(logIndex
						.toString().getBytes(CHARSET)) : null;
				Blob blob = conn.createBlob();
				blob.setBytes(1, logIndexBytes);
				pstmt.setBlob(1, blob);
				
				pstmt.setString(2, paxosID);
				try {
					pstmt.executeUpdate();
				} catch (SQLException e) {
					pstmt.close();
					// try insert
					pstmt = conn.prepareStatement(insertCmd);

					blob = conn.createBlob();
					blob.setBytes(1, logIndexBytes);
					pstmt.setBlob(1, blob);
					
					pstmt.setString(2, paxosID);
					pstmt.executeUpdate();
				}
				paused = true;
			} catch (SQLException | IOException sqle) {
				log.severe(this + " failed to pause logIndex for " + paxosID);
				sqle.printStackTrace();
			} finally {
				cleanup(pstmt);
				cleanup(conn);
			}
			// free up memory
			this.messageLog.uncache(paxosID);
		}
		return paused;
	}
	private synchronized Set<String> pauseLogIndexIndividually(Map<String,LogIndex> toCommit) {
		Set<String> paused = new HashSet<String>();
		for(Iterator<String> strIter = toCommit.keySet().iterator(); strIter.hasNext(); ) {
			String paxosID = strIter.next();
			LogIndex logIndex = toCommit.get(paxosID);
			if(this.pauseLogIndex(paxosID, logIndex)) paused.add(paxosID);
		}
		return paused;
	}

	private synchronized Set<String> pauseLogIndex(
			Map<String, LogIndex> toCommit) {
		if (isClosed())
			return null;
		if (!USE_CHECKPOINTS_AS_PAUSE_TABLE)
			return this.pauseLogIndexIndividually(toCommit);
		String updateCmd = "update "
				+ (USE_CHECKPOINTS_AS_PAUSE_TABLE ? getCTable() : getPTable())
				+ " set logindex=? where  paxos_id=?";

		PreparedStatement pstmt = null;
		Connection conn = null;
		Set<String> paused = new HashSet<String>();
		Set<String> batch = new HashSet<String>();
		synchronized (this.messageLog) {
			try {
				int i = 0;
				for (String paxosID : toCommit.keySet()) {
					LogIndex logIndex = toCommit.get(paxosID);
					if (conn == null) {
						conn = this.getDefaultConn();
						conn.setAutoCommit(false);
						pstmt = conn.prepareStatement(updateCmd);
					}

					byte[] logIndexBytes = logIndex != null ? deflate(logIndex
							.toString().getBytes(CHARSET)) : null;
					if (logIndexBytes != null && Util.oneIn(Integer.MAX_VALUE))
						DelayProfiler.updateMovAvg(
								"logindex_size",
								logIndexBytes.length);
					Blob blob = conn.createBlob();
					if (logIndexBytes != null)
						blob.setBytes(1, logIndexBytes);
					pstmt.setBlob(1, logIndexBytes != null ? blob : null);
					pstmt.setString(2, paxosID);
					pstmt.addBatch();
					batch.add(paxosID);
					if ((i + 1) % MAX_DB_BATCH_SIZE == 0
							|| (i + 1) == toCommit.size()) {
						pstmt.executeBatch();
						conn.commit();
						pstmt.clearBatch();
						paused.addAll(batch);

						log.log(Level.FINE,
								"{0} paused batch {1}",
								new Object[] { this,
										Util.truncatedLog(batch, 16) });
						batch.clear();
					}
					i++;
				}
			} catch (SQLException | IOException sqle) {
				log.severe(this + " failed to pause logIndex batch");
				sqle.printStackTrace();
			} finally {
				cleanup(pstmt);
				cleanup(conn);
			}
			// free up memory
			for (String paxosID : paused)
				this.messageLog.uncache(paxosID);
		}
		if (paused.size() != toCommit.size())
			paused.addAll(this
					.pauseLogIndexIndividually(diffLI(toCommit, paused)));
		return paused;
	}

	private synchronized LogIndex unpauseLogIndex(String paxosID) {
		if (isClosed() /* || !isLoggingEnabled() */)
			return null;

		log.fine(this + " trying to unpause logIndex for " + paxosID);
		PreparedStatement pstmt = null;
		ResultSet rset = null;
		Connection conn = null;
		LogIndex logIndex = null;
		String logIndexString = null;
		try {
			conn = this.getDefaultConn();
			pstmt = this.getPreparedStatement(conn, (USE_CHECKPOINTS_AS_PAUSE_TABLE ? getCTable() : getPTable()), paxosID,
					"logindex");
			rset = pstmt.executeQuery();
			while (rset.next()) {
				Blob logIndexBlob = rset.getBlob(1);
				if (logIndexBlob == null)
					continue;
				logIndexString = (lobToString(logIndexBlob));
				logIndex = new LogIndex(new JSONArray(logIndexString));
				this.messageLog.restore(logIndex);
				log.log(Level.FINE, "{0} unpaused logIndex for {1}", new Object[]{this, paxosID});
			}
		} catch (SQLException | JSONException | IOException e) {
			log.severe(this + " failed to unpause instance " + paxosID + "; logIndex = " + logIndexString);
			e.printStackTrace();
		} finally {
			cleanup(pstmt, rset);
			cleanup(conn);
		}
		return logIndex;
	}

	/**
	 * Gets current checkpoint. There can be only one checkpoint for a paxosID
	 * at any time.
	 */
	@Override
	public String getCheckpointState(String paxosID) {
		return this.getCheckpointState(paxosID, "state");
	}

	private String getCheckpointState(String paxosID, String column) {
		return this.getCheckpointState(getCTable(), paxosID, column);
	}

	private String getCheckpointState(String table, String paxosID,
			String column) {
		if (isClosed())
			return null;

		String state = null;
		PreparedStatement pstmt = null;
		ResultSet stateRS = null;
		Connection conn = null;
		try {
			conn = getDefaultConn();
			pstmt = getPreparedStatement(conn, table, paxosID, column);
			stateRS = pstmt.executeQuery();
			while (stateRS.next()) {
				assert (state == null); // single result
				state = (!getCheckpointBlobOption() || !column.equals("state") ? stateRS
						.getString(1) : lobToString(stateRS.getBlob(1)));
			}
		} catch (IOException e) {
			log.severe("IOException while getting state " + " : " + e);
			e.printStackTrace();
		} catch (SQLException sqle) {
			log.severe("SQLException while getting state: " + table + " "
					+ paxosID + " " + column + " : " + sqle);
			sqle.printStackTrace();
		} finally {
			cleanup(pstmt, stateRS);
			cleanup(conn);
		}

		return state;
	}

	private boolean existsRecord(String table, String paxosID) {
		boolean exists = false;
		PreparedStatement pstmt = null;
		ResultSet stateRS = null;
		Connection conn = null;
		try {
			conn = getDefaultConn();
			pstmt = getPreparedStatement(conn, table, paxosID, "paxos_id");
			stateRS = pstmt.executeQuery();
			while (stateRS.next()) {
				exists = true;
			}
		} catch (SQLException sqle) {
			log.severe("SQLException while getting state: " + table + " "
					+ paxosID + " : " + sqle);
			sqle.printStackTrace();
		} finally {
			cleanup(pstmt, stateRS);
			cleanup(conn);
		}

		return exists;
	}

	protected static String clobToString(Clob clob) throws SQLException,
			IOException {
		if (clob == null)
			return null;
		StringBuilder sb = new StringBuilder();
		BufferedReader br = new BufferedReader(clob.getCharacterStream());
		while (true) {
			String s = br.readLine();
			if (s == null)
				break;
			sb.append(s);
		}
		return sb.toString();
	}

	private static String lobToString(Blob blob) throws SQLException,
			IOException {
		if (blob == null)
			return null;
		byte[] blobBytes = blob.getBytes(1L, (int) blob.length());
		return new String(inflate(blobBytes), CHARSET);
	}

	/**
	 * Methods to get slot, ballotnum, coordinator, state, and version of
	 * checkpoint
	 * 
	 * @param table
	 * 
	 * @param paxosID
	 * @param version
	 * @param matchVersion
	 * @return Returns SlotBallotState object retrieved for
	 *         {@code paxosID:version}.
	 */
	public SlotBallotState getSlotBallotState(String table, String paxosID,
			int version, boolean matchVersion) {
		if (isClosed())
			return null;

		SlotBallotState sb = null;
		ResultSet stateRS = null;
		PreparedStatement cpStmt = null;
		Connection conn = null;
		boolean versionMismatch = false;
		try {
			conn = this.getDefaultConn();
			assert (conn != null);

			cpStmt = this
					.getPreparedStatement(conn, table, paxosID,
							"slot, ballotnum, coordinator, state, version, create_time, members");

			cpStmt.setString(1, paxosID);
			stateRS = cpStmt.executeQuery();
			while (stateRS.next()) {
				assert (sb == null); // single result
				versionMismatch = (matchVersion && version != stateRS.getInt(5));
				if (versionMismatch)
					log.log(Level.INFO,
							"{0} asked for {1}:{2} but got version {3}",
							new Object[] { this, paxosID, version,
									stateRS.getInt(5) });

				assert (table.equals(getCTable()) || table.equals(getPCTable()));
				if (!versionMismatch)
					sb = new SlotBallotState(stateRS.getInt(1),
							stateRS.getInt(2), stateRS.getInt(3),
							(!getCheckpointBlobOption() ? stateRS.getString(4)
									: lobToString(stateRS.getBlob(4))),
							stateRS.getInt(5), stateRS.getLong(6),
							Util.stringToStringSet(stateRS.getString(7)));
			}
		} catch (SQLException | IOException | JSONException e) {
			log.severe(e.getClass().getSimpleName() + " while getting slot "
					+ " : " + e);
			e.printStackTrace();
		} finally {
			cleanup(stateRS);
			cleanup(cpStmt);
			cleanup(conn);
		}
		return versionMismatch ? null : sb;
	}

	/**
	 * @param paxosID
	 * @param version
	 * @param matchVersion
	 * @return A {@link SlotBallotState} structure containing those three fields
	 *         and a couple more.
	 */
	public SlotBallotState getSlotBallotState(String paxosID, int version,
			boolean matchVersion) {
		// default is checkpoint table
		return this.getSlotBallotState(getCTable(), paxosID, version,
				matchVersion);
	}

	public SlotBallotState getSlotBallotState(String paxosID) {
		return this.getSlotBallotState(paxosID, 0, false);
	}

	@Override
	public SlotBallotState getSlotBallotState(String paxosID, int version) {
		return this.getSlotBallotState(paxosID, version, true);
	}

	public int getCheckpointSlot(String paxosID) {
		SlotBallotState sb = getSlotBallotState(paxosID);
		return (sb != null ? sb.slot : -1);
	}

	public Ballot getCheckpointBallot(String paxosID) {
		SlotBallotState sb = getSlotBallotState(paxosID);
		return (sb != null ? new Ballot(sb.ballotnum, sb.coordinator) : null);
	}

	public StatePacket getStatePacket(String paxosID) {
		SlotBallotState sbs = this.getSlotBallotState(paxosID);
		StatePacket statePacket = null;
		if (sbs != null)
			statePacket = new StatePacket(new Ballot(sbs.ballotnum,
					sbs.coordinator), sbs.slot, sbs.state);
		return statePacket;
	}

	public RecoveryInfo getRecoveryInfo(String paxosID) {
		if (isClosed())
			return null;

		RecoveryInfo pri = null;
		PreparedStatement pstmt = null;
		ResultSet stateRS = null;
		Connection conn = null;

		try {
			conn = this.getDefaultConn();
			pstmt = this.getPreparedStatement(conn, getCTable(), paxosID,
					"version, members");
			stateRS = pstmt.executeQuery();
			while (stateRS != null && stateRS.next()) {
				int version = stateRS.getInt(1);
				String members = stateRS.getString(2);
				String[] pieces = Util.jsonToStringArray(members);
				pri = new RecoveryInfo(paxosID, version, pieces);
			}
		} catch (SQLException | JSONException e) {
			log.severe((e instanceof SQLException ? "SQL" : "JSON")
					+ "Exception while getting all paxos IDs " + " : " + e);
		} finally {
			cleanup(pstmt, stateRS);
			cleanup(conn);
		}
		return pri;
	}

	/************* Start of incremental checkpoint read methods **********************/
	public synchronized boolean initiateReadCheckpoints(boolean readState) {
		if (isClosed() || this.cursorPstmt != null || this.cursorRset != null
				|| this.cursorConn != null)
			return false;

		log.log(Level.FINE, "{0}{1}", new Object[] { this,
				" initiatedReadCheckpoints" });
		boolean initiated = false;
		try {
			this.cursorPstmt = this.getPreparedStatement(this.getCursorConn(),
					getCTable(), null, "paxos_id, version, members"
							+ (readState ? ", state" : ""));
			this.cursorRset = this.cursorPstmt.executeQuery();
			initiated = true;
		} catch (SQLException sqle) {
			log.severe("SQLException while getting all paxos IDs " + " : "
					+ sqle);
		}
		return initiated;
	}

	public synchronized RecoveryInfo readNextCheckpoint(boolean readState) {
		RecoveryInfo pri = null;
		try {
			if (cursorRset != null && cursorRset.next()) {
				String paxosID = cursorRset.getString(1);
				int version = cursorRset.getInt(2);
				String members = cursorRset.getString(3);
				String[] pieces = Util.jsonToStringArray(members);
				String state = (readState ? (!getCheckpointBlobOption() ? cursorRset
						.getString(4) : lobToString(cursorRset.getBlob(4)))
						: null);
				pri = new RecoveryInfo(paxosID, version, pieces, state);
				/*
				 * Whenever a checkpoint is found, we must try to restore the
				 * corresponding logIndex so that we maintain the invariant that
				 * logIndex is always unpaused when the corresponding paxos
				 * instance is unpaused.
				 */
				this.unpauseLogIndex(paxosID);
			}
		} catch (SQLException | JSONException | IOException e) {
			log.severe(e.getClass().getSimpleName()
					+ " in readNextCheckpoint: " + " : " + e);
		}
		return pri;
	}

	private File[] logfiles = null;
	private int logfileIndex = 0;
	RandomAccessFile curRAF = null;

	public synchronized boolean initiateReadMessages() {
		if (isClosed() || this.cursorPstmt != null || this.cursorRset != null
				|| this.cursorConn != null)
			return false;

		log.log(Level.FINE, "{0} invoked initiatedReadMessages()",
				new Object[] { this, });
		boolean initiated = false;
		if (!isJournalingEnabled())
			try {
				this.cursorPstmt = this.getPreparedStatement(
						this.getCursorConn(), getMTable(), null, "message");
				this.cursorRset = this.cursorPstmt.executeQuery();
				initiated = true;
			} catch (SQLException sqle) {
				log.severe("SQLException while getting all paxos IDs " + " : "
						+ sqle);
			}
		else if (isJournalingEnabled()) {
			logfiles = ((logfiles = this.getJournalFiles()) != null ?
			// important to sort to replay in order
			toFiles(getLatest(logfiles, logfiles.length).toArray(
					new Filename[0]))
					: new File[0]);

			if (logfiles.length > 0)
				try {
					int i = 0;
					for (File minLogfile = this.getMinLogfile(); minLogfile != null
							&& i < logfiles.length; i++)
						if (logfiles[i].toString()
								.equals(minLogfile.toString()))
							break;
					if (i == logfiles.length)
						i = 0; // not found
					
					log.info(this
							+ " rolling forward logged messages from logfile "
							+ logfiles[i] + " onwards");

					this.logfileIndex = i;
					curRAF = new RandomAccessFile(logfiles[i], "r");
					log.log(Level.FINEST,
							"{0} rolling forward logged messages from file {1}",
							new Object[] { this.journaler,
									this.logfiles[this.logfileIndex] });
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
		}
		return initiated;
	}

	private ArrayList<String> getUnpausedBeforeRecovery() {
		if (isClosed())
			return null;

		PreparedStatement pstmt = null;
		ResultSet rset = null;
		Connection conn = null;

		ArrayList<String> unpaused = new ArrayList<String>();
		try {
			conn = this.getDefaultConn();
			pstmt = conn
					.prepareStatement("select paxos_id from "
							+ getCTable()
							+ " where "
							+ (USE_CHECKPOINTS_AS_PAUSE_TABLE ? " has_serialized=false"
									: " paxos_id NOT in (select paxos_id from "
											+ getPTable() + ")"));
			rset = pstmt.executeQuery();
			while (rset != null && rset.next()) {
				unpaused.add(rset.getString(1));
			}
		} catch (SQLException e) {
			log.severe("SQLException while getting all paxos IDs " + " : " + e);
		} finally {
			cleanup(pstmt, rset);
			cleanup(conn);
		}
		return unpaused;
	}

	// for garbage collection frontier
	private String getMinLogfile(String paxosID) {
		String minLogfile = this.messageLog.getMinLogfile(paxosID);
		if (minLogfile == null)
			minLogfile = this.journaler.curLogfile;
		return minLogfile;
	}

	// for roll forward point
	private File getMinLogfile() {
		if (isClosed())
			return null;
		Filename minLogfilename = null;
		ArrayList<String> unpaused = this.getUnpausedBeforeRecovery();
		log.info(this + " found " + unpaused.size()
				+ " instances unpaused before last shutdown");
		for (String paxosID : unpaused) {
			String logfile = this.messageLog.getMinLogfile(paxosID);
			// logfile==null iff there were no log messages ever for the paxosID
			if (logfile == null)
				continue;
			Filename curFilename = new Filename(new File(logfile));
			if (minLogfilename == null)
				minLogfilename = curFilename;
			else if (curFilename.compareTo(minLogfilename) < 0)
				minLogfilename = curFilename;
		}
		return minLogfilename != null ? minLogfilename.file : null;
	}

	// private PendingLogTask prevRolledMsg = null;

	/*
	 * This method used to return PaxosPacket earlier. We now return a string
	 * because it may be necessary to fixNodeStringToInt on the corresponding
	 * json before conversion to PaxosPacket.
	 */
	@Override
	public synchronized String readNextMessage() {
		String packetStr = null;
		if (!isJournalingEnabled())
			try {
				if (cursorRset != null && cursorRset.next()) {
					String msg = (!getLogMessageBlobOption() ? cursorRset
							.getString(1) : lobToString(cursorRset.getBlob(1)));
					packetStr = msg;
				}
			} catch (SQLException | IOException e) {
				log.severe(this + " got " + e.getClass().getSimpleName()
						+ " in readNextMessage while reading: " + " : "
						+ packetStr);
				e.printStackTrace();
			}
		else if (isJournalingEnabled()) {
			String latest = this.getLatestJournalFile();
			try {
				while (this.curRAF != null
						&& this.curRAF.getFilePointer() == this.curRAF.length()) {
					this.curRAF.close();
					this.curRAF = null;
					// move on to the next file
					if (this.logfileIndex + 1 < this.logfiles.length)
						this.curRAF = new RandomAccessFile(
								this.logfiles[++this.logfileIndex], "r");
					if (this.curRAF != null)
						log.log(Level.INFO,
								"{0} rolling forward logged messages from file {1}",
								new Object[] { this.journaler,
										this.logfiles[this.logfileIndex] });
				}
				if (this.curRAF == null)
					return null;

				log.log(Level.FINEST,
						"{0} reading from offset {1} from file {2}",
						new Object[] { this, this.curRAF.getFilePointer(),
								this.logfiles[this.logfileIndex] });

				long msgOffset = this.curRAF.getFilePointer();
				int msgLength = this.curRAF.readInt();
				byte[] msg = new byte[msgLength];
				this.curRAF.readFully(msg);
				packetStr = new String(msg, CHARSET);

				PaxosPacket pp = this.getPacketizer() != null ? this
						.getPacketizer().stringToPaxosPacket(packetStr)
						: PaxosPacket.getPaxosPacket(packetStr);

				// also index latest log file
				if (DB_INDEX_JOURNAL
						&& latest != null
						&& this.logfiles[this.logfileIndex].toString().equals(
								latest))
					this.indexJournalEntryInDB(pp,
							this.logfiles[this.logfileIndex].toString(),
							msgOffset, msgLength);

				if (this.messageLog.getLogIndex(pp.getPaxosID()) == null)
					this.unpauseLogIndex(pp.getPaxosID());

				// feed into in-memory log
				this.messageLog.add(pp,
						this.logfiles[this.logfileIndex].toString(), msgOffset,
						msgLength);

			} catch (IOException | JSONException e) {
				e.printStackTrace();
			}
		}
		return packetStr;
	}

	public synchronized void closeReadAll() {
		log.log(Level.FINE, "{0}{1}", new Object[] { this,
				" invoking closeReadAll" });
		this.cleanupCursorConn();
	}

	private void indexJournalEntryInDB(PaxosPacket pp, String journalFile,
			long offset, int length) throws JSONException {
		if (pp == null)
			return;
		PendingLogTask[] pending = { new PendingLogTask(
				new LogMessagingTask(pp), journalFile, offset, length) };
		this.logBatchDB(pending);
	}

	/************* End of incremental checkpoint read methods **********************/

	/**
	 * Convenience method invoked by a number of other methods. Should be called
	 * only from a self-synchronized method.
	 * 
	 * @param table
	 * @param paxosID
	 * @param column
	 * @return PreparedStatement to lookup the specified table, paxosID and
	 *         column(s)
	 * @throws SQLException
	 */
	private PreparedStatement getPreparedStatement(Connection conn,
			String table, String paxosID, String column, String fieldConstraints)
			throws SQLException {
		String cmd = "select " + column + " from " + table
				+ (paxosID != null ? " where paxos_id=?" : "");
		cmd += (fieldConstraints != null ? fieldConstraints : "");
		PreparedStatement getCPState = (conn != null ? conn : this
				.getDefaultConn()).prepareStatement(cmd);
		if (paxosID != null)
			getCPState.setString(1, paxosID);
		return getCPState;
	}

	private PreparedStatement getPreparedStatement(Connection conn,
			String table, String paxosID, String column) throws SQLException {
		return this.getPreparedStatement(conn, table, paxosID, column, "");
	}

	/**
	 * Logs the given packet. The packet must have a paxosID in it already for
	 * this method to be useful.
	 * 
	 * @param packet
	 */
	public boolean log(PaxosPacket packet) {
		int[] slotballot = AbstractPaxosLogger.getSlotBallot(packet);
		assert (slotballot.length == 3);
		return log(packet.getPaxosID(), packet.getVersion(), slotballot[0],
				slotballot[1], slotballot[2], packet.getType(),
				packet.toString());
	}

	static class PendingLogTask {
		final LogMessagingTask lmTask;
		final String logfile;
		final long logfileOffset;
		final int length;

		PendingLogTask(LogMessagingTask lmTask, String logfile, long offset,
				int length) {
			this.lmTask = lmTask;
			this.logfile = logfile;
			this.logfileOffset = offset;
			this.length = length;
		}
	}

	private void syncLogMessagesIndexBackground() {
		//DelayProfiler.updateMovAvg("#bgsync", this.pendingLogMessages.size());
		this.GC.submit(new TimerTask() {
			@Override
			public void run() {
				try {
					SQLPaxosLogger.this.syncLogMessagesIndex();
				} catch (Exception |Error e) {
					log.severe(this + " incurred exception " + e.getMessage());
					e.printStackTrace();
				}
			}
		}, 0);
	}

	private boolean syncLogMessagesIndex() {
		return this.syncLogMessagesIndex(null);
	}

	private synchronized boolean syncLogMessagesIndex(String paxosID) {
		if (!DB_INDEX_JOURNAL) {
			this.pendingLogMessages.clear();
			return true;
		}
		int prevSize = this.pendingLogMessages.size();
		if (prevSize == 0)
			return true;

		ArrayList<PendingLogTask> pendingQ = new ArrayList<PendingLogTask>();
		for (Iterator<PendingLogTask> lmIter = this.pendingLogMessages
				.iterator(); lmIter.hasNext();) {
			PendingLogTask pending = lmIter.next();
			if (!pending.lmTask.isEmpty()
					&& (paxosID == null || pending.lmTask.logMsg.getPaxosID()
							.equals(paxosID)))
				pendingQ.add(pending);
			lmIter.remove();
		}
		log.log(Level.FINE,
				"{0} trimmed pending log message queue from {1} to {2}",
				new Object[] { this, prevSize, this.pendingLogMessages.size() });
		return this.logBatchDB(pendingQ.toArray(new PendingLogTask[0]));
	}

	private String getLatestJournalFile() {
		File[] journalFiles = this.getJournalFiles();
		Set<Filename> latest = getLatest(journalFiles, 1);
		assert (latest.size() <= 1) : latest.size();
		return latest.size() == 1 ? latest.toArray(new Filename[0])[0].file
				.toString() : null;
	}

	/**
	 * Gets the list of logged messages for the paxosID. The static method
	 * PaxosLogger.rollForward(.) can be directly invoked to replay these
	 * messages without explicitly invoking this method.
	 * 
	 * @param paxosID
	 * @param fieldConstraints
	 * @return A list of logged messages for {@code paxosID} meeting
	 *         {@code fieldConstraints}.
	 */
	private synchronized ArrayList<PaxosPacket> getLoggedMessages(
			String paxosID, String fieldConstraints) {
		long t = System.currentTimeMillis();
		if (ENABLE_JOURNALING && LOG_INDEX_FREQUENCY > 0 )
			this.syncLogMessagesIndex(paxosID);

		ArrayList<PaxosPacket> messages = new ArrayList<PaxosPacket>();
		if (DISABLE_GET_LOGGED_MESSAGES)
			return messages;

		PreparedStatement pstmt = null;
		ResultSet messagesRS = null;
		Connection conn = null;
		try {
			conn = this.getDefaultConn();
			pstmt = this.getPreparedStatement(conn, getMTable(), paxosID,
					"packet_type, message"
							+ (ENABLE_JOURNALING ? ", logfile, foffset, length"
									: ""), fieldConstraints);
			messagesRS = pstmt.executeQuery();

			assert (!messagesRS.isClosed());
			while (messagesRS.next()) {
				assert (!ENABLE_JOURNALING || messagesRS.getString("logfile") != null);

				String logMsgStr = null;
				try {
					logMsgStr = (!ENABLE_JOURNALING ? (!getLogMessageBlobOption() ? messagesRS
							.getString("message") : lobToString(messagesRS
							.getBlob("message")))

							: this.getJournaledMessage(
									messagesRS.getString("logfile"),
									messagesRS.getLong("foffset"),
									messagesRS.getInt("length"), null));
				} catch (SQLException | IOException e) {
					/*
					 * It is possible that a journal file gets garbage collected
					 * while getJournaledMessage is trying to get logged
					 * messages from it, so IOExceptions here are not fatal.
					 */
					log.severe(this + ":" + e.getClass().getSimpleName()
							+ " while getting logged messages for " + paxosID
							+ ":" + messagesRS.getString("packet_type") + ":"
							+ messagesRS.getString("logfile") + ":"
							+ messagesRS.getLong("foffset") + ":"
							+ messagesRS.getInt("length"));
					e.printStackTrace();
				}
				if (logMsgStr == null)
					continue;

				PaxosPacket packet = this.getPacketizer() != null ? getPacketizer()
						.stringToPaxosPacket(logMsgStr) : PaxosPacket
						.getPaxosPacket(logMsgStr);
				if (packet == null) {
					log.severe(this + " retrieved null packet from logMsgStr");
					continue;
				}
				assert (packet == null || !(packet instanceof AcceptPacket) || ((AcceptPacket) packet)
						.hasRequestValue()) : packet;
				// sanity check for DB-journal consistency
				assert (packet == null || packet.getType().getInt() == messagesRS
						.getInt("packet_type"));
				messages.add(packet);
			}
		} catch (SQLException | JSONException e) {
			log.severe(e.getClass().getSimpleName()
					+ " while getting slot for " + paxosID);
			e.printStackTrace();
		} finally {
			cleanup(pstmt, messagesRS);
			cleanup(conn);
		}
		if (Util.oneIn(Integer.MAX_VALUE))
			DelayProfiler.updateDelay("getLoggedMessages", t);
		return messages;
	}

	private String getJournaledMessage(String logfile, long offset, int length,
			RandomAccessFile raf) throws IOException {
		assert (logfile != null);
		if (!new File(logfile).exists())
			return null;
		boolean locallyOpened = false;
		if (raf == null) {
			locallyOpened = true;
			raf = new RandomAccessFile(logfile, "r");
		}
		boolean error = false;
		String msg = null;
		try {
			raf.seek(offset);
			assert (raf.length() > offset) : this + " " + raf.length() + " <= " + offset
					+ " while reading logfile " + logfile;
			int readLength = raf.readInt();
			try {
				assert (readLength == length) : this + " : " + readLength
						+ " != " + length;
			} catch (Error e) {
				error = true;
				log.severe(this + ": " + e);
				e.printStackTrace();
			}
			int bufLength = length;
			byte[] buf = new byte[bufLength];
			raf.readFully(buf);
			if (JOURNAL_COMPRESSION)
				buf = inflate(buf);
			msg = new String(buf, CHARSET);
		} catch (IOException | Error e) {
			log.log(Level.INFO,
					"{0} incurred IOException while retrieving journaled message {1}:{2}",
					new Object[] { this, logfile, offset +":"+length });
			e.printStackTrace();
			if (locallyOpened)
				raf.close();
			throw e;
		}
		log.log(error ? Level.INFO : Level.FINEST,
				"{0} returning journaled message from {1}:{2} = [{3}]",
				new Object[] { this, logfile, offset+":"+length, msg });
		return msg;
	}
	
	private static class FileOffsetLength {
		final String file;
		final long offset;
		final int length;
		FileOffsetLength(String file, long offset, int length) {
			this.file = file;
			this.offset = offset;
			this.length = length;
		}
	}

	private String[] getJournaledMessage(FileOffsetLength[] fols)
			throws IOException {
		ArrayList<String> logStrings = new ArrayList<String>();
		RandomAccessFile raf = null;
		String filename = null;
		for (FileOffsetLength fol : fols) {
			try {
				if (raf == null) {
					raf = new RandomAccessFile(filename = fol.file, "r");
				} else if (!filename.equals(fol.file)) {
					raf.close();
					raf = new RandomAccessFile(filename = fol.file, "r");
				}
				logStrings.add(this.getJournaledMessage(fol.file, fol.offset,
						fol.length, raf));
			} catch(IOException e) {
				if (raf != null)
					raf.close();
				raf = null;
			}
		}
		return logStrings.toArray(new String[0]);
	}

	private static final long LOGFILE_AGE_THRESHOLD = Config
			.getGlobalLong(PC.LOGFILE_AGE_THRESHOLD);

	private void garbageCollectJournal(TreeSet<Filename> candidates) {
		// long t = System.currentTimeMillis();
		// first get file list, then live list
		if (SQLPaxosLogger.this.journaler.numOngoingGCs++ > 0)
			log.severe(this + " has "
					+ SQLPaxosLogger.this.journaler.numOngoingGCs
					+ " ongoing log GC tasks");
		this.deleteJournalFiles(
				candidates,
				DB_INDEX_JOURNAL ? this.getActiveLogfiles() : this
						.getActiveLogfilesFromCheckpointTable(candidates));
		if (!candidates.isEmpty()
				&& System.currentTimeMillis()
						- candidates.iterator().next().file.lastModified() > LOGFILE_AGE_THRESHOLD*1000
				&& Util.oneIn(COMPACTION_FREQUENCY))
			this.compactLogfiles();
		--SQLPaxosLogger.this.journaler.numOngoingGCs;
		// DelayProfiler.updateDelay("logGC", t);
	}

	private synchronized ArrayList<String> getActiveLogfiles() {
		ArrayList<String> pending = this.getPendingLogfiles();
		pending.addAll(this.getIndexedLogfiles(getMTable()));
		return pending;
	}

	private synchronized ArrayList<String> getPendingLogfiles() {
		PendingLogTask[] pending = this.pendingLogMessages
				.toArray(new PendingLogTask[0]);
		ArrayList<String> filenames = new ArrayList<String>();
		for (PendingLogTask p : pending)
			filenames.add(p.logfile);
		return filenames;
	}

	private synchronized ArrayList<String> getIndexedLogfiles(String table) {

		PreparedStatement pstmt = null;
		ResultSet messagesRS = null;
		Connection conn = null;
		ArrayList<String> logfiles = new ArrayList<String>();
		try {
			//long t = System.currentTimeMillis();
			conn = this.getDefaultConn();
			pstmt = conn.prepareStatement("select distinct "
					+ (table.equals(getMTable()) ? "logfile" : "min_logfile")
					+ " from " + table);
			messagesRS = pstmt.executeQuery();

			assert (!messagesRS.isClosed());
			while (messagesRS.next()) {
				logfiles.add(messagesRS.getString(1));
			}
			//DelayProfiler.updateDelay("get_indexed_logfiles", t);
		} catch (SQLException e) {
			log.severe(e.getClass().getSimpleName()
					+ " while getting logfile names");
			e.printStackTrace();
		} finally {
			cleanup(pstmt, messagesRS);
			cleanup(conn);
		}
		return logfiles;
	}

	private ArrayList<String> getActiveLogfilesFromCheckpointTable(
			TreeSet<Filename> candidates) {
		ArrayList<String> activeFrontier = this.getIndexedLogfiles(getCTable());
		Filename minLogfilename = null;
		for (String active : activeFrontier) {
			Filename curFilename = new Filename(new File(active));
			if (minLogfilename == null)
				minLogfilename = curFilename;
			if (curFilename.compareTo(minLogfilename) < 0)
				minLogfilename = curFilename;
		}
		assert (minLogfilename != null);
		ArrayList<String> activeLogfiles = new ArrayList<String>();
		for (Filename candidate : candidates)
			if (minLogfilename.compareTo(candidate) <= 0)
				activeLogfiles.add(candidate.file.toString());
		return activeLogfiles;
	}

	private void compactLogfiles() {
		File[] logfiles = this.getJournalFiles();
		TreeSet<Filename> sortedLogfiles = new TreeSet<Filename>();
		for (File f : logfiles)
			sortedLogfiles.add(new Filename(f));

		int empties = 0;
		File prevFile = null;
		for (Filename filename : sortedLogfiles) {
			File logfile = filename.file;
			// never try to compact the current log file
			if (logfile.toString().equals(this.journaler.curLogfile))
				break;
			log.log(Level.INFO, "{0} compacting logfile {1}", new Object[] {
					this, logfile });
			try {
				compactLogfile(logfile, this.getPacketizer(), this.messageLog);
				if(!logfile.exists()) if(++empties > 1) return;
				// we allow merging to double the file size limit
				if (prevFile != null
						&& prevFile.exists()
						&& logfile.exists()
						&& (prevFile.length() + logfile.length() <= 2 * MAX_LOG_FILE_SIZE))
					mergeLogfiles(prevFile, logfile, this.getPacketizer(),
							this.messageLog);
			} catch (IOException | JSONException e) {
				/*
				 * IOExceptions here are not necessarily bad and can happen
				 * because files being compacted or merged can be deleted by a
				 * parallel thread garbage collecting journal files. We could
				 * use something like stringLocker to efficiently synchronize
				 * between the two threads, but it is simpler to just incur the
				 * exception and move on.
				 */
				if (logfile.exists() && (prevFile == null || prevFile.exists()))
					log.severe(this + " incurred IOException " + e.getMessage());
				e.printStackTrace();
			}
			if(logfile.exists()) prevFile = logfile;

			if (logfile.length() < 3 * MAX_LOG_FILE_SIZE / 4)
				continue;
			/*
			 * The break in the else clause below assumes that once we have
			 * reached a point where logfiles can not be compacted by more than
			 * 25%, we might as well stop instead of trying to compact the
			 * remaining files. But it is still possible with some workloads for
			 * more recent files to be compactable even though older files are
			 * not compactable. For example, a recent flurry of requests all to
			 * the same or a small number of paxos groups could result in all or
			 * most of the logfile being unnecessary. To aggressively try to
			 * compact anyway, LAZY_COMPACTION should be disabled; that will
			 * also increase the compaction overhead even for less "adversarial"
			 * workloads.
			 */
			else if (LAZY_COMPACTION)
				break;
		}
	}
	
	private static final String TMP_FILE_SUFFIX = ".tmp";
	
	private static void compactLogfile(File file, PaxosPacketizer packetizer,
			MessageLogDiskMap msgLog) throws IOException, JSONException {
		RandomAccessFile raf = null, rafTmp = null;
		File tmpFile = new File(file.toString() + TMP_FILE_SUFFIX);
		int tmpFileSize = 0;
		boolean compacted = false, neededAtAll=false;
		HashMap<String, ArrayList<LogIndexEntry>> logIndexEntries = new HashMap<String, ArrayList<LogIndexEntry>>();
		
		try {
			long t = System.currentTimeMillis();
			raf = new RandomAccessFile(file.toString(), "r");
			rafTmp = new RandomAccessFile(tmpFile.toString(), "rw");
			while (raf.getFilePointer() < raf.length()) {
				long offset = rafTmp.getFilePointer();
				int length = raf.readInt();
				byte[] msg = new byte[length];
				raf.readFully(msg);
				PaxosPacket pp = packetizer != null ? packetizer
						.stringToPaxosPacket(new String(msg, CHARSET))
						: PaxosPacket.getPaxosPacket(new String(msg, CHARSET));
				if (!logIndexEntries.containsKey(pp.getPaxosID()))
					logIndexEntries.put(pp.getPaxosID(),
							new ArrayList<LogIndexEntry>());
				logIndexEntries.get(pp.getPaxosID()).add(
						new LogIndexEntry(getSlot(pp),
								getBallot(pp).ballotNumber,
								getBallot(pp).coordinatorID, pp.getType()
										.getInt(), file.toString(), offset,
								length));

				if (isLogMsgNeeded(pp, msgLog)) {
					ByteBuffer bbuf = ByteBuffer.allocate(length + 4);
					bbuf.putInt(length);
					bbuf.put(msg);
					rafTmp.write(bbuf.array());
					neededAtAll = true;
					tmpFileSize += bbuf.capacity();
				} else {
					compacted = true;
					log.log(Level.FINE,
							"From logfile {0} garbage collecting {1}",
							new Object[] { file, pp.getSummary() });
				}
			}
			DelayProfiler.updateDelay("compact", t);
		} finally {
			if (raf != null)
				raf.close();
			if (rafTmp != null) {
				rafTmp.getChannel().force(true);
				rafTmp.close();
			}
		}
		assert (tmpFile.exists() && tmpFile.length() == tmpFileSize) : tmpFile
				.length() + " != " + tmpFileSize;
		if (compacted && neededAtAll)
			synchronized (msgLog) {
				modifyLogfileAndLogIndex(file, tmpFile, logIndexEntries, msgLog);
			}
		else if (!neededAtAll) {
			log.log(Level.INFO,
					"Deleting logfile {0} as its log entries are no longer needed",
					new Object[] { file });
			deleteFile(file, msgLog);
		} else // !compacted
			log.log(Level.INFO,
					"Logfile {0} unchanged upon compaction attempt",
					new Object[] { file });
		assert (tmpFile.toString().endsWith(TMP_FILE_SUFFIX));
		if(tmpFile.exists()) deleteFile(tmpFile, msgLog);
	}
	

	// caller synchronizes
	private static void modifyLogfileAndLogIndex(File logfile, File tmpLogfile,
			HashMap<String, ArrayList<LogIndexEntry>> logIndexEntries, MessageLogDiskMap msgLog) {
		{
			logfile.delete();
			assert(!logfile.exists());
			while(!tmpLogfile.renameTo(logfile))
				log.severe(msgLog + " failed to rename " + tmpLogfile + " to " + logfile);
			//long t = System.currentTimeMillis();
			for(String paxosID : logIndexEntries.keySet()) 
				for (LogIndexEntry entry : logIndexEntries.get(paxosID))
					msgLog.modifyLogIndexEntry(paxosID, entry);
			//DelayProfiler.updateDelay("modindex", t);
		}
	}

	private static boolean isLogMsgNeeded(PaxosPacket pp,
			MessageLogDiskMap msgLog) {
		LogIndex logIndex = msgLog.get(pp.getPaxosID());
		assert (logIndex != null);
		return logIndex.isLogMsgNeeded(getSlot(pp), getBallot(pp).ballotNumber,
				getBallot(pp).coordinatorID, pp.getType().getInt());
	}

	/* This method merges the logfile prev into cur. Compacting only
	 * decreases the aggregate size of all logfiles. We need to merge logfiles
	 * in addition to compacting them because otherwise some type of workloads 
	 * can result in a very large number of small logfiles. Without merging,
	 * some weird workloads can result in as many as O(N*I) files, where N is
	 * the total number of paxos groups and I is the inter-checkpoint interval,
	 * each of which contains just a single log entry.
	 */
	private static void mergeLogfiles(File prev, File cur, PaxosPacketizer packetizer,
			MessageLogDiskMap msgLog) throws IOException, JSONException {
		File tmpFile = new File(cur.toString() + TMP_FILE_SUFFIX);
		RandomAccessFile rafTmp = null, rafPrev=null, rafCur=null;
		long t=System.currentTimeMillis();
		try {
			rafTmp = new RandomAccessFile(tmpFile.toString(), "rw");
			rafPrev = new RandomAccessFile(prev.toString(), "r");
			rafCur = new RandomAccessFile(cur.toString(), "r");
			byte[] buf = new byte[1024];
			int numRead = 0;
			// copy prev file to tmp file
			while ((numRead = rafPrev.read(buf)) > 0)
				rafTmp.write(buf, 0, numRead);
			// copy cur file to tmp file
			while ((numRead = rafCur.read(buf)) > 0)
				rafTmp.write(buf, 0, numRead);
		} finally {
			if(rafTmp!=null) rafTmp.close();
			if(rafPrev!=null) rafPrev.close();
			if(rafCur!=null) rafCur.close();
		}
		
		// copy tmp file index into memory
		HashMap<String, ArrayList<LogIndexEntry>> logIndexEntries = new HashMap<String, ArrayList<LogIndexEntry>>();
		try {
			rafTmp = new RandomAccessFile(tmpFile.toString(), "r");
			while (rafTmp.getFilePointer() < rafTmp.length()) {
				long offset = rafTmp.getFilePointer();
				int length = rafTmp.readInt();
				byte[] msg = new byte[length];
				rafTmp.readFully(msg);
				PaxosPacket pp = packetizer != null ? packetizer
						.stringToPaxosPacket(new String(msg, CHARSET))
						: PaxosPacket.getPaxosPacket(new String(msg, CHARSET));
				assert (pp != null) : " read logged message "
						+ new String(msg, CHARSET);
				if (!logIndexEntries.containsKey(pp.getPaxosID()))
					logIndexEntries.put(pp.getPaxosID(),
							new ArrayList<LogIndexEntry>());
				logIndexEntries.get(pp.getPaxosID()).add(
						new LogIndexEntry(getSlot(pp),
								getBallot(pp).ballotNumber,
								getBallot(pp).coordinatorID, pp.getType()
										.getInt(), cur.toString(), offset,
								length));
			}
		} finally {
			if(rafTmp!=null) rafTmp.close();
		}
		
		// atomically copy tmpFile to cur, adjust log index, delete prev
		synchronized(msgLog) {
			modifyLogfileAndLogIndex(cur, tmpFile, logIndexEntries, msgLog);
			prev.delete();
		}
		DelayProfiler.updateDelay("merge", t);
		log.log(Level.INFO, "{0} merged logfile {1} into {2}", new Object[] {
				msgLog, prev, cur });
	}

	private File[] getJournalFiles(String additionalMatch) {
		File[] dirFiles = (new File(this.journaler.logdir))
				.listFiles(new FileFilter() {
					@Override
					public boolean accept(File pathname) {
						return pathname.toString().startsWith(
								SQLPaxosLogger.this.journaler
										.getLogfilePrefix())
								|| (additionalMatch !=null ? pathname.toString().startsWith(
										additionalMatch) : false);
					}
				});
		return dirFiles;
	}
	private File[] getJournalFiles() {
		return this.getJournalFiles(null);
	}

	private ArrayList<File> deleteTmpJournalFiles() {
		File[] files = this.getJournalFiles(null);
		ArrayList<File> tmpFiles = new ArrayList<File>();
		for (File f : files)
			if (f.toString().endsWith(TMP_FILE_SUFFIX)) {
				f.delete();
				tmpFiles.add(f);
			}
		return tmpFiles;
	}

	private void deleteJournalFiles(TreeSet<Filename> candidates,
			ArrayList<String> activeLogfiles) {

		// delete files not in DB
		ArrayList<File> deleted = new ArrayList<File>();
		for (Filename filename : candidates)
			if (!activeLogfiles.contains(filename.file.toString())
					&& filename.file.toString().startsWith(
							this.journaler.getLogfilePrefix())) {
				deleteFile(filename.file, this.messageLog);
				log.log(Level.FINE,
						"{0} garbage collecting {1} because activeLogfiles = {2} and logfilePrefix = {3}",
						new Object[] { this, filename, activeLogfiles,
								this.journaler.getLogfilePrefix() });
				deleted.add(filename.file);
			}
		if (!deleted.isEmpty())
			log.log(Level.INFO, "{0} garbage collected log files {1}",
					new Object[] { this, (deleted) });
	}

	public ArrayList<PaxosPacket> getLoggedMessages(String paxosID) {
		return this.getLoggedMessages(paxosID, null);
	}

	/*
	 * Acceptors remove decisions right after executing them. So they need to
	 * fetch logged decisions from the disk to handle synchronization requests.
	 */
	@Override
	public ArrayList<PValuePacket> getLoggedDecisions(String paxosID,
			int version, int minSlot, int maxSlot) {
		if (ENABLE_JOURNALING && !DB_INDEX_JOURNAL)
			return new ArrayList<PValuePacket>(this.getLoggedFromMessageLog(
					paxosID, version, minSlot, maxSlot,
					PaxosPacketType.DECISION.getInt()).values());

		ArrayList<PValuePacket> decisions = new ArrayList<PValuePacket>();
		if (maxSlot - minSlot <= 0)
			return decisions;
		ArrayList<PaxosPacket> list = this.getLoggedMessages(paxosID,
				"and version=" + version + " and packet_type="
						+ PaxosPacketType.DECISION.getInt() + " and "
						+ getIntegerGTEConstraint("slot", minSlot) + " and "
						+ getIntegerLTConstraint("slot", maxSlot)); // wraparound-arithmetic
		assert (list != null);
		for (PaxosPacket p : list)
			decisions.add((PValuePacket) p);
		return decisions;
	}

	/*
	 * Called by an acceptor to return accepted proposals to the new potential
	 * coordinator. We store and return these from disk to reduce memory
	 * pressure. This allows us to remove accepted proposals once they have been
	 * committed.
	 */
	public Map<Integer, PValuePacket> getLoggedAccepts(String paxosID,
			int version, int firstSlot, Integer maxSlot) {

		if (ENABLE_JOURNALING && !DB_INDEX_JOURNAL)
			return this.getLoggedFromMessageLog(paxosID, version, firstSlot,
					maxSlot, PaxosPacketType.ACCEPT.getInt());

		//long t1 = System.currentTimeMillis();
		// fetch all accepts and then weed out those below firstSlot
		ArrayList<PaxosPacket> list = this.getLoggedMessages(
				paxosID,
				" and packet_type="
						+ PaxosPacketType.ACCEPT.getInt()
						+ " and "
						+ getIntegerGTEConstraint("slot", firstSlot)
						// maxSlot is null for getting lower ballot pvalues
						+ (maxSlot != null ? " and "
								+ getIntegerLTConstraint("slot", maxSlot) : "")
						+ " and version=" + version);

		TreeMap<Integer, PValuePacket> accepted = new TreeMap<Integer, PValuePacket>();
		for (PaxosPacket p : list) {
			int slot = AbstractPaxosLogger.getSlotBallot(p)[0];
			assert (p instanceof AcceptPacket) : p.getType() + ":" + p;
			AcceptPacket accept = (AcceptPacket) p;
			if ((slot - firstSlot >= 0)
					&& /* wraparound-arithmetic */
					(!accepted.containsKey(slot) || accepted.get(slot).ballot
							.compareTo(accept.ballot) < 0))
				accepted.put(slot, accept);
		}
		//DelayProfiler.updateDelay("getAccepts", t1);
		return accepted;
	}

	private Map<Integer, PValuePacket> getLoggedFromMessageLog(String paxosID,
			int version, int firstSlot, Integer maxSlot, int type) {
		//long t = System.currentTimeMillis();
		Map<Integer, PValuePacket> accepts = new HashMap<Integer, PValuePacket>();
		ArrayList<LogIndexEntry> logEntries = null;
		String[] logMsgStrings = null;

		synchronized(this.messageLog) {

			// first get logEntries from logIndex
			LogIndex logIndex = null;
			if ((logIndex = this.messageLog.getLogIndex(paxosID, version)) != null)
				logEntries = ((type == PaxosPacket.PaxosPacketType.ACCEPT
						.getInt() ? logIndex.getLoggedAccepts(firstSlot,
						maxSlot) : logIndex.getLoggedDecisions(firstSlot,
						maxSlot)));
			if (logEntries == null || logEntries.isEmpty()) {
				log.log(Level.FINE,
						"{0} found no {1} for {2}:[{3},{4}]",
						new Object[] {
								this,
								(PaxosPacketType.getPaxosPacketType(type)),
								paxosID,
								firstSlot,
								maxSlot,
								logIndex != null ? logIndex.getSummary(log
										.isLoggable(Level.FINE)) : null });
				return accepts;
			}

			// then get log message strings from file
			ArrayList<FileOffsetLength> fols = new ArrayList<FileOffsetLength>();
			for (LogIndexEntry logEntry : logEntries)
				fols.add(new FileOffsetLength(logEntry.getLogfile(), logEntry
						.getOffset(), logEntry.getLength()));
			try {
				logMsgStrings = this.getJournaledMessage(fols
						.toArray(new FileOffsetLength[0]));
			} catch (IOException e) {
				log.severe(this + " incurred IOException while getting logged "
						+ PaxosPacketType.getPaxosPacketType(type) + "s for "
						+ paxosID);
				e.printStackTrace();
			}
			if (logMsgStrings == null || logMsgStrings.length == 0) {
				log.log(Level.SEVERE,
						"{0} found no journaled {1} for {2}:[{3},{4}] despite logIndex = {5}",
						new Object[] { this,
								(PaxosPacketType.getPaxosPacketType(type)),
								paxosID, firstSlot, maxSlot,
								logIndex.getSummary(true) });
				return accepts;
			}
		}

		// then convert log message strings to pvalues 
		for (String logMsgStr : logMsgStrings) {
			assert (logMsgStr != null);
			PValuePacket packet = null;
			try {
				packet = (PValuePacket) (this.getPacketizer() != null ? this
						.getPacketizer().stringToPaxosPacket(logMsgStr)
						: PaxosPacket.getPaxosPacket(logMsgStr));
			} catch (JSONException e) {
				log.severe(this
						+ " incurred JSONException while getting logged accepts for "
						+ paxosID);
				e.printStackTrace();
			}
			if (packet != null)
				accepts.put(packet.slot, packet);
		}

		log.log(Level.FINE,
				"{0} returning {1} logged {2}s in response to {3}:[{4}, {5}]",
				new Object[] { this, accepts.size(),
						PaxosPacketType.getPaxosPacketType(type), paxosID,
						firstSlot, maxSlot });
		//DelayProfiler.updateDelay("getAccepts", t);
		return accepts;
	}

	/**
	 * Removes all state for paxosID except epoch final state. If paxosID is
	 * null, it removes state for **all** paxosIDs.
	 */
	public synchronized boolean remove(String paxosID, int version) {
		boolean removedCP = false, removedM = false, removedP = false;
		Statement stmt = null;
		String cmdC = "delete from "
				+ getCTable()
				+ (paxosID != null ? " where paxos_id='"
						+ paxosID
						+ "' and (version="
						+ version
						+ " or "
						+ SQLPaxosLogger.getIntegerLTConstraint("version",
								version) + ")" : " where true");
		synchronized(this.messageLog) {
			if (paxosID == null)
				this.messageLog.clear();
			else if (paxosID != null
					&& this.messageLog.getLogIndex(paxosID, version) != null) {
				this.messageLog.remove(paxosID);
				assert(this.messageLog.get(paxosID)==null);
				log.log(Level.INFO, "{0} removed logIndex for {1}:{2}",
						new Object[] { this, paxosID, version });
			}
		}
		String cmdM = "delete from "
				+ getMTable()
				+ (paxosID != null ? " where paxos_id='"
						+ paxosID
						+ "' and (version="
						+ version
						+ " or "
						+ SQLPaxosLogger.getIntegerLTConstraint("version",
								version) + ")" : " where true");
		String cmdP = "delete from "
				+ getPTable()
				+ (paxosID != null ? " where paxos_id='" + paxosID + "'"
						: " where true");
		Connection conn = null;
		try {
			conn = this.getDefaultConn();
			stmt = conn.createStatement();
			stmt.execute(cmdC);
			removedCP = true;
			stmt.execute(cmdM);
			removedM = true;
			stmt.execute(cmdP);
			removedP = true;
			// conn.commit();
			log.log(Level.FINE,
					"{0} removed all state for {1}:{2} and pause state for all versions of {3} ",
					new Object[] { this, paxosID, version, paxosID });
		} catch (SQLException sqle) {
			if (!removedP)
				log.severe("Could not remove table "
						+ (removedCP ? (removedM ? getPTable() : getMTable())
								: getCTable()));
			sqle.printStackTrace();
		} finally {
			cleanup(stmt);
			cleanup(conn);
		}
		return removedCP && removedM;
	}

	public boolean removeAll() {
		for (File f : this.getJournalFiles(this.getLogIndexDBPrefix()))
			if (f.length() != 0) {
				log.log(Level.INFO, "{0} removing log file {1}", new Object[] {
						this, f });
				f.delete();
			}
		return this.remove(null, 0);
	}

	public void closeImpl() {
		log.log(Level.INFO, "{0}{1}", new Object[] { this, " closing DB" });
		this.GC.shutdownNow();// cancel();
		// messageLog should be closed before DB
		this.messageLog.close();
		this.setClosed(true);
		if(this.mapDB!=null) this.mapDB.close();
		// can not close derby until all instances are done
		if (allClosed() || !isEmbeddedDB())
			this.closeGracefully();
	}

	public String toString() {
		return this.getClass().getSimpleName() + strID;
	}

	private static boolean isEmbeddedDB() {
		return SQL_TYPE.equals(SQL.SQLType.EMBEDDED_DERBY)
				|| SQL_TYPE.equals(SQL.SQLType.EMBEDDED_H2);
	}

	/**
	 * Closes the database and the connection. Must be invoked by anyone
	 * creating a DerbyPaxosLogger object, otherwise recovery will take longer
	 * upon the next bootup.
	 * 
	 * @return Returns true if closed successfully.
	 */
	public boolean closeGracefully() {

		/*
		 * If there are pending tasks and the DB is closed ungracefully, this
		 * can cause problems upon recovery. The DB is consistent as per its
		 * design but it takes some time upon recovery for it to rollback or
		 * roll forward incomplete tasks. What this means is that some logged
		 * messages may not be available when a recovering node reads them to
		 * roll forward but may suddenly become available a little while later
		 * when the logged messages finally get committed. This triggers some
		 * assert violations in the paxos code as prepare replies contain
		 * positive replies even though the list of contained accepts implies
		 * otherwise. Discovered these symptoms the hard way!
		 * 
		 * The static waitToFinishAll() parent method ensures that all derby DB
		 * instances have finished processing any pending log or checkpoint
		 * tasks before actually closing the DB. Otherwise, because it is an
		 * embedded DB, invoking shutdown like below within any instance will
		 * end up ungracefully shutting down the DB for all instances. Invoking
		 * shutdown also means that tests with recovery need to instantiate a
		 * new JVM, so we simply don't shutdown derby (but wait till derby is
		 * all done before the JVM terminates).
		 */

		if (isEmbeddedDB()) {
			// whole block is a no-op because DONT_SHUTDOWN_DB defaults to true
			try {
				// the shutdown=true attribute shuts down Derby
				if (!DONT_SHUTDOWN_EMBEDDED)
					DriverManager.getConnection(SQL.getProtocolOrURL(SQL_TYPE)
							+ ";shutdown=true");
				// To shut down a specific database only, but keep the
				// databases), specify a database in the connection URL:
			} catch (SQLException sqle) {
				if (((sqle.getErrorCode() == 50000) && ("XJ015".equals(sqle
						.getSQLState())))) {
					// we got the expected exception
					log.info("Derby shut down normally");
					// Note that for single database shutdown, the expected
					// SQL state is "08006", and the error code is 45000.
				} else {
					// if the error code or SQLState is different, we have
					// an unexpected exception (shutdown failed)
					log.severe("Derby did not shut down normally");
					sqle.printStackTrace();
				}
			}
		}
		// not embedded => just need to close connections

		try {
			// Close statements
			this.cleanup(logMsgStmt);
			this.cleanup(checkpointStmt);
			this.cleanup(cursorPstmt);
			this.cleanup(cursorRset);
			// Close connections
			if (this.defaultConn != null && !this.defaultConn.isClosed()) {
				cleanup(this.defaultConn);
				this.defaultConn = null;
			}
			if (this.cursorConn != null && !this.cursorConn.isClosed()) {
				cleanup(this.cursorConn);
				this.defaultConn = null;
			}

		} catch (SQLException sqle) {
			log.severe("Could not close connection gracefully");
			sqle.printStackTrace();
		}
		return isClosed();
	}

	/***************** End of public methods ********************/

	// synchronized coz it should be called just onece
	private synchronized boolean initialize() {
		if (!isClosed())
			return true;
		if (!connectDB() || !createTables())
			return false;
		setClosed(false); // setting open
		return true;
	}

	/**
	 * Creates a paxosID-primary-key table for checkpoints and another table for
	 * messages that indexes slot, ballotnum, and coordinator. The checkpoint
	 * table also stores the slot, ballotnum, and coordinator of the checkpoint.
	 * The index in the messages table is useful to optimize searching for old
	 * advanced. The test for "old" is based on the slot, ballotnum, and
	 * coordinator fields, so they are indexed.
	 */
	private boolean createTables() {
		boolean createdCheckpoint = false, createdMessages = false, createdPTable = false, createdPrevCheckpoint = false;
		String cmdC = "create table "
				+ getCTable()
				+ " ("
				+ C.PAXOS_ID.toString()
				+ " varchar("
				+ MAX_PAXOS_ID_SIZE
				+ ") not null, "
				+ C.VERSION.toString()
				+ " int, members varchar("
				+ MAX_GROUP_STR_LENGTH
				+ "), "
				+ C.SLOT.toString()
				+ " int, "
				+ C.BALLOTNUM.toString()
				+ " int, "
				+ C.COORDINATOR.toString()
				+ " int, "
				+ C.MIN_LOGFILE.toString()
				+ " varchar("
				+ MAX_FILENAME_SIZE
				+ "), "
				+ C.CREATE_TIME.toString()
				+ " bigint, serialized varchar("
				+ PAUSE_STATE_SIZE
				+ "), has_serialized boolean default false, logindex "
				+ SQL.getBlobString(LOG_INDEX_SIZE, SQL_TYPE)
				+ ", "
				+ C.STATE.toString()
				+ (getCheckpointBlobOption() ? SQL.getBlobString(
						maxCheckpointSize, SQL_TYPE) : " varchar("
						+ maxCheckpointSize + ")") + ", " + " primary key ("
				+ C.PAXOS_ID.toString() + "))";
		/*
		 * It is best not to have a primary key in the log message table as
		 * otherwise batch inserts can create exceptions as derby does not seem
		 * to have an insert if not exists primitive.
		 */
		String cmdM = "create table "
				+ getMTable()
				+ " (paxos_id varchar("
				+ MAX_PAXOS_ID_SIZE
				+ ") not null, version int, slot int, ballotnum int, "
				+ "coordinator int, packet_type int, logfile varchar ("
				+ MAX_FILENAME_SIZE
				+ "),  foffset int, length bigint, message "
				+ (getLogMessageBlobOption() ? SQL.getBlobString(
						maxLogMessageSize, SQL_TYPE) : " varchar("
						+ maxLogMessageSize + ")") + ")";

		String cmdPC = "create table "
				+ getPCTable()
				+ " (paxos_id varchar("
				+ MAX_PAXOS_ID_SIZE
				+ ") not null, version int, members varchar("
				+ MAX_GROUP_STR_LENGTH
				+ "), slot int, "
				+ "ballotnum int, coordinator int, state "
				+ (getCheckpointBlobOption() ? SQL.getBlobString(
						maxCheckpointSize, SQL_TYPE) : " varchar("
						+ maxCheckpointSize + ")")
				+ ", create_time bigint, primary key (paxos_id))";

		/*
		 * We create a non-unique-key index below instead of (unique) primary
		 * key (commented out above) as otherwise we will get duplicate key
		 * exceptions during batch inserts. It is unnecessary to create an index
		 * on ballotnum and coordinator as the number of logged prepares is
		 * likely to be small for any single group.
		 */
		String cmdMI = "create index messages_index on " + getMTable() + "("
				+ C.PAXOS_ID.toString() + ", " + C.PACKET_TYPE.toString()
				+ ", slot)"; // ,ballotnum,coordinator)";
		String cmdCI = "create index messages_index on " + getCTable() + "(has_serialized)";

		String cmdP = "create table " + getPTable() + " (paxos_id varchar("
				+ MAX_PAXOS_ID_SIZE + ") not null, serialized varchar("
				+ PAUSE_STATE_SIZE + "), logindex "
				+ SQL.getBlobString(LOG_INDEX_SIZE, SQL_TYPE)
				+ ", primary key (paxos_id))";

		// this.dropTable(getPTable()); // pause table is unnecessary
		// this.clearTable(getPTable()); // pause table is unnecessary

		Statement stmt = null;
		Connection conn = null;
		try {
			conn = this.getDefaultConn();
			stmt = conn.createStatement();
			createdCheckpoint = createTable(stmt, cmdC, getCTable())
					&& createIndex(stmt, cmdCI, getCTable());
			createdMessages = createTable(stmt, cmdM, getMTable())
					&& (!Config.getGlobalBoolean(PC.INDEX_LOG_TABLE) || createIndex(
							stmt, cmdMI, getMTable()));
			createdPTable = createTable(stmt, cmdP, getPTable());
			createdPrevCheckpoint = createTable(stmt, cmdPC, getPCTable());
			log.log(Level.INFO, "{0}{1}{2}{3}{4}{5}", new Object[] {
					"Created tables ", getCTable(), " and ", getMTable(),
					" and ", getPTable() });
		} catch (SQLException sqle) {
			log.severe("Could not create table(s): "
					+ (createdPTable ? "" : getPTable()) + " "
					+ (createdPrevCheckpoint ? "" : getPCTable()) + " "
					+ (createdMessages ? "" : getMTable()) + " "
					+ (createdCheckpoint ? "" : getCTable()));
			sqle.printStackTrace();
		} finally {
			cleanup(stmt);
			cleanup(conn);
		}
		this.sanityCheckTables(cmdC, cmdMI, cmdPC);
		return createdCheckpoint && createdMessages && createdPTable;
	}

	private void sanityCheckTables(String cmdC, String cmdM, String cmdPC) {
		Statement stmt = null;
		ResultSet rset = null;
		Connection conn = null;
		try {
			conn = this.getDefaultConn();
			stmt = conn.createStatement();
			DatabaseMetaData meta = conn.getMetaData();
			rset = meta.getColumns(null, null, null, null);
			if (!rset.next()) {
				log.severe(this + ": metadata query returned null; exiting");
				System.exit(1);
			}
			while (rset.next()) {
				if (rset.getString("TABLE_NAME").equals(
						getCTable().toUpperCase())
						&& rset.getString("COLUMN_NAME").equals("STATE")) {
					log.info(this
							+ " : "
							+ rset.getString("TABLE_NAME")
							+ " : "
							+ rset.getString("COLUMN_NAME")
							+ " : "
							+ rset.getInt("COLUMN_SIZE")
							+ (MAX_CHECKPOINT_SIZE > rset.getInt("COLUMN_SIZE") ? " -> "
									+ MAX_CHECKPOINT_SIZE
									: ""));
					if (MAX_CHECKPOINT_SIZE > rset.getInt("COLUMN_SIZE")) {
						stmt.execute("alter table "
								+ getCTable()
								+ " alter column state set data type "
								+ (getCheckpointBlobOption() ? SQL
										.getBlobString(maxCheckpointSize,
												SQL_TYPE) : " varchar("
										+ maxCheckpointSize + ")"));
						stmt.execute("alter table "
								+ getPCTable()
								+ " alter column state set data type "
								+ (getCheckpointBlobOption() ? SQL
										.getBlobString(maxCheckpointSize,
												SQL_TYPE) : " varchar("
										+ maxCheckpointSize + ")"));

					}
				}
				if (rset.getString("TABLE_NAME").equals(
						getMTable().toUpperCase())
						&& rset.getString("COLUMN_NAME").equals("MESSAGE")) {
					log.info(this
							+ " : "
							+ rset.getString("TABLE_NAME")
							+ " : "
							+ rset.getString("COLUMN_NAME")
							+ " : "
							+ rset.getInt("COLUMN_SIZE")
							+ (MAX_LOG_MESSAGE_SIZE > rset
									.getInt("COLUMN_SIZE") ? " -> "
									+ MAX_LOG_MESSAGE_SIZE : ""));
					if (MAX_LOG_MESSAGE_SIZE > rset.getInt("COLUMN_SIZE"))
						stmt.execute("alter table "
								+ getMTable()
								+ " alter column message set data type "
								+ (getLogMessageBlobOption() ? SQL
										.getBlobString(maxLogMessageSize,
												SQL_TYPE) : " varchar("
										+ maxLogMessageSize + ")"));
				}
			}

		} catch (Exception sqle) {
			log.severe("SQLException while sanity checking table schema");
			sqle.printStackTrace();
			System.exit(1);
		} finally {
			cleanup(stmt);
			cleanup(rset);
			cleanup(conn);
		}
	}

	private boolean createTable(Statement stmt, String cmd, String table) {
		boolean created = false;
		try {
			stmt.execute(cmd);
			created = true;
		} catch (SQLException sqle) {
			if (SQL.DUPLICATE_TABLE.contains(sqle.getSQLState())) {
				log.log(Level.INFO, "{0}{1}{2}", new Object[] { "Table ",
						table, " already exists" });
				created = true;
			} else {
				log.severe("Could not create table: " + table + " "
						+ sqle.getSQLState() + " " + sqle.getErrorCode());
				sqle.printStackTrace();
			}
		}
		return created;
	}

	private boolean createIndex(Statement stmt, String cmd, String table) {
		return createTable(stmt, cmd, table);
	}

	// used only to drop the pause table
	protected boolean dropTable(String table) {
		String cmd = "drop table " + getPTable();
		PreparedStatement pstmt = null;
		boolean dropped = false;
		try {
			Connection conn = this.getDefaultConn();
			pstmt = conn.prepareStatement(cmd);
			pstmt.execute();
			// conn.commit();
			dropped = true;
			log.log(Level.FINE, "{0}{1}{2}", new Object[] { this,
					" dropped pause table ", table });
		} catch (SQLException sqle) {
			if (!SQL.NONEXISTENT_TABLE.contains(sqle.getSQLState())) {
				log.severe(this + " could not drop table " + table + ":"
						+ sqle.getSQLState() + ":" + sqle.getErrorCode());
				sqle.printStackTrace();
			}
		}
		return dropped;
	}

	// FIXME: used to test with clearing pause table upon recovery
	@SuppressWarnings("unused")
	private boolean clearTable(String table) {
		String cmd = "delete from " + getPTable() + " where true";
		PreparedStatement pstmt = null;
		boolean dropped = false;
		try {
			Connection conn = this.getDefaultConn();
			pstmt = conn.prepareStatement(cmd);
			pstmt.execute();
			// conn.commit();
			dropped = true;
			log.log(Level.FINE, "{0}{1}{2}", new Object[] { this,
					" dropped pause table ", table });
		} catch (SQLException sqle) {
			if (!SQL.NONEXISTENT_TABLE.contains(sqle.getSQLState())) {
				log.severe(this + " could not clear table " + table + ":"
						+ sqle.getSQLState() + ":" + sqle.getErrorCode());
				sqle.printStackTrace();
			}
		}
		return dropped;
	}

	private static boolean dbDirectoryExists(String dbDirectory) {
		File f = new File(dbDirectory);
		return f.exists() && f.isDirectory();
	}

	/**
	 * This method will connect to the DB while creating it if it did not
	 * already exist. This method is not really needed but exists only because
	 * otherwise c3p0 throws unsuppressable warnings about DB already existing
	 * no matter how you use it. So we now create the DB separately and always
	 * invoke c3p0 without the create flag (default false).
	 * 
	 * @param sqlType
	 * @param logDir
	 * @param database
	 * @return True if database exists.
	 */
	// @Deprecated
	public static boolean existsDB(SQL.SQLType sqlType, String logDir,
			String database) {
		try {
			Class.forName(SQL.getDriver(SQL_TYPE)).newInstance();
		} catch (InstantiationException | IllegalAccessException
				| ClassNotFoundException e) {
			e.printStackTrace();
			return false;
		}
		Connection conn = null;
		try {
			conn = DriverManager.getConnection(SQL.getProtocolOrURL(sqlType)
					+ logDir
					+ database
					+ (!dbDirectoryExists(logDir + database) ? ";create=true"
							: ""));
		} catch (SQLException sqle) {
			sqle.printStackTrace();
		} finally {
			if (conn != null)
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
		}
		return true;
	}

	private void ensureLogDirectoryExists(String logDir) {
		File f = new File(logDir);
		if (!f.exists())
			f.mkdirs();
	}

	private boolean connectDB() {
		boolean connected = false;
		int connAttempts = 0, maxAttempts = 1;
		long interAttemptDelay = 2000; // ms
		Properties props = new Properties(); // connection properties
		/*
		 * Providing a user name and PASSWORD is optional in embedded derby.
		 * But, for some inscrutable, undocumented reason, it is important for
		 * derby (or maybe c3p0) to have different user names for different
		 * nodes, otherwise the performance with concurrent inserts and updates
		 * is terrible.
		 */
		props.put("user", SQL.getUser() + (isEmbeddedDB() ? this.myID : ""));
		props.put("password", SQL.getPassword());
		ensureLogDirectoryExists(this.logDirectory);
		String dbCreation = SQL.getProtocolOrURL(SQL_TYPE)
				+ (isEmbeddedDB() ?
				// embedded DB pre-creates DB to avoid c3p0 stack traces
				this.logDirectory
						+ DATABASE
						+ this.myID
						+ (!existsDB(SQL_TYPE, this.logDirectory, DATABASE
								+ this.myID) ? ";create=true" : "")
						:
						// else just use like a typical SQL DB
						DATABASE + this.myID + "?createDatabaseIfNotExist=true");

		try {
			dataSource = (ComboPooledDataSource) setupDataSourceC3P0(
					dbCreation, props);
		} catch (SQLException e) {
			log.severe("Could not create pooled data source to DB "
					+ dbCreation);
			e.printStackTrace();
			return false;
		}

		while (!connected && connAttempts < maxAttempts) {
			try {
				connAttempts++;
				log.info("Attempting getCursorConn() to db " + dbCreation);
				if (getCursorConn() == null)
					// test opening a connection
					this.cursorConn = dataSource.getConnection();
				log.info("Connected to and created database " + DATABASE
						+ this.myID);
				connected = true;
				// mchange complains at unsuppressable INFO otherwise
				if (isEmbeddedDB())
					fixURI(); // remove create flag
			} catch (SQLException sqle) {
				log.severe("Could not connect to derby DB: "
						+ sqle.getSQLState() + ":" + sqle.getErrorCode());
				sqle.printStackTrace();
				try {
					Thread.sleep(interAttemptDelay);
				} catch (InterruptedException ie) {
					ie.printStackTrace();
				}
			} finally {
				cleanupCursorConn();
			} // close the test connection
		}
		return connected;
	}

	private static void addDerbyLogger(SQLPaxosLogger derbyLogger) {
		synchronized (SQLPaxosLogger.instances) {
			if (!SQLPaxosLogger.instances.contains(derbyLogger))
				SQLPaxosLogger.instances.add(derbyLogger);
		}
	}

	private static boolean allClosed() {
		synchronized (SQLPaxosLogger.instances) {
			for (SQLPaxosLogger logger : instances) {
				if (!logger.isClosed())
					return false;
			}
			return true;
		}
	}

	private synchronized boolean isClosed() {
		return closed;
	}

	private synchronized void setClosed(boolean c) {
		closed = c;
	}

	private String getCTable() {
		return CHECKPOINT_TABLE + this.myID;
	}

	private String getPCTable() {
		return PREV_CHECKPOINT_TABLE + this.myID;
	}

	private String getMTable() {
		return MESSAGES_TABLE + this.myID;
	}

	private String getPTable() {
		return PAUSE_TABLE + this.myID;
	}

	private synchronized void cleanupCursorConn() {
		try {
			if (this.cursorConn != null && CONN_POOLING) {
				this.cursorConn.close();
				this.cursorConn = null;
			}
			if (this.cursorPstmt != null) {
				this.cursorPstmt.close();
				this.cursorPstmt = null;
			}
			if (this.cursorRset != null) {
				this.cursorRset.close();
				this.cursorRset = null;
			}
		} catch (SQLException sqle) {
			log.severe("Could not close connection " + this.cursorConn);
			sqle.printStackTrace();
		}
	}

	private void cleanup(Connection conn) {
		try {
			if (conn != null && CONN_POOLING) {
				conn.close();
			}
		} catch (SQLException sqle) {
			log.severe("Could not close connection " + conn);
			sqle.printStackTrace();
		}
	}

	private void cleanup(Statement stmt) {
		try {
			if (stmt != null) {
				stmt.close();
			}
		} catch (SQLException sqle) {
			log.severe("Could not clean up statement " + stmt);
			sqle.printStackTrace();
		}
	}

	private void cleanup(ResultSet rs) {
		try {
			if (rs != null) {
				rs.close();
			}
		} catch (SQLException sqle) {
			log.severe("Could not close result set " + rs);
			sqle.printStackTrace();
		}
	}

	private void cleanup(PreparedStatement pstmt, ResultSet rset) {
		cleanup(pstmt);
		cleanup(rset);
	}

	/******************** Start of testing methods ***********************/
	// Convenient for testing and debugging
	protected String getDBAsString(String paxosID) {
		String print = "";
		ArrayList<RecoveryInfo> recoveries = getAllPaxosInstances();
		for (RecoveryInfo pri : recoveries) {
			String s = pri.getPaxosID();
			String state = getCheckpointState(s);
			Ballot b = getCheckpointBallot(s);
			int slot = getCheckpointSlot(s);
			print += (s + " " + (pri.getMembers()) + " " + slot + " " + b + " "
					+ state + "\n");
			ArrayList<PaxosPacket> loggedMsgs = getLoggedMessages(paxosID);
			if (loggedMsgs != null)
				for (PaxosPacket pkt : loggedMsgs)
					print += (pkt + "\n");
		}
		return print;
	}

	protected boolean isInserted(String paxosID, int[] group, int slot,
			Ballot ballot, String state) {
		return this
				.isInserted(getCTable(), paxosID, group, slot, ballot, state);
	}

	protected boolean isInserted(String table, String paxosID, int[] group,
			int slot, Ballot ballot, String state) {
		System.out.println(getCheckpointState(table, paxosID, "members"));
		return (getCheckpointState(table, paxosID, "members").equals(Util
				.toJSONString(group).toString()))
				&& (getCheckpointState(table, paxosID, "slot")
						.equals("" + slot))
				&& (getCheckpointState(table, paxosID, "ballotnum").equals(""
						+ ballot.ballotNumber))
				&& (getCheckpointState(table, paxosID, "coordinator").equals(""
						+ ballot.coordinatorID))
				&& (state == null || getCheckpointState(table, paxosID, "state")
						.equals("" + state));
	}

	// used only for testing
	private boolean isLogged(String paxosID, int slot, int ballotnum,
			int coordinator, String msg) {
		PreparedStatement pstmt = null;
		ResultSet messagesRS = null;
		Connection conn = null;
		String cmd = "select paxos_id, message from " + getMTable()
				+ " where paxos_id='" + paxosID + "' " + " and slot=" + slot
				+ " and ballotnum=" + ballotnum + " and coordinator="
				+ coordinator
				+ (getLogMessageBlobOption() ? "" : " and message=?");
		boolean logged = false;

		try {
			conn = this.getDefaultConn();
			pstmt = conn.prepareStatement(cmd);
			if (!getLogMessageBlobOption())
				pstmt.setString(1, msg); // will not work for clobs
			messagesRS = pstmt.executeQuery();
			while (messagesRS.next() && !logged) {
				String insertedMsg = (!getLogMessageBlobOption() ? messagesRS
						.getString(2) : lobToString(messagesRS.getBlob(2)));
				logged = msg.equals(insertedMsg);
			}
		} catch (SQLException | IOException e) {
			log.severe(e.getClass().getSimpleName() + " while getting slot "
					+ " : " + e);
			e.printStackTrace();
		} finally {
			cleanup(pstmt, messagesRS);
			cleanup(conn);
		}
		return logged;
	}

	protected boolean isLogged(PaxosPacket packet) throws JSONException {
		int[] sb = AbstractPaxosLogger.getSlotBallot(packet);
		assert (sb.length == 3);
		return this.isLogged(packet.getPaxosID(), sb[0], sb[1], sb[2],
				packet.toString());
	}
	private double createCheckpoints(int size) {
		return this.createCheckpoints(size, false);
	}
	private double createCheckpoints(int size, boolean batched) {
		int[] group = { 2, 4, 5, 11, 23, 34, 56, 78, 80, 83, 85, 96, 97, 98, 99 };
		if(size > 1 ) System.out.println("\nStarting " + size + " writes: ");
		long t1 = System.currentTimeMillis();
		int k = 1;
		DecimalFormat df = new DecimalFormat("#.##");
		CheckpointTask[] cpTasks = new CheckpointTask[size];
		for (int i = 0; i < size; i++) {
			if(!batched) {
				this.putCheckpointState("paxos" + i, 0, group, 0, new Ballot(0,
						i % 34), "hello" + i, 0);
				if (i % k == 0 && i > 0) {
					System.out.print("[" + i + " : "
							+ df.format(((double) (System.currentTimeMillis() - t1)) / i) + "ms]\n");
					k *= 2;
				}
			} else {
				cpTasks[i] = new CheckpointTask(this, "paxos" + i, 0,
						Util.arrayOfIntToStringSet(group), 0, new Ballot(0,
								i % 34), "hello", 0);
			}
		}
		if(batched) 
			this.putCheckpointState(cpTasks);
		return (double) (System.currentTimeMillis() - t1) / size;
	}

	private double readCheckpoints(int size) {
		if(size > 1) System.out.println("\nStarting " + size + " reads: ");
		long t1 = System.currentTimeMillis(), t2 = t1;
		int k = 1;
		DecimalFormat df = new DecimalFormat("#.##");
		for (int i = 0; i < size; i++) {
			this.getStatePacket("paxos" + i);
			t2 = System.currentTimeMillis();
			if (i % k == 0 && i > 0) {
				System.out.print("[" + i + " : "
						+ df.format(((double) (t2 - t1)) / i) + "ms]\n");
				k *= 2;
			}
		}
		return (double) (t2 - t1) / size;
	}

	private static DataSource setupDataSourceC3P0(String connectURI,
			Properties props) throws SQLException {

		ComboPooledDataSource cpds = new ComboPooledDataSource();
		try {
			cpds.setDriverClass(SQL.getDriver(SQL_TYPE));
			cpds.setJdbcUrl(connectURI);
			if (!SQL_TYPE.equals(SQL.SQLType.EMBEDDED_H2)) {
				cpds.setUser(props.getProperty("user"));
				cpds.setPassword(props.getProperty("password"));
			}
			cpds.setAutoCommitOnClose(true);
			cpds.setMaxPoolSize(MAX_POOL_SIZE);
		} catch (PropertyVetoException pve) {
			pve.printStackTrace();
		}

		return cpds;
	}

	private void fixURI() {
		this.dataSource.setJdbcUrl(SQL.getProtocolOrURL(SQL_TYPE)
				+ this.logDirectory + DATABASE + this.myID
				+ (isEmbeddedDB() ? "" : "?rewriteBatchedStatements=true"));
	}

	/**
	 * Gets a map of all paxosIDs and their corresponding group members. Used
	 * only for testing.
	 */
	private ArrayList<RecoveryInfo> getAllPaxosInstances() {
		if (isClosed())
			return null;

		ArrayList<RecoveryInfo> allPaxosInstances = new ArrayList<RecoveryInfo>();
		PreparedStatement pstmt = null;
		ResultSet stateRS = null;
		Connection conn = null;
		try {
			conn = this.getDefaultConn();
			pstmt = this.getPreparedStatement(conn, getCTable(), null,
					"paxos_id, version, members");
			stateRS = pstmt.executeQuery();
			while (stateRS != null && stateRS.next()) {
				String paxosID = stateRS.getString(1);
				int version = stateRS.getInt(2);
				String members = stateRS.getString(3);
				String[] pieces = Util.jsonToStringArray(members);
				allPaxosInstances
						.add(new RecoveryInfo(paxosID, version, pieces));
			}
		} catch (SQLException | JSONException e) {
			log.severe((e instanceof SQLException ? "SQL" : "JSON")
					+ "Exception while getting all paxos IDs " + " : " + e);
		} finally {
			cleanup(pstmt, stateRS);
			cleanup(conn);
		}
		return allPaxosInstances;
	}

	@Override
	public boolean deleteEpochFinalCheckpointState(String paxosID, int version) {
		if (isClosed() /* || !isLoggingEnabled() */)
			return true;
		boolean deleted = false;
		PreparedStatement pstmt = null;
		ResultSet rset = null;
		Connection conn = null;
		String cmd = "delete from " + getPCTable()
				+ " where paxos_id=? and (version=" + version + " or "
				+ getIntegerLTConstraint("version", version) + ")";
		try {
			conn = this.getDefaultConn();
			pstmt = conn.prepareStatement(cmd);
			pstmt.setString(1, paxosID);
			int numDeleted = pstmt.executeUpdate();
			// conn.commit();
			deleted = true;
			if (numDeleted > 0)
				log.log(Level.INFO,
						"{0} dropped epoch final state for {1}:{2}",
						new Object[] { this, paxosID, version });
			{// sanity check block
				Integer ghostVersion = null;
				assert ((ghostVersion = this
						.getEpochFinalCheckpointVersion(paxosID)) == null || ghostVersion
						- version > 0);
			}
		} catch (SQLException sqle) {
			log.severe(this + " failed to delete final state for " + paxosID
					+ ":" + version);
			sqle.printStackTrace();
		} finally {
			cleanup(pstmt, rset);
			cleanup(conn);
		}
		;
		return deleted;
	}
	
	static class MapDBContainer {

		final DB dbDisk;

		final DB dbMemory;

		// big map populated with data expired from cache
		final HTreeMap<String, LogIndex> onDisk;

		// fast in-memory collection with limited size
		final HTreeMap<String, LogIndex> inMemory;

		MapDBContainer(DB dbDisk, DB dbMemory) {
			this.dbDisk = dbDisk;
			this.dbMemory = dbMemory;
			this.onDisk = dbDisk.hashMapCreate("onDisk").makeOrGet();
			this.inMemory = dbMemory.hashMapCreate("inMemory")
					.expireAfterAccess(120, TimeUnit.SECONDS)
					// this registers overflow to `onDisk`
					.expireOverflow(onDisk, true)
					// good idea is to enable background expiration
					.executorEnable().make();
		}

		public void close() {
			if(this.dbMemory!=null) {
				this.dbMemory.commit();
				this.dbMemory.close();
			}
			if(this.dbDisk!=null) {
				this.dbDisk.commit();
				this.dbDisk.close();
			}
		}
	}

	/* This was an experimental MessageLog based on mapdb that is no longer used.
	 * mapdb seems way too slow even when all map entries ought to comfortably
	 * fit in memory. We also don't need mapdb's level of durability upon a 
	 * crash because we maintain our own write-ahead log.
	 */
	@Deprecated
	static class MessageLogMapDB extends MessageLogPausable {
		final HTreeMap<String, LogIndex> inMemory;
		final HTreeMap<String, LogIndex> onDisk;

		MessageLogMapDB(HTreeMap<String, LogIndex> inMemory,
				HTreeMap<String, LogIndex> onDisk, Diskable<String,LogIndex> disk) {
			super(disk);
			this.inMemory = inMemory;
			this.onDisk = onDisk;
		}
		
		synchronized LogIndex getOrCreateIfNotExistsOrLower(String paxosID,
				int version) {
			LogIndex logIndex = this.inMemory.get(paxosID);
			if (logIndex == null || logIndex.version - version < 0)
				this.onDisk.put(paxosID, (logIndex = new LogIndex(paxosID,
						version)));
			return logIndex!=null && logIndex.version == version ? logIndex : null;
		}

		synchronized void add(PaxosPacket msg, String logfile, long offset,
				int length) {
			long t = System.nanoTime();
			LogIndex logIndex = this.getOrCreateIfNotExistsOrLower(
					msg.getPaxosID(), msg.getVersion());
			if (logIndex == null)
				return;

			boolean isPValue = msg instanceof PValuePacket;
			logIndex.add(isPValue ? ((PValuePacket) msg).slot
					: ((PreparePacket) msg).firstUndecidedSlot,
					isPValue ? ((PValuePacket) msg).ballot.ballotNumber
							: ((PreparePacket) msg).ballot.ballotNumber,
					isPValue ? ((PValuePacket) msg).ballot.coordinatorID
							: ((PreparePacket) msg).ballot.coordinatorID, msg
							.getType().getInt(), logfile, offset, length);
			this.inMemory.put(msg.getPaxosID(), logIndex);
			assert (logIndex.getMinLogfile() != null);
			if (Util.oneIn(10)) DelayProfiler.updateDelayNano("logAddDelay", t);
		}

		synchronized void setGCSlot(String paxosID, int version, int gcSlot) {
			LogIndex logIndex = this.getOrCreateIfNotExistsOrLower(paxosID,
					version);
			if (logIndex == null)
				return;

			this.inMemory.put(paxosID, logIndex.setGCSlot(gcSlot));
		}

		synchronized LogIndex getLogIndex(String paxosID, int version) {
			LogIndex logIndex = this.inMemory.get(paxosID);
			return logIndex != null && logIndex.version == version ? logIndex
					: null;
		}

		synchronized String toString(String paxosID) {
			LogIndex logIndex = this.inMemory.get(paxosID);
			return logIndex != null ? logIndex.toString() : null;
		}

		synchronized LogIndex getLogIndex(String paxosID) {
			LogIndex logIndex = this.inMemory.get(paxosID);
			if (logIndex == null) {
				// restore from disk
			}
			return logIndex;
		}

		synchronized String getMinLogfile(String paxosID) {
			LogIndex logIndex = this.inMemory.get(paxosID);
			return logIndex != null ? logIndex.getMinLogfile() : null;
		}
	}

	/*
	 * This MessageLog structure is no longer used and has been replaced with
	 * MessageLogDiskMap that is a more general-purpose pausable hash map.
	 */
	@Deprecated
	static class MessageLogPausable extends MessageLogDiskMap {
		MultiArrayMap<String, LogIndex> logIndexes = new MultiArrayMap<String, LogIndex>(
				Config.getGlobalInt(PC.PINSTANCES_CAPACITY));
		ConcurrentHashMap<String,LogIndex> pauseQ = new ConcurrentHashMap<String,LogIndex>();
		
		final Diskable<String,LogIndex> disk;
		private static final long IDLE_THRESHOLD = Config.getGlobalLong(PC.DEACTIVATION_PERIOD);
		private static final long THRESHOLD_SIZE = 1024*1024;

		Timer pauser = new Timer(MessageLogPausable.class.getSimpleName());
		
		MessageLogPausable(Diskable<String,LogIndex> disk) {
			super(disk);
			this.disk = disk;
		}
		
		synchronized LogIndex getOrCreateIfNotExistsOrLower(String paxosID, int version) {
			LogIndex logIndex = this.getOrRestore(paxosID);
			if(logIndex==null || logIndex.version - version < 0)
				this.logIndexes.put(paxosID, logIndex = new LogIndex(paxosID, version));
			return logIndex!=null && logIndex.version == version ? logIndex : null;
		}

		synchronized void add(PaxosPacket msg, String logfile, long offset,
				int length) {
			//long t = System.nanoTime();
			LogIndex logIndex = this.getOrCreateIfNotExistsOrLower(
					msg.getPaxosID(), msg.getVersion());
			if(logIndex==null) return;

			boolean isPValue = msg instanceof PValuePacket;
			logIndex.add(isPValue ? ((PValuePacket) msg).slot
					: ((PreparePacket) msg).firstUndecidedSlot,
					isPValue ? ((PValuePacket) msg).ballot.ballotNumber
							: ((PreparePacket) msg).ballot.ballotNumber,
					isPValue ? ((PValuePacket) msg).ballot.coordinatorID
							: ((PreparePacket) msg).ballot.coordinatorID, msg
							.getType().getInt(), logfile, offset, length);
			//if (Util.oneIn(10)) DelayProfiler.updateDelayNano("logAddDelay", t);
		}

		synchronized void setGCSlot(String paxosID, int version, int gcSlot) {
			LogIndex logIndex = this.getOrCreateIfNotExistsOrLower(paxosID, version);
			if(logIndex!=null)
				logIndex.setGCSlot(gcSlot);
		}

		synchronized LogIndex getLogIndex(String paxosID, int version) {
			LogIndex logIndex = this.getOrRestore(paxosID);
			return logIndex != null && logIndex.version == version ? logIndex
					: null;
		}

		synchronized String toString(String paxosID) {
			LogIndex logIndex = this.getOrRestore(paxosID);
			return logIndex != null ? logIndex.toString() : null;
		}

		synchronized LogIndex getLogIndex(String paxosID) {
			return this.getOrRestore(paxosID);
		}

		synchronized String getMinLogfile(String paxosID) {
			LogIndex logIndex = this.getOrRestore(paxosID);
			return logIndex != null ? logIndex.getMinLogfile() : null;
		}

		synchronized void uncache(String paxosID) {
			this.logIndexes.remove(paxosID);
		}

		synchronized void restore(LogIndex logIndex) {
			assert (logIndex != null);
			this.logIndexes.putIfAbsent(logIndex.paxosID, logIndex);
		}
		
		void deactivate() throws IOException {
			LogIndex logIndex = null;
			for (Iterator<LogIndex> strIter = this.logIndexes
					.concurrentIterator(); strIter.hasNext();)
				if (System.currentTimeMillis()
						- (logIndex = strIter.next()).getLastActive() > IDLE_THRESHOLD)
					this.pauseQ.put(logIndex.paxosID, logIndex);
			/*
			 * Synchronized because otherwise a put can happen in between the
			 * commit and the remove causing the remove to remove a more recent
			 * value. The only way to not have the commit inside the
			 * synchronized block is to verify if the serialized form of what
			 * was written to disk is the same as the serialized form of the
			 * current value. Unclear if this is much of a net 
			 */
				// first commit, then remove
			this.disk.commit(this.pauseQ);
			for (String paxosID : this.pauseQ.keySet())
				synchronized (this) {
					if (this.logIndexes.containsKey(paxosID)
							&& (System.currentTimeMillis()
									- this.logIndexes.get(paxosID)
											.getLastActive() > IDLE_THRESHOLD))
						this.logIndexes.remove(paxosID);
				}
		}
		
		LogIndex getOrRestore(String paxosID) {
			tryPause();
			if (!this.logIndexes.containsKey(paxosID)) {
				LogIndex logIndex = null;
				try {
					logIndex = this.disk.restore(paxosID);
				} catch (IOException e) {
					e.printStackTrace();
				}
				synchronized (this) {
					if (logIndex != null
							&& !this.logIndexes.containsKey(paxosID)) {
						this.logIndexes.put(paxosID, logIndex);
					}
				}
			}
			return this.logIndexes.get(paxosID);
		}
		
		void tryPause() {
			if(this.logIndexes.size() < THRESHOLD_SIZE) return;
			this.pauser.schedule(new TimerTask() {

				@Override
				public void run() {
					try {
						deactivate();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				
			}, 0);
		}
		
		public String toString() {
			return this.disk.toString();
		}
		
		public void close() {
			this.pauser.cancel();
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		SQLPaxosLogger logger = new SQLPaxosLogger(23, null, null);

		int[] group = { 32, 43, 54 };
		String paxosID = "paxos";
		int slot = 0;
		int ballotnum = 1;
		int coordinator = 2;
		String state = "Hello World";
		Ballot ballot = new Ballot(ballotnum, coordinator);
		logger.putCheckpointState(paxosID, 0, group, slot, ballot, state, 0);
		logger.putCheckpointState(paxosID, 0, group, slot, ballot, state, 0);

		assert (logger.isInserted(paxosID, group, slot, ballot, state));
		logger.copyEpochFinalCheckpointState(paxosID, 0);
		assert (logger.isInserted(logger.getPCTable(), paxosID, group, slot,
				ballot, state));
		DecimalFormat df = new DecimalFormat("#.##");

		int million = 1000000;
		int size = 1024*8;// (int)(0.001*million);

		boolean testBatchedCPs = false;
		double avg_write_time = logger.createCheckpoints(size, testBatchedCPs);
		System.out.println("Average time to write " + size + " checkpoints = "
				+ df.format(avg_write_time));
		double avg_read_time = logger.readCheckpoints(size);
		System.out.println("Average time to read " + size + " checkpoints = "
				+ df.format(avg_read_time) + "ms");

		//size = size*16;
		long t = System.nanoTime();
		for(int i=0; i<size; i++) {
			logger.readCheckpoints(1);
			logger.createCheckpoints(1);
		}
		System.out.println("avg_rw_time = " + (System.nanoTime()-t)/size+"ns");
		
		try {
			int numPackets = 65536;
			System.out.println("\nStarting " + numPackets + " message logs: ");

			PaxosPacket[] packets = new PaxosPacket[numPackets];
			long time = 0;
			int i = 0;
			String reqValue = "26";
			int nodeID = coordinator;
			int k = 1;
			for (int j = 0; j < packets.length; j++) {
				RequestPacket req = new RequestPacket(0, reqValue, false);
				ProposalPacket prop = new ProposalPacket(i, req);
				PValuePacket pvalue = new PValuePacket(ballot, prop);
				AcceptPacket accept = new AcceptPacket(nodeID, pvalue, -1);
				pvalue = pvalue.makeDecision(-1);
				PreparePacket prepare = new PreparePacket(new Ballot(i,
						ballot.coordinatorID));
				if (j % 3 == 0) { // prepare
					packets[j] = prepare;
				} else if (j % 3 == 1) { // accept
					// accept.setCreateTime(0);
					packets[j] = accept;
				} else if (j % 3 == 2) { // decision
					// pvalue.setCreateTime(0);
					packets[j] = pvalue;
				}
				if (j % 3 == 2)
					i++;
				packets[j].putPaxosID(paxosID, 0);
				long t1 = System.currentTimeMillis();
				long t2 = System.currentTimeMillis();
				time += t2 - t1;
				if (j > 0 && ((j + 1) % k == 0 || j % million == 0)) {
					System.out.println("[" + j + " : "
							+ df.format(((double) time) / j) + "ms] ");
					k *= 2;
				}
			}

			long t1 = System.currentTimeMillis();
			LogMessagingTask[] lmTasks = new LogMessagingTask[packets.length];
			assert (packets.length == lmTasks.length);
			for (int j = 0; j < lmTasks.length; j++) {
				lmTasks[j] = new LogMessagingTask(packets[j]);
			}
			logger.logBatchDB(lmTasks);
			long logTime = System.currentTimeMillis() - t1;
			System.out.println("Average log time = "
					+ df.format((logTime) * 1.0 / packets.length));

			System.out.print("Checking logged messages...");
			for (int j = 0; j < packets.length; j++) {
				if (j % 100 == 0)
					System.out.print(j + " ");
			}
			System.out.println("checked");

			int newSlot = 200;
			int gcSlot = 100;
			Ballot newBallot = new Ballot(0, 2);
			SQLPaxosLogger.setLogGCFrequency(0);
			logger.putCheckpointState(paxosID, 0, group, newSlot, newBallot,
					"Hello World", gcSlot);
			Thread.sleep(2000);
			System.out
					.println("Invoking initiateReadCheckpoints after checkpointing:");
			logger.initiateReadCheckpoints(true);
			RecoveryInfo pri = null;
			while ((pri = logger.readNextCheckpoint(true)) != null) {
				assert (pri != null);
			}
			System.out.print("Checking deletion of logged messages...");
			for (int j = 0; j < packets.length; j++) {
				int[] sbc = AbstractPaxosLogger.getSlotBallot(packets[j]);
				PaxosPacket.PaxosPacketType type = (packets[j].getType());
				if (type == PaxosPacket.PaxosPacketType.ACCEPT) {
					if (sbc[0] < gcSlot)
						assert (!logger.isLogged(packets[j])) : sbc[0] + " : " + gcSlot;
					else
						assert (SQLPaxosLogger.getLogMessageBlobOption() || logger
								.isLogged(packets[j]));
				} else if (type == PaxosPacket.PaxosPacketType.PREPARE) {
					if ((sbc[1] < newBallot.ballotNumber || (sbc[1] == newBallot.ballotNumber && sbc[2] < newBallot.coordinatorID))) {
						assert (!logger.isLogged(packets[j])) : packets[j]
								.toString();
					} else
						assert (SQLPaxosLogger.getLogMessageBlobOption() || logger
								.isLogged(packets[j]));
				} else if (type == PaxosPacket.PaxosPacketType.DECISION) {
					if (sbc[0] < newSlot - MAX_OLD_DECISIONS)
						assert (!logger.isLogged(packets[j]));
					else
						assert (SQLPaxosLogger.getLogMessageBlobOption() || logger
								.isLogged(packets[j]));
				}
			}
			System.out.println("checked");
			logger.closeGracefully();
			System.out
					.println("SUCCESS: No exceptions or assertion violations were triggered. "
							+ "Average log time over "
							+ numPackets
							+ " packets = "
							+ df.format(((double) logTime) / numPackets)
							+ " ms");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
