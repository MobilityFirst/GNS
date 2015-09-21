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
import edu.umass.cs.gigapaxos.paxosutil.LogMessagingTask;
import edu.umass.cs.gigapaxos.paxosutil.Messenger;
import edu.umass.cs.gigapaxos.paxosutil.RecoveryInfo;
import edu.umass.cs.gigapaxos.paxosutil.SQL;
import edu.umass.cs.gigapaxos.paxosutil.SlotBallotState;
import edu.umass.cs.gigapaxos.paxosutil.StringContainer;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.Util;
import edu.umass.cs.utils.DelayProfiler;

import org.json.JSONException;

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
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;
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
	 *  Disable persistent logging altogether
	 */
	private static boolean disableLogging = Config
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
	/**
	 * Truncated checkpoint state size for java logging purposes
	 */
	private static final int TRUNCATED_STATE_SIZE = 20;
	private static final int MAX_OLD_DECISIONS = PaxosInstanceStateMachine.INTER_CHECKPOINT_INTERVAL;
	/**
	 * Needed for testing with recovery in the same JVM
	 */
	private static final boolean DONT_SHUTDOWN_EMBEDDED = true;

	private static final int MAX_FILENAME_SIZE = 512;

	/**
	 * Batching can make log messages really big, so we need a maximum
	 * size here to ensure that we don't try to batch more than we can
	 * chew.
	 */
	private static int maxLogMessageSize = MAX_LOG_MESSAGE_SIZE;

	private static boolean getLogMessageClobOption() {
		return (maxLogMessageSize > SQL.getVarcharSize(SQL_TYPE))
				|| Config.getGlobalBoolean(PC.BATCHING_ENABLED);
	}

	private static int maxCheckpointSize = MAX_CHECKPOINT_SIZE;

	private static boolean getCheckpointClobOption() {
		return (maxCheckpointSize > SQL.getVarcharSize(SQL_TYPE));
	}

	// FIXME: Replace field name string constants with enums
	@SuppressWarnings("unused")
	private static enum Columns {
		PAXOS_ID,
		VERSION,
		SLOT,
		BALLOTNUM,
		COORDINATOR,
		PACKET_TYPE,
		STATE,
		LOGFILE,
		OFFSET,
		LENGTH,
		MESSAGE,
	};

	private static final ArrayList<SQLPaxosLogger> instances = new ArrayList<SQLPaxosLogger>();

	private ComboPooledDataSource dataSource = null;
	private Connection defaultConn = null;
	private Connection cursorConn = null;
	
	private final Journaler journaler;

	private boolean closed = true;

	protected static boolean isLoggingEnabled() {
		return !disableLogging;
	}

	protected static boolean disableLogging() {
		return disableLogging && (disableLogging = true);
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
	
	private final Timer GC;
	
	private static Logger log = PaxosManager.getLogger();

	SQLPaxosLogger(int id, String strID, String dbPath, Messenger<?> messenger) {
		super(id, dbPath, messenger);
		this.strID = strID;
		GC = new Timer(strID);
		addDerbyLogger(this);
		this.journaler = new Journaler(this.logDirectory, this.myID);
		initialize(); // will set up db, connection, tables, etc. as needed
	}

	SQLPaxosLogger(int id, String dbPath, Messenger<?> messenger) {
		this(id, "" + id, dbPath, messenger);

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
		if (isClosed() || !isLoggingEnabled())
			return true;

		boolean copied = false;
		// Stupid derby doesn't have an insert if not exist command
		String insertCmd = "insert into "
				+ getPCTable()
				+ " (version,members,slot,ballotnum,coordinator,state,createtime,paxos_id) values (?,?,?,?,?,?,?,?)";
		String updateCmd = "update "
				+ getPCTable()
				+ " set version=?,members=?, slot=?, ballotnum=?, coordinator=?, state=?, createtime=? where paxos_id=?";
		String cmd = this.existsRecord(getPCTable(), paxosID) ? updateCmd
				: insertCmd;
		String readCmd = "select version, members, slot, ballotnum, coordinator, state, createtime from "
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
				if (getCheckpointClobOption()) {
					// insertCP.setClob(6, new
					// StringReader(cpRecord.getString("state")));
					insertCP.setBlob(7, cpRecord.getBlob("state"));
				} else
					insertCP.setString(6, cpRecord.getString("state"));
				insertCP.setLong(7, cpRecord.getLong("createtime"));
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
										(getCheckpointClobOption() ? new String(
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
		private String curLogfile=null;
		private FileOutputStream fos;
		private long curLogfileSize = 0;
		private Object fosLock = new Object();

		Journaler(String logdir, int myID) {
			this.myID = myID;
			this.logdir = logdir;
			this.logfilePrefix = PREFIX+myID+POSTPREFIX;
			assert(this.logdir!=null && this.logfilePrefix!=null);
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
			return this.logdir + this.logfilePrefix + System.currentTimeMillis();
		}
		private FileOutputStream createLogfile(String filename, boolean deleteEmpty) {
			assert(this.logdir!=null && this.logfilePrefix!=null);
			if(deleteEmpty) this.deleteEmptyLogfiles();
			try {
				new File(filename).getParentFile().mkdirs();
				(new FileWriter(filename, false)).close();
				this.fos =  new FileOutputStream(new File(filename));
				this.curLogfileSize = 0;
				log.info(this + " created new log file " + this.curLogfile);
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
		public String toString() {
			return this.getClass().getSimpleName()+this.myID;
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
			if(emptyFiles!=null) 
				for (File f : emptyFiles)
					f.delete();
		}
		
		private void rollLogFile() {
			synchronized (fosLock) {
				// check again here
				if (curLogfileSize > MAX_LOG_FILE_SIZE) {
					try {
						fos.close();
						fos = createLogfile(curLogfile = generateLogfileName());
						curLogfileSize = 0;
					} catch (IOException e) {
						log.severe(this + " unable to close existing log file "
								+ this.curLogfile);
						e.printStackTrace();
					} finally {
						if (fos == null) {
							log.severe(this + " unable to open log file "
									+ this.curLogfile);
							System.exit(1); // can not proceed further
						}
					}
				}
			}
		}
		
		private void appendToLogFile(byte[] bytes) throws IOException {
			synchronized (fosLock) {
				fos.write(JOURNAL_COMPRESSION ? deflate(bytes) : bytes);
				curLogfileSize += bytes.length;
			}
		}

	}
	
	private static final int MAX_LOG_FILE_SIZE = 64 * 1024 * 1024;



	/*
	 * Deletes all but the most recent checkpoint for the RC group name. We
	 * could track recency based on timestamps using either the timestamp in the
	 * filename or the OS file creation time. Here, we just supply the latest
	 * checkpoint filename explicitly as we know it when this method is called
	 * anyway.
	 */
	private static boolean deleteOldCheckpoints(final String cpDir, final String rcGroupName, int keep, Object lockMe) {
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
		synchronized(lockMe) {
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
	private static Set<Filename> getLatest(File[] files, int numLatest) {
		TreeSet<Filename> allFiles = new TreeSet<Filename>();
		TreeSet<Filename> oldFiles = new TreeSet<Filename>();
		for (File file : files)
			allFiles.add(new Filename(file));
		if (allFiles.size() <= numLatest)
			return oldFiles;
		Iterator<Filename> iter = allFiles.descendingIterator();
		for (int i = 0; i < numLatest; i++)
			oldFiles.add(iter.next());

		return oldFiles;
	}

	private static class Filename implements Comparable<Filename> {
		final File file;

		Filename(File f) {
			this.file = f;
		}

		@Override
		public int compareTo(SQLPaxosLogger.Filename o) {
			long t1 = file.lastModified();
			long t2 = o.file.lastModified();
			if (t1 < t2)
				return -1;
			else if (t1 == t2)
				return 0;
			else
				return 1;
		}
	}
	
	private static final byte[] testBytes = new byte[2000*1000];
	static {
		for(int i=0; i<testBytes.length; i++) testBytes[i] = (byte)(-256 + (int)(Math.random()*256)); 
	}

	private long[] journal(LogMessagingTask[] packets) {
		if(!ENABLE_JOURNALING) return new long[0]; // no error
		if (this.journaler.fos == null)
			return new long[0]; // error
		boolean amCoordinator = false, isAccept = false;
		long[] offsets = new long[packets.length];
		for (int i=0; i<packets.length; i++) {
			LogMessagingTask pkt = packets[i];
			amCoordinator = pkt.logMsg instanceof PValuePacket ? ((PValuePacket) pkt.logMsg).ballot.coordinatorID == myID
					: pkt.logMsg instanceof PreparePacket ? ((PreparePacket) pkt.logMsg).ballot.coordinatorID == myID : false;
			isAccept = pkt.logMsg.getType() == PaxosPacketType.ACCEPT; // else decision
			if(DONT_LOG_DECISIONS && !isAccept) continue;
			if(NON_COORD_ONLY && amCoordinator && !COORD_STRINGIFIES_WO_JOURNALING) continue;
			if (COORD_ONLY && !amCoordinator) continue;
			if(NON_COORD_DONT_LOG_DECISIONS && !amCoordinator && !isAccept) continue;
			if(COORD_DONT_LOG_DECISIONS && amCoordinator && !isAccept) continue;
			
			try {
				{
					byte[] bytes = !NO_STRINGIFY_JOURNALING
							&& !(COORD_JOURNALS_WO_STRINGIFYING && amCoordinator) ? toBytes(pkt.logMsg)
							: Arrays.copyOf(testBytes,
									((RequestPacket) pkt.logMsg)
											.lengthEstimate());
							
					// format: <size><message>*
					ByteBuffer bbuf = null;
					bbuf = ByteBuffer.allocate(4 + bytes.length);
					bbuf.putInt(bytes.length);
					bbuf.put(bytes);
					bytes = bbuf.array();
					
					offsets[i] = this.journaler.curLogfileSize;
							
					if (!STRINGIFY_WO_JOURNALING
							&& !(COORD_STRINGIFIES_WO_JOURNALING && amCoordinator))
						SQLPaxosLogger.this.journaler.appendToLogFile(bytes);
				}

			} catch (IOException ioe) {
				ioe.printStackTrace();
				return null;
			}
		}
		if (this.journaler.curLogfileSize > MAX_LOG_FILE_SIZE) {
			this.GC.schedule(new TimerTask() {
				@Override
				public void run() {
					try {
						// always commit pending before rolling log file
						log.log(Level.FINE,
								"{0} rolling log file {1}",
								new Object[] {
								SQLPaxosLogger.this.journaler,
										SQLPaxosLogger.this.journaler.curLogfile });
						SQLPaxosLogger.this.commitPendingLogMessages();
						SQLPaxosLogger.this.journaler.rollLogFile();
						if (!INDEX_JOURNAL && !SYNC_INDEX_JOURNAL)
							// used only during testing
							SQLPaxosLogger
									.deleteOldCheckpoints(
											logDirectory,
											SQLPaxosLogger.this.journaler.logfilePrefix,
											5, this);
						if (Util.oneIn(10))
							SQLPaxosLogger.this.garbageCollectJournal();
					} catch(Exception e) {
						log.severe(this + " incurred exception " + e.getMessage());
						e.printStackTrace();
					}
				}
			}, 0);
		}
		return offsets;
	}

	// kept around for performance testing of different stringification hacks
	protected byte[] toBytes(PaxosPacket packet) throws UnsupportedEncodingException {
		return toString(packet).getBytes(CHARSET);
	}

	private String toString(PaxosPacket packet) {
		return packet.toString();
	}

	// various options for performance testng below
	private static final boolean ENABLE_JOURNALING=Config.getGlobalBoolean(PC.ENABLE_JOURNALING);
	private static final boolean STRINGIFY_WO_JOURNALING=Config.getGlobalBoolean(PC.STRINGIFY_WO_JOURNALING);
	private static final boolean NON_COORD_ONLY=Config.getGlobalBoolean(PC.NON_COORD_ONLY);
	private static final boolean COORD_ONLY=Config.getGlobalBoolean(PC.NON_COORD_ONLY);
	private static final boolean NO_STRINGIFY_JOURNALING=Config.getGlobalBoolean(PC.NO_STRINGIFY_JOURNALING);
	private static final boolean COORD_STRINGIFIES_WO_JOURNALING=Config.getGlobalBoolean(PC.COORD_STRINGIFIES_WO_JOURNALING);
	private static final boolean COORD_JOURNALS_WO_STRINGIFYING=Config.getGlobalBoolean(PC.COORD_JOURNALS_WO_STRINGIFYING);
	private static final boolean DONT_LOG_DECISIONS=Config.getGlobalBoolean(PC.DONT_LOG_DECISIONS);
	private static final boolean NON_COORD_DONT_LOG_DECISIONS=Config.getGlobalBoolean(PC.NON_COORD_DONT_LOG_DECISIONS);
	private static final boolean COORD_DONT_LOG_DECISIONS=Config.getGlobalBoolean(PC.COORD_DONT_LOG_DECISIONS);
	private static final boolean JOURNAL_COMPRESSION=Config.getGlobalBoolean(PC.JOURNAL_COMPRESSION);
	private static final boolean SYNC_INDEX_JOURNAL=Config.getGlobalBoolean(PC.SYNC_INDEX_JOURNAL);
	private static final boolean INDEX_JOURNAL=Config.getGlobalBoolean(PC.INDEX_JOURNAL);
	private static final int LOG_INDEX_FREQUENCY=Config.getGlobalInt(PC.LOG_INDEX_FREQUENCY);
	
	private static final int MAX_BATCH_SIZE = 10000;

	/*
	 * A wrapper to select between the purely DB-based logger and the
	 * work-in-progress journaling logger.
	 */
	@Override
	public boolean logBatch(final LogMessagingTask[] packets) {
		if (isClosed())
			return false;
		if (!isLoggingEnabled() && !ENABLE_JOURNALING) 
			return true;
		if (isLoggingEnabled()) 
			// no need to journal and the file, offset have no meaning here
			return this.logBatchDB(packets);
		
		// else journaling but no DB logging
		String journalFile = this.journaler.curLogfile;
		long[] offsets = null;
		boolean journaled = (ENABLE_JOURNALING && (offsets = this
				.journal(packets)) != null);
		String[] journalFiles = new String[packets.length];
		for(int i=0; i<packets.length; i++) journalFiles[i] = journalFile;
		
		// can asynchronously log to DB as already journaled synchronously
		if (SYNC_INDEX_JOURNAL)
			return journaled && logBatchDB(packets, journalFiles, offsets);
		else if(INDEX_JOURNAL && journaled) {
			final long[] finalOffsets = offsets;
			for (int i = 0; i < packets.length; i++)
				SQLPaxosLogger.this.pendingLogMessages.add(new PendingLogTask(
						packets[i], journalFile, finalOffsets[i]));
			log.log(Level.FINER, "{0} has {1} pending log messages",
					new Object[] { this, this.pendingLogMessages.size() });
			// FIXME: needed given we index upon rolling anyway?
			if (Util.oneIn(LOG_INDEX_FREQUENCY))
				this.GC.schedule(new TimerTask() {
					@Override
					public void run() {
						try {
							SQLPaxosLogger.this.commitPendingLogMessages();
						} catch(Exception e) {
							log.severe(this + " incurred exception " + e.getMessage());
							e.printStackTrace();
						}
					}
				}, 0);
		}
		// else no indexing of journal
		return journaled;
	}
		
	private LinkedBlockingQueue<PendingLogTask> pendingLogMessages = new LinkedBlockingQueue<PendingLogTask>();

	private boolean logBatchDB(PendingLogTask[] packets) {

		String[] journalFiles = new String[packets.length];
		long[] journalFileOffsets = new long[packets.length];
		LogMessagingTask[] lmTasks = new LogMessagingTask[packets.length];
		for (int i = 0; i < packets.length; i++) {
			lmTasks[i] = packets[i].lmTask;
			journalFiles[i] = packets[i].logfile;
			journalFileOffsets[i] = packets[i].logfileOffset;
		}
		log.log(Level.FINE, "{0} committing {1} pending log tasks from {2}",
				new Object[] { this, packets.length,
						this.journaler.curLogfile });
		return this.logBatchDB(lmTasks, journalFiles, journalFileOffsets);
	}

	// latches meaningless journal files and offsets
	private boolean logBatchDB(LogMessagingTask[] packets) {
		String[] journalFiles = new String[packets.length];
		long[] journalFileOffsets = new long[packets.length];
		for(int i=0; i<packets.length; i++) {
			journalFiles[i] =  this.journaler.curLogfile;
			journalFileOffsets[i] = this.journaler.curLogfileSize;
		}
		return this.logBatchDB(packets, journalFiles, journalFileOffsets);
	}

	/* The main method to log to the DB. If journaling is enabled, this method is 
	 * always called after journaling; in that case, this method performs indexing.
	 */
	private synchronized boolean logBatchDB(LogMessagingTask[] packets,
			String[] journalFiles, long[] journalFileOffsets) {
		assert(packets.length == journalFiles.length && journalFiles.length == journalFileOffsets.length);
		if (isClosed())
			return false;
		long[] offsets = journalFileOffsets;
		if (!isLoggingEnabled() && !ENABLE_JOURNALING) 
			return true;
			;
		
		boolean logged = true;
		PreparedStatement pstmt = null;
		Connection conn = null;
		String cmd = "insert into " + getMTable()
				+ " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		long t1 = System.currentTimeMillis();
		int i = 0;
		try {
			for (i = 0; i < packets.length; i++) {
				if (conn == null) {
					conn = this.getDefaultConn();
					// conn.setAutoCommit(false);
					pstmt = conn.prepareStatement(cmd);
				}
				PaxosPacket packet = packets[i].logMsg;
				// accept and decision use a faster implementation
				int[] sb = AbstractPaxosLogger.getSlotBallot(packet);

				pstmt.setString(1, packet.getPaxosID());
				pstmt.setInt(2, packet.getVersion());
				pstmt.setInt(3, sb[0]);
				pstmt.setInt(4, sb[1]);
				pstmt.setInt(5, sb[2]);
				pstmt.setInt(6, packet.getType().getInt());
				pstmt.setString(7, journalFiles[i]);
				pstmt.setLong(8, offsets!=null ? offsets[i] : 0);

				byte[] msgBytes = toBytes(packet);

				if (getLogMessageClobOption()) {
					byte[] compressed = isLoggingEnabled() ? deflate(msgBytes)
							: new byte[0];
					pstmt.setInt(9, msgBytes.length);
					Blob blob = conn.createBlob();
					blob.setBytes(1, compressed);
					pstmt.setBlob(10, blob);
				} else {
					assert(false); // unused 
					String packetString = packet.toString();
					pstmt.setInt(9, packetString.length());
					pstmt.setString(10, packetString);
				}
				
				pstmt.addBatch();
				if ((i + 1) % MAX_BATCH_SIZE == 0 || (i + 1) == packets.length) {
					int[] executed = pstmt.executeBatch();
					// conn.commit();
					pstmt.clearBatch();
					for (int j : executed)
						logged = logged && (j > 0);
					if (logged)
						log.log(Level.FINE,
								"{0}{1}{2}{3}{4}{5}",
								new Object[] { this,
										" successfully logged the " + "last ",
										(i + 1), " messages in ",
										(System.currentTimeMillis() - t1),
										" ms" });
					t1 = System.currentTimeMillis();
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

		return logged;// && journaled;
	}

	/**
	 * Encoding used by the logger.
	 */
	public static final String CHARSET = "ISO-8859-1";

	private static final boolean DB_COMPRESSION = Config.getGlobalBoolean(PC.DB_COMPRESSION);
	/**
	 * @param data
	 * @return Compressed form.
	 * @throws IOException
	 */
	public static byte[] deflate(byte[] data) throws IOException {
		if(!DB_COMPRESSION) return data;
		Deflater deflator = new Deflater();
		byte[] compressed = new byte[data.length];
		int compressedLength = data.length;
		deflator.setInput(data);
		deflator.finish();
		compressedLength = deflator.deflate(compressed);
		assert (compressedLength <= data.length);
		deflator.end();
		//if(Util.oneIn(10)) DelayProfiler.updateMovAvg("deflation", data.length*1.0/compressedLength);
		return Arrays.copyOf(compressed, compressedLength);
	}

	/**
	 * @param buf
	 * @return Uncompressed form.
	 * @throws IOException
	 */
	public static byte[] inflate(byte[] buf) throws IOException {
		if(!DB_COMPRESSION) return buf;
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
	public void putCheckpointState(final String paxosID, final int version,
			final Set<String> group, final int slot, final Ballot ballot, final String state,
			final int acceptedGCSlot, final long createTime) {
		if (isClosed() || (!isLoggingEnabled() && !ENABLE_JOURNALING))
			return;

		long t1 = System.currentTimeMillis();
		// Stupid derby doesn't have an insert if not exist command
		String insertCmd = "insert into "
				+ getCTable()
				+ " (version,members,slot,ballotnum,coordinator,state,createtime,paxos_id) values (?,?,?,?,?,?,?,?)";
		String updateCmd = "update "
				+ getCTable()
				+ " set version=?,members=?, slot=?, ballotnum=?, coordinator=?, state=?, createTime=? where paxos_id=?";
		boolean existingCP = this.existsRecord(getCTable(), paxosID);
		String cmd = existingCP ? updateCmd : insertCmd;
		PreparedStatement insertCP = null;
		Connection conn = null;
		try {
			conn = this.getDefaultConn();
			insertCP = conn.prepareStatement(cmd);
			insertCP.setInt(1, version);
			insertCP.setString(2, Util.toJSONString(group));
			insertCP.setInt(3, slot);
			insertCP.setInt(4, ballot.ballotNumber);
			insertCP.setInt(5, ballot.coordinatorID);
			if (getCheckpointClobOption()) {
				// insertCP.setClob(6, state != null ? new StringReader(state) :
				// null);
				Blob blob = conn.createBlob();
				blob.setBytes(1, state.getBytes(CHARSET));
				insertCP.setBlob(6, blob);
			} else
				insertCP.setString(6, state);
			insertCP.setLong(7, createTime);
			insertCP.setString(8, paxosID);
			insertCP.executeUpdate();
			// conn.commit();
			incrTotalCheckpoints();

			DelayProfiler.updateDelay("checkpoint", t1);
			// why can't insertCP.toString() return the query string? :/
			log.log(shouldLogCheckpoint() ? Level.INFO : Level.FINE,
					"{0} checkpointed ({1}:{2}, {3}{4}, {5}, ({6}) [{7}]) in {8} ms",
					new Object[] {
							this,
							paxosID,
							version,
							Util.toJSONString(group).substring(0, 0),
							slot,
							ballot,
							acceptedGCSlot,
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

		/*
		 * Delete logged messages from before the checkpoint. Note: Putting this
		 * before cleanup(conn) above can cause deadlock if we don't have at
		 * least 2x the number of connections as concurrently active paxosIDs.
		 * Realized this the hard way. :)
		 */
		if(Util.oneIn(Config.getGlobalInt(PC.LOG_GC_FREQUENCY))) {
			this.GC.schedule(new TimerTask() {

				@Override
				public void run() {
					try {
						Thread.currentThread().setPriority(Thread.MIN_PRIORITY);
						long t = System.currentTimeMillis();
						SQLPaxosLogger.this.deleteOutdatedMessages(paxosID,
								slot, ballot.ballotNumber,
								ballot.coordinatorID, acceptedGCSlot);
						DelayProfiler.updateDelay("GC", t);
					} catch(Exception e) {
						log.severe(this + " incurred exception " + e.getMessage());
						e.printStackTrace();
					}
				}
			}, 0);
		}
	}

	private static int CHECKPOINT_LOG_THRESHOLD = 10000;
	private static int totalCheckpoints = 0;

	private synchronized static void incrTotalCheckpoints() {
		totalCheckpoints++;
	}

	private boolean shouldLogCheckpoint() {
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
	private void deleteOutdatedMessages(String paxosID, int slot,
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
			// Create delete command using the slot, ballot, and gcSlot
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
			// have to literally break it down for derby :(
			for (int i = 0; i < 3; i++) {
				pstmt = conn.prepareStatement(cmds[i]);
				pstmt.execute();
				pstmt.close();
			}

			// conn.commit();
			log.log(Level.FINE, "{0}{1}{2}", new Object[] { this,
					" DB deleted up to slot ", acceptedGCSlot });
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
			if (getLogMessageClobOption()) {
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

	/*
	 * Can not start pause or unpause after close has been called. For other
	 * operations like checkpointing or logging, we need to be able to do them
	 * even after close has been called as waitToFinish needs that.
	 */
	@Override
	public synchronized boolean pause(String paxosID, String serializedState) {
		if (isClosed() || !isLoggingEnabled())
			return false;

		boolean paused = false;
		String insertCmd = "insert into " + getPTable()
				+ " (serialized, paxos_id) values (?,?)";
		String cmd = insertCmd; // this.unpause(paxosID)!=null ? updateCmd :
								// insertCmd;
		PreparedStatement insertP = null;
		Connection conn = null;
		try {
			conn = this.getDefaultConn();
			insertP = conn.prepareStatement(cmd);
			insertP.setString(1, serializedState);
			insertP.setString(2, paxosID);
			insertP.executeUpdate();
			paused = true;
		} catch (SQLException sqle) {
			log.severe(this + " failed to pause instance " + paxosID);
			this.deletePaused(paxosID);
			sqle.printStackTrace();
		} finally {
			cleanup(insertP);
			cleanup(conn);
		}
		;
		return paused;
	}

	@Override
	public synchronized HotRestoreInfo unpause(String paxosID) {
		if (isClosed() || !isLoggingEnabled())
			return null;

		HotRestoreInfo hri = null;
		PreparedStatement pstmt = null;
		ResultSet rset = null;
		Connection conn = null;
		try {
			conn = this.getDefaultConn();
			pstmt = this.getPreparedStatement(conn, getPTable(), paxosID,
					"serialized");
			rset = pstmt.executeQuery();
			while (rset.next()) {
				assert (hri == null); // exactly onece
				String serialized = rset.getString(1); // no clob option
				hri = new HotRestoreInfo(serialized);
			}
		} catch (SQLException sqle) {
			log.severe(this + " failed to pause instance " + paxosID);
			sqle.printStackTrace();
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
		if (isClosed() || !isLoggingEnabled())
			return;
		PreparedStatement pstmt = null;
		ResultSet rset = null;
		Connection conn = null;
		try {
			conn = this.getDefaultConn();
			pstmt = conn.prepareStatement("delete from " + getPTable()
					+ " where paxos_id='" + paxosID + "'");
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
				state = (!getCheckpointClobOption() || !column.equals("state") ? stateRS
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
							"slot, ballotnum, coordinator, state, version, createTime, members");

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
							(!getCheckpointClobOption() ? stateRS.getString(4)
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
				String state = (readState ? (!getCheckpointClobOption() ? cursorRset
						.getString(4) : lobToString(cursorRset.getBlob(4)))
						: null);
				pri = new RecoveryInfo(paxosID, version, pieces, state);
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
		if(isLoggingEnabled()) 
			try {
				this.cursorPstmt = this.getPreparedStatement(
						this.getCursorConn(), getMTable(), null, "message");
				this.cursorRset = this.cursorPstmt.executeQuery();
				initiated = true;
			} catch (SQLException sqle) {
				log.severe("SQLException while getting all paxos IDs " + " : "
						+ sqle);
			}
		else if(ENABLE_JOURNALING) {
			logfiles = (new File(this.logDirectory)).listFiles(new FileFilter() {
				@Override
				public boolean accept(File pathname) {
							return pathname.isFile()
									&& pathname.toString().startsWith(
											SQLPaxosLogger.this.journaler
													.getLogfilePrefix());
				}
			});
			if (logfiles != null && logfiles.length > 0)
				try {
					curRAF = new RandomAccessFile(logfiles[0], "r");
					log.log(Level.INFO,
							"{0} rolling forward logged messages from file {1}",
							new Object[] { this.journaler,
									this.logfiles[this.logfileIndex] });
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
		}
		return initiated;
	}

	/* This method used to return PaxosPacket earlier. We now return 
	 * a string because it may be necessary to fixNodeStringToInt on
	 * the corresponding json before conversion to PaxosPacket. 
	 */
	@Override
	public synchronized String readNextMessage() {
		String packetStr = null;
		if (isLoggingEnabled())
			try {
				if (cursorRset != null && cursorRset.next()) {
					String msg = (!getLogMessageClobOption() ? cursorRset
							.getString(1) : lobToString(cursorRset.getBlob(1)));
					packetStr = msg;
				}
			} catch (SQLException | IOException e) {
				log.severe(this + " got " + e.getClass().getSimpleName()
						+ " in readNextMessage while reading: " + " : "
						+ packetStr);
				e.printStackTrace();
			}
		else if (ENABLE_JOURNALING) {
			String latest = this.getLatestJournalFile();
			try {
				while (this.curRAF != null
						&& this.curRAF.getFilePointer() == this.curRAF.length()) {
					this.curRAF.close();
					this.curRAF=null;
					// move on to the next file
					if (this.logfileIndex + 1 < this.logfiles.length) 
						this.curRAF = new RandomAccessFile(
								this.logfiles[++this.logfileIndex], "r");
					if(this.curRAF!=null) 
						log.log(Level.INFO,
								"{0} rolling forward logged messages from file {1}",
								new Object[] { this.journaler, this.logfiles[this.logfileIndex] });
				}
				if (this.curRAF == null)
					return null;

				log.log(Level.FINEST,
						"{0} reading from offset {1} from file {2}",
						new Object[] { this, this.curRAF.getFilePointer(),
								this.logfiles[this.logfileIndex] });

				long prevOffset = this.curRAF.getFilePointer();
				int msgLength = this.curRAF.readInt();
				byte[] msg = new byte[msgLength];
				this.curRAF.read(msg);
				packetStr = new String(msg, CHARSET);
				
				if (latest != null
						&& this.logfiles[this.logfileIndex].toString().equals(
								latest))
					this.indexJournalEntryInDB(packetStr,
							this.logfiles[this.logfileIndex].toString(),
							prevOffset);
					
			} catch (IOException |JSONException e) {
				e.printStackTrace();
			}
		}
		return packetStr;
	}
	
	private void indexJournalEntryInDB(String packetStr, String journalFile,
			long offset) throws JSONException {
		PaxosPacket pp = this.packetizer != null ? packetizer
				.stringToPaxosPacket(packetStr) : PaxosPacket
				.getPaxosPacket(packetStr);
		if (pp != null) {
			LogMessagingTask[] lmTasks = { new LogMessagingTask(pp) };
			String[] journalFiles = {journalFile};
			long[] offsets = {offset};
			this.logBatchDB(lmTasks, journalFiles, offsets);
		}
	}

	public synchronized void closeReadAll() {
		log.log(Level.FINE, "{0}{1}", new Object[] { this,
				" invoking closeReadAll" });
		this.cleanupCursorConn();
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
	
	class PendingLogTask {
		final LogMessagingTask lmTask;
		final String logfile;
		final long logfileOffset;
		PendingLogTask(LogMessagingTask lmTask, String logfile, long offset) {
			this.lmTask = lmTask;
			this.logfile = logfile;
			this.logfileOffset = offset;
		}
	}

	private boolean commitPendingLogMessages() {
		return this.commitPendingLogMessages(null);
	}
	private boolean commitPendingLogMessages(String paxosID) {
		int prevSize = this.pendingLogMessages.size();
		if(prevSize==0) return true;
		
		ArrayList<PendingLogTask> pendingQ = new ArrayList<PendingLogTask>();
		for(Iterator<PendingLogTask> lmIter = this.pendingLogMessages.iterator(); lmIter.hasNext(); ) {
			PendingLogTask pending = lmIter.next();
			if(!pending.lmTask.isEmpty() && (paxosID==null || pending.lmTask.logMsg.getPaxosID().equals(paxosID)))
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
		assert(latest.size()<=1) : latest.size();
		return latest.size()==1 ? latest.toArray(new Filename[0])[0].file.toString() : null;
	}
	
	
	/*
	 * FIXME: Upon recovery, we need to commit pending log messages that were
	 * not sync'd to the DB. For this, it suffices to find the list of all
	 * logfiles that are equal to or more recent than the most recent logfile in
	 * the DB or pick the most recent logfile if there are no such files, and
	 * commit the messages in them to the DB.
	 */

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
		
		if (ENABLE_JOURNALING && !SYNC_INDEX_JOURNAL) 
			this.commitPendingLogMessages(paxosID);
		
		ArrayList<PaxosPacket> messages = new ArrayList<PaxosPacket>();
		
		PreparedStatement pstmt = null;
		ResultSet messagesRS = null;
		Connection conn = null;
		try {
			conn = this.getDefaultConn();
			pstmt = this.getPreparedStatement(conn, getMTable(), paxosID,
					"packet_type, message" + (ENABLE_JOURNALING ? ", logfile, offset, length" :""), fieldConstraints);
			messagesRS = pstmt.executeQuery();

			assert (!messagesRS.isClosed());
			while (messagesRS.next()) {
				assert(!ENABLE_JOURNALING || messagesRS.getString("logfile") != null);
								
				String logMsgStr = (!ENABLE_JOURNALING ? (!getLogMessageClobOption() ? messagesRS
						.getString("message") : lobToString(messagesRS.getBlob("message")))

						: this.getJournaledMessage(
								messagesRS.getString("logfile"),
								messagesRS.getLong("offset"),
								messagesRS.getInt("length")));

				PaxosPacket packet = this.packetizer != null ? packetizer
						.stringToPaxosPacket(logMsgStr) : PaxosPacket
						.getPaxosPacket(logMsgStr);
				// sanity check for DB-journal consistency
				assert (packet.getType().getInt() == messagesRS
						.getInt("packet_type"));
				messages.add(packet);
			}
		} catch (SQLException | JSONException | IOException e) {
			log.severe(e.getClass().getSimpleName()
					+ " while getting slot for " + paxosID);
			e.printStackTrace();
		} finally {
			cleanup(pstmt, messagesRS);
			cleanup(conn);
		}
		return messages;
	}
	
	private String getJournaledMessage(String logfile, long offset, int length)
			throws IOException {
		assert (logfile != null);
		RandomAccessFile raf = new RandomAccessFile(logfile, "r");
		raf.seek(offset);
		int readLength = raf.readInt();
		assert (readLength == length) : this + " : " + readLength + " != "
				+ length;
		byte[] buf = new byte[readLength];
		int nread = raf.read(buf);
		assert (nread == buf.length) : nread + " != " + buf.length
				+ " while reading from " + logfile + ":" + offset;
		raf.close();
		String msg = new String(buf, CHARSET);
		log.log(Level.FINEST,
				"{0} returning journaled message from {1}:{2}:{3} = [{4}]",
				new Object[] { this, logfile, offset, length, msg });
		return msg;
	}

	private void garbageCollectJournal() {
		if(!INDEX_JOURNAL && !SYNC_INDEX_JOURNAL) return;
		this.deleteJournalFiles(getIndexedLogfiles());
	}
	
	private ArrayList<String> getIndexedLogfiles() {
				
		PreparedStatement pstmt = null;
		ResultSet messagesRS = null;
		Connection conn = null;
		ArrayList<String> logfiles = new ArrayList<String>();
		try {
			conn = this.getDefaultConn();
			pstmt = conn.prepareStatement("select distinct logfile from " + getMTable());
			messagesRS = pstmt.executeQuery();

			assert (!messagesRS.isClosed());
			while (messagesRS.next()) {
				logfiles.add(messagesRS.getString(1));
			}
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

	private File[] getJournalFiles() {
		File[] dirFiles = (new File(this.journaler.logdir))
				.listFiles(new FileFilter() {
					@Override
					public boolean accept(File pathname) {
						return pathname.toString().startsWith(
								SQLPaxosLogger.this.journaler
										.getLogfilePrefix());
					}
				});
		return dirFiles;
	}

	private void deleteJournalFiles(ArrayList<String> logfiles) {

		File[] dirFiles = this.getJournalFiles();
		// delete files not in DB
		for (File f : dirFiles)
			if (!logfiles.contains(f)
					&& f.toString().startsWith(
							this.journaler.getLogfilePrefix())) {
				log.log(Level.INFO, "{0} garbage collecting log file {1}",
						new Object[] { this, f, logfiles });
				f.delete();
			}
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
			assert(p instanceof AcceptPacket) : p.getType() + ":"+p; 
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
						+ " and "
						+ SQLPaxosLogger.getIntegerLTConstraint("version",
								version) + ")" : " where true");
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
		for (File f : this.getJournalFiles())
			if (f.toString().startsWith(this.journaler.getLogfilePrefix()) && f.length()!=0) {
				log.log(Level.INFO, "{0} removing log file {1}", new Object[]{this, f});
				f.delete();
			}
		return this.remove(null, 0);
	}

	public void closeImpl() {
		log.log(Level.INFO, "{0}{1}", new Object[] { this, " closing DB" });
		this.GC.cancel();
		this.setClosed(true);
		// can not close derby until all instances are done
		if (allClosed() || !isEmbeddedDB())
			this.closeGracefully();
	}

	public String toString() {
		return this.getClass().getSimpleName() + strID;
	}

	private static boolean isEmbeddedDB() {
		return SQL_TYPE.equals(SQL.SQLType.EMBEDDED_DERBY);
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
				+ " (paxos_id varchar("
				+ MAX_PAXOS_ID_SIZE
				+ ") not null, version int, members varchar("
				+ MAX_GROUP_STR_LENGTH
				+ "), slot int, "
				+ "ballotnum int, coordinator int, state "
				+ (getCheckpointClobOption() ? SQL.getClobString(
						maxCheckpointSize, SQL_TYPE) : " varchar("
						+ maxCheckpointSize + ")")
				+ ", createTime bigint, primary key (paxos_id))";
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
				+ "),  offset int, length bigint, message "
				+ (getLogMessageClobOption() ? SQL.getClobString(
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
				+ (getCheckpointClobOption() ? SQL.getClobString(
						maxCheckpointSize, SQL_TYPE) : " varchar("
						+ maxCheckpointSize + ")")
				+ ", createtime bigint, primary key (paxos_id))";

		/*
		 * We create a non-unique-key index below instead of (unique) primary
		 * key (commented out above) as otherwise we will get duplicate key
		 * exceptions during batch inserts. It is unnecessary to create
		 * an index on ballotnum and coordinator as the number of logged
		 * prepares is likely to be small for any single group.
		 */
		String cmdMI = "create index messages_index on " + getMTable()
				+ "(paxos_id,packet_type,slot)";  //,ballotnum,coordinator)";
		String cmdP = "create table " + getPTable() + " (paxos_id varchar("
				+ MAX_PAXOS_ID_SIZE + ") not null, serialized varchar("
				+ PAUSE_STATE_SIZE + ") not null, primary key (paxos_id))";

		// this.dropTable(getPTable()); // pause table is unnecessary
		this.clearTable(getPTable()); // pause table is unnecessary

		Statement stmt = null;
		Connection conn = null;
		try {
			conn = this.getDefaultConn();
			stmt = conn.createStatement();
			createdCheckpoint = createTable(stmt, cmdC, getCTable());
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
								+ (getCheckpointClobOption() ? SQL
										.getClobString(maxCheckpointSize,
												SQL_TYPE) : " varchar("
										+ maxCheckpointSize + ")"));
						stmt.execute("alter table "
								+ getPCTable()
								+ " alter column state set data type "
								+ (getCheckpointClobOption() ? SQL
										.getClobString(maxCheckpointSize,
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
								+ (getLogMessageClobOption() ? SQL
										.getClobString(maxLogMessageSize,
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
				+ (getLogMessageClobOption() ? "" : " and message=?");
		boolean logged = false;

		try {
			conn = this.getDefaultConn();
			pstmt = conn.prepareStatement(cmd);
			if (!getLogMessageClobOption())
				pstmt.setString(1, msg); // will not work for clobs
			messagesRS = pstmt.executeQuery();
			while (messagesRS.next() && !logged) {
				String insertedMsg = (!getLogMessageClobOption() ? messagesRS
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
		int[] group = { 2, 4, 5, 11, 23, 34, 56, 78, 80, 83, 85, 96, 97, 98, 99 };
		System.out.println("\nStarting " + size + " writes: ");
		long t1 = System.currentTimeMillis(), t2 = t1;
		int k = 1;
		DecimalFormat df = new DecimalFormat("#.##");
		for (int i = 0; i < size; i++) {
			this.putCheckpointState("paxos" + i, 0, group, 0, new Ballot(0,
					i % 34), "hello" + i, 0);
			t2 = System.currentTimeMillis();
			if (i % k == 0 && i > 0) {
				System.out.print("[" + i + " : "
						+ df.format(((double) (t2 - t1)) / i) + "ms]\n");
				k *= 2;
			}
		}
		return (double) (t2 - t1) / size;
	}

	private double readCheckpoints(int size) {
		System.out.println("\nStarting " + size + " reads: ");
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
			cpds.setUser(props.getProperty("user"));
			cpds.setPassword(props.getProperty("password"));
			cpds.setAutoCommitOnClose(true);
			cpds.setMaxPoolSize(MAX_POOL_SIZE);
		} catch (PropertyVetoException pve) {
			pve.printStackTrace();
		}

		return cpds;
	}

	private void fixURI() {
		this.dataSource.setJdbcUrl(SQL.getProtocolOrURL(SQL_TYPE)
				+ this.logDirectory + DATABASE + this.myID);
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
		if (isClosed() || !isLoggingEnabled())
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

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// DerbyPaxosLogger.setLogMessageClobOption(false);
		SQLPaxosLogger logger = new SQLPaxosLogger(23, null, null);

		int[] group = { 32, 43, 54 };
		String paxosID = "paxos0";
		int slot = 0;
		int ballotnum = 1;
		int coordinator = 2;
		String state = "Hello World";
		Ballot ballot = new Ballot(ballotnum, coordinator);
		logger.putCheckpointState(paxosID, 0, group, slot, ballot, state, 0);
		assert (logger.isInserted(paxosID, group, slot, ballot, state));
		logger.copyEpochFinalCheckpointState(paxosID, 0);
		assert (logger.isInserted(logger.getPCTable(), paxosID, group, slot,
				ballot, state));
		DecimalFormat df = new DecimalFormat("#.##");

		int million = 1000000;
		int size = 1024;// (int)(0.001*million);

		double avg_write_time = logger.createCheckpoints(size);
		System.out.println("Average time to write " + size + " checkpoints = "
				+ df.format(avg_write_time));
		double avg_read_time = logger.readCheckpoints(size);
		System.out.println("Average time to read " + size + " checkpoints = "
				+ df.format(avg_read_time));

		try {
			int numPackets = 1024; // 65536;
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
				if (j > 0 && (j % k == 0 || j % million == 0)) {
					System.out.println("[" + j + " : "
							+ df.format(((double) time) / j) + "ms] ");
					k *= 2;
				}
			}

			long t1 = System.currentTimeMillis();
			LogMessagingTask[] lmTasks = new LogMessagingTask[packets.length];
			for(int j=0; i<lmTasks.length; j++) lmTasks[i] = new LogMessagingTask(packets[j]);
			logger.logBatch(lmTasks);
			System.out.println("Average log time = "
					+ Util.df((System.currentTimeMillis() - t1) * 1.0
							/ packets.length));

			System.out.print("Checking logged messages...");
			for (int j = 0; j < packets.length; j++) {
				assert (logger.isLogged(packets[j])) : packets[j];
				if (j % 100 == 0)
					System.out.print(j + " ");
			}
			System.out.println("checked");

			int newSlot = 200;
			int gcSlot = 100;
			Ballot newBallot = new Ballot(0, 2);
			logger.putCheckpointState(paxosID, 0, group, newSlot, newBallot,
					"Hello World", gcSlot);
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
					if (sbc[0] <= gcSlot)
						assert (!logger.isLogged(packets[j]));
					else
						assert (logger.isLogged(packets[j]));
				} else if (type == PaxosPacket.PaxosPacketType.PREPARE) {
					if ((sbc[1] < newBallot.ballotNumber || (sbc[1] == newBallot.ballotNumber && sbc[2] < newBallot.coordinatorID))) {
						assert (!logger.isLogged(packets[j])) : packets[j]
								.toString();
					} else
						assert (logger.isLogged(packets[j]));
				} else if (type == PaxosPacket.PaxosPacketType.DECISION) {
					if (sbc[0] < newSlot - MAX_OLD_DECISIONS)
						assert (!logger.isLogged(packets[j]));
					else
						assert (logger.isLogged(packets[j]));
				}
			}
			System.out.println("checked");
			logger.closeGracefully();
			System.out
					.println("SUCCESS: No exceptions or assertion violations were triggered. "
							+ "Average log time over "
							+ numPackets
							+ " packets = "
							+ ((double) time)
							/ numPackets
							+ " ms");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
