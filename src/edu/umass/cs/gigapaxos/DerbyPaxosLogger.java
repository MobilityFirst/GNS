package edu.umass.cs.gigapaxos;

import com.mchange.v2.c3p0.ComboPooledDataSource;

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
import edu.umass.cs.gigapaxos.paxosutil.Messenger;
import edu.umass.cs.gigapaxos.paxosutil.RecoveryInfo;
import edu.umass.cs.gigapaxos.paxosutil.SlotBallotState;
import edu.umass.cs.gigapaxos.paxosutil.StringContainer;
import edu.umass.cs.gigapaxos.testing.TESTPaxosConfig;
import edu.umass.cs.utils.Util;
import edu.umass.cs.utils.DelayProfiler;

import org.json.JSONException;

import javax.sql.DataSource;

import java.beans.PropertyVetoException;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

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
@SuppressWarnings("javadoc")
public class DerbyPaxosLogger extends AbstractPaxosLogger {
	/* ********************************************************************
	 * DB related parameters to be changed to use a different database service.
	 */
	private static final String FRAMEWORK = "embedded";
	private static final String DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
	private static final String PROTOCOL = "jdbc:derby:";
	private static final String USER = "user";
	private static final String PASSWORD = "user";
	private static final String DATABASE = "paxos_logs";
	/* ************ End of DB service related parameters ************** */

	private static final boolean CONN_POOLING = true;
	private static final int MAX_POOL_SIZE = 100;

	private static final String DUPLICATE_KEY = "23505";
	private static final String DUPLICATE_TABLE = "X0Y32";
	private static final List<String> NONEXISTENT_TABLE = new ArrayList<String>(
			Arrays.asList("42Y07", "42Y55"));

	private static final String CHECKPOINT_TABLE = "checkpoint";
	private static final String PREV_CHECKPOINT_TABLE = "prev_checkpoint";
	private static final String PAUSE_TABLE = "pause";
	private static final String MESSAGES_TABLE = "messages";
	private static final boolean AUTO_COMMIT = true;

	// disable persistent logging altogether
	private static final boolean DISABLE_LOGGING = TESTPaxosConfig.DISABLE_LOGGING; // false;

	// maximum size of a paxos group name
	private static final int PAXOS_ID_SIZE = 40;
	private static final int PAUSE_STATE_SIZE = 256;
	// maximum size of a paxos replica group
	private static final int MAX_GROUP_SIZE = 256;
	// maximum size of a log message
	private static final int MAX_LOG_MESSAGE_SIZE = 32672;
	// maximum size of a checkpoint
	private static final int MAX_CHECKPOINT_SIZE = 32672;
	// truncated checkpoint state size for java logging purposes
	private static final int TRUNCATED_STATE_SIZE = 512;
	private static final int MAX_OLD_DECISIONS = PaxosInstanceStateMachine.INTER_CHECKPOINT_INTERVAL;
	private static final int MAX_VARCHAR_SIZE = 32672;
	private static final boolean CHECKPOINT_CLOB_OPTION = (MAX_CHECKPOINT_SIZE > MAX_VARCHAR_SIZE);
	private static final boolean LOG_MESSAGE_CLOB_OPTION = (MAX_LOG_MESSAGE_SIZE > MAX_VARCHAR_SIZE)
			|| PaxosManager.BATCHING_ENABLED;

	// needed for testing with recovery in the same JVM
	private static final boolean DONT_SHUTDOWN_EMBEDDED = true;

	// need to be able to set this for unit-testing
	private static boolean logMessageClobOption = LOG_MESSAGE_CLOB_OPTION;

	private static boolean getLogMessageClobOption() {
		return logMessageClobOption;
	}

	private static void setLogMessageClobOption(boolean b) {
		logMessageClobOption = b;
	}

	// FIXME: Replace field name string constants with enums
	// private static enum Fields {PAXOS_ID, SLOT, BALLOTNUM, COORDINATOR,
	// PACKET_TYPE, STATE, MESSAGE};

	private static final ArrayList<DerbyPaxosLogger> instances = new ArrayList<DerbyPaxosLogger>();

	private ComboPooledDataSource dataSource = null;
	private Connection defaultConn = null;
	private Connection cursorConn = null;

	private boolean closed = true;

	protected static boolean isLoggingEnabled() {
		return !DISABLE_LOGGING;
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

	private static Logger log = PaxosManager.getLogger();

	DerbyPaxosLogger(int id, String dbPath, Messenger<?> messenger) {
		super(id, dbPath, messenger);
		addDerbyLogger(this);
		initialize(); // will set up db, connection, tables, etc. as needed
	}

	private synchronized Connection getDefaultConn() throws SQLException {
		return dataSource.getConnection();
	}

	private synchronized Connection getCursorConn() throws SQLException {
		return (this.cursorConn = this.dataSource.getConnection());
	}

	@Override
	public void putCheckpointState(String paxosID, int version, int[] group,
			int slot, Ballot ballot, String state, int acceptedGCSlot,
			long createTime) {
		this.putCheckpointState(paxosID, version,
				Util.arrayOfIntToStringSet(group), slot, ballot, state,
				acceptedGCSlot, createTime);
	}

	@Override
	public void putCheckpointState(String paxosID, int version, int[] group,
			int slot, Ballot ballot, String state, int acceptedGCSlot) {
		this.putCheckpointState(paxosID, version,
				Util.arrayOfIntToStringSet(group), slot, ballot, state,
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
		String cmd = this.getCheckpointState(getPCTable(), paxosID, "paxos_id") != null ? updateCmd
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
				if (CHECKPOINT_CLOB_OPTION)
					insertCP.setClob(6,
							new StringReader(cpRecord.getString("state")));
				else
					insertCP.setString(6, cpRecord.getString("state"));
				insertCP.setLong(7, cpRecord.getLong("createtime"));
				insertCP.setString(8, paxosID);
				copied = (insertCP.executeUpdate() > 0);
				conn.commit();
				log.log(Level.INFO,
						"{0} copied epoch final state for {1}:{2}: [{3}]",
						new Object[] { this, paxosID, version,
								cpRecord.getString("state") });
			}
		} catch (SQLException sqle) {
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

	/*
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
	public static final long MAX_FINAL_STATE_AGE = PaxosManager.MAX_FINAL_STATE_AGE;

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

	// can reuse getSlotBallotState here
	@Override
	public Integer getEpochFinalCheckpointVersion(String paxosID) {
		Integer version = null;
		ResultSet stateRS = null;
		PreparedStatement cpStmt = null;
		Connection conn = null;
		try {
			conn = this.getDefaultConn();
			assert (conn != null);

			cpStmt = this.getPreparedStatement(conn, getPCTable(), paxosID,
					"version");

			cpStmt.setString(1, paxosID);
			stateRS = cpStmt.executeQuery();
			while (stateRS.next()) {
				version = stateRS.getInt("version");
			}
		} catch (SQLException e) {
			log.severe((e instanceof SQLException ? "SQL" : "IO")
					+ "Exception while getting slot " + " : " + e);
			e.printStackTrace();
		} finally {
			cleanup(stateRS);
			cleanup(cpStmt);
			cleanup(conn);
		}
		return version;
	}

	@Override
	public synchronized boolean logBatch(PaxosPacket[] packets) {
		if (isClosed())
			return false;
		if (!isLoggingEnabled())
			return true;
		boolean logged = true;
		PreparedStatement pstmt = null;
		Connection conn = null;
		String cmd = "insert into " + getMTable()
				+ " values (?, ?, ?, ?, ?, ?, ?)";
		long t1 = System.currentTimeMillis();
		try {
			for (int i = 0; i < packets.length; i++) {
				if (conn == null) {
					conn = this.getDefaultConn();
					conn.setAutoCommit(false);
					pstmt = conn.prepareStatement(cmd);
				}
				PaxosPacket packet = packets[i];
				String packetString = packet.toString();
				int[] sb = AbstractPaxosLogger.getSlotBallot(packet);

				pstmt.setString(1, packet.getPaxosID());
				pstmt.setInt(2, packet.getVersion());
				pstmt.setInt(3, sb[0]);
				pstmt.setInt(4, sb[1]);
				pstmt.setInt(5, sb[2]);
				pstmt.setInt(6, packet.getType().getInt());
				if (getLogMessageClobOption())
					pstmt.setClob(7, new StringReader(packetString));
				else
					pstmt.setString(7, packetString);

				pstmt.addBatch();
				if ((i + 1) % 1000 == 0 || (i + 1) == packets.length) {
					int[] executed = pstmt.executeBatch();
					conn.commit();
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
					+ " while logging batch of size:" + packets.length + ": ");
			logged = false;
		} finally {
			cleanup(pstmt);
			cleanup(conn);
		}

		return logged;
	}

	/*
	 * The entry point for checkpointing. Puts given checkpoint state for
	 * paxosID. 'state' could be anything that allows PaxosInterface to later
	 * restore the corresponding state. For example, 'state' could be the name
	 * of a file where the app maintains a checkpoint of all of its state. It
	 * could of course be the stringified form of the actual state if the state
	 * is at most MAX_STATE_SIZE.
	 */
	private void putCheckpointState(String paxosID, int version,
			Set<String> group, int slot, Ballot ballot, String state,
			int acceptedGCSlot, long createTime) {
		if (isClosed() || !isLoggingEnabled())
			return;

		long t1 = System.currentTimeMillis(), t2 = 0;
		// Stupid derby doesn't have an insert if not exist command
		String insertCmd = "insert into "
				+ getCTable()
				+ " (version,members,slot,ballotnum,coordinator,state,createtime,paxos_id) values (?,?,?,?,?,?,?,?)";
		String updateCmd = "update "
				+ getCTable()
				+ " set version=?,members=?, slot=?, ballotnum=?, coordinator=?, state=?, createTime=? where paxos_id=?";
		String existingCP = this.getCheckpointState(paxosID, "paxos_id");
		String cmd = existingCP != null ? updateCmd : insertCmd;
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
			if (CHECKPOINT_CLOB_OPTION)
				insertCP.setClob(6, new StringReader(state));
			else
				insertCP.setString(6, state);
			insertCP.setLong(7, createTime);
			insertCP.setString(8, paxosID);
			insertCP.executeUpdate();
			conn.commit();
			t2 = System.currentTimeMillis();
		} catch (SQLException sqle) {
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
		if (!BATCH_GC_ENABLED)
			this.deleteOutdatedMessages(paxosID, slot, ballot.ballotNumber,
					ballot.coordinatorID, acceptedGCSlot);
		// why can't insertCP.toString() return the query string? :/
		log.log(Level.INFO,
				"{0} DB inserted checkpoint ({1}:{2}, {3}, {4}, {5}, {6}); took {7} ms; garbage collection took {8} ms",
				new Object[] { this, paxosID, version,
						Util.toJSONString(group), slot, ballot,
						Util.truncate(state, TRUNCATED_STATE_SIZE),
						acceptedGCSlot, (t2 - t1),
						(System.currentTimeMillis() - t2) });
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

		PreparedStatement pstmt = null, dstmt = null;
		ResultSet checkpointRS = null;
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

			// Create delete command using the slot, ballot, and gcSlot
			String dcmd = "delete from " + getMTable() + " where paxos_id='"
					+ paxosID + "' and " + "(packet_type="
					+ PaxosPacketType.DECISION.getInt() + " and "
					+ decisionConstraint + ") or " + "(packet_type="
					+ PaxosPacketType.PREPARE.getInt() + " and ("
					+ ballotnumConstraint + " or (ballotnum=" + ballotnum
					+ " and coordinator<" + coordinator + "))) or"
					+ "(packet_type=" + PaxosPacketType.ACCEPT.getInt()
					+ " and " + acceptConstraint + ")";
			conn = getDefaultConn();
			dstmt = conn.prepareStatement(dcmd);
			dstmt.execute();
			conn.commit();
			log.log(Level.FINE, "{0}{1}{2}", new Object[] { this,
					" DB deleted up to slot ", acceptedGCSlot });
		} catch (SQLException sqle) {
			log.severe("SQLException while deleting outdated messages for "
					+ paxosID);
			sqle.printStackTrace();
		} finally {
			cleanup(pstmt, checkpointRS);
			cleanup(dstmt);
			cleanup(conn);
		}
	}

	/*
	 * Used to be the entry point for message logging. Replaced by batchLog and
	 * log(PaxosPacket) now.
	 */
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
			if (getLogMessageClobOption())
				localLogMsgStmt.setClob(7, new StringReader(message));
			else
				localLogMsgStmt.setString(7, message);

			int rowcount = localLogMsgStmt.executeUpdate();
			assert (rowcount == 1);
			logged = true;
			log.log(Level.FINEST, "{0}{1}{2}{3}{4}{5}{6}{7}{8}{9}",
					new Object[] { "Inserted (", paxosID, ",", slot, ",",
							ballotnum, ",", coordinator, ":", message });
		} catch (SQLException sqle) {
			if (sqle.getSQLState().equals(DerbyPaxosLogger.DUPLICATE_KEY)) {
				log.log(Level.FINE, "{0}{1}{2}{3}", new Object[] { this,
						" log message ", message, " previously logged" });
				logged = true;
			} else {
				log.severe("SQLException while logging as " + cmd + " : "
						+ sqle);
				sqle.printStackTrace();
			}
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
			conn.commit();
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
				state = (!CHECKPOINT_CLOB_OPTION ? stateRS.getString(1)
						: clobToString(stateRS.getClob(1)));
			}
		} catch (IOException e) {
			log.severe("IOException while getting state " + " : " + e);
			e.printStackTrace();
		} catch (SQLException sqle) {
			log.severe("SQLException while getting state " + " : " + sqle);
		} finally {
			cleanup(pstmt, stateRS);
			cleanup(conn);
		}

		return state;
	}

	private static String clobToString(Clob clob) throws SQLException,
			IOException {
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

	/**
	 * Methods to get slot, ballotnum, coordinator, state, and version of
	 * checkpoint
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

			cpStmt = this.getPreparedStatement(conn, table, paxosID,
					"slot, ballotnum, coordinator, state, version, createTime");

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

				if (!versionMismatch)
					sb = new SlotBallotState(stateRS.getInt(1),
							stateRS.getInt(2), stateRS.getInt(3),
							(!CHECKPOINT_CLOB_OPTION ? stateRS.getString(4)
									: clobToString(stateRS.getClob(4))),
							stateRS.getInt(5), stateRS.getLong(6));
			}
		} catch (SQLException | IOException e) {
			log.severe((e instanceof SQLException ? "SQL" : "IO")
					+ "Exception while getting slot " + " : " + e);
			e.printStackTrace();
		} finally {
			cleanup(stateRS);
			cleanup(cpStmt);
			cleanup(conn);
		}
		return versionMismatch ? null : sb;
	}

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

		log.log(Level.INFO, "{0}{1}", new Object[] { this,
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
				String state = (readState ? (!CHECKPOINT_CLOB_OPTION ? cursorRset
						.getString(4) : clobToString(cursorRset.getClob(4)))
						: null);
				pri = new RecoveryInfo(paxosID, version, pieces, state);
			}
		} catch (SQLException | JSONException | IOException e) {
			log.severe(e.getClass().getSimpleName()
					+ " in readNextCheckpoint: " + " : " + e);
		}
		return pri;
	}

	public synchronized boolean initiateReadMessages() {
		if (isClosed() || this.cursorPstmt != null || this.cursorRset != null
				|| this.cursorConn != null)
			return false;

		log.log(Level.INFO, "{0}{1}", new Object[] { this,
				" invoked initiatedReadMessages()" });
		boolean initiated = false;
		try {
			this.cursorPstmt = this.getPreparedStatement(this.getCursorConn(),
					getMTable(), null, "message");
			this.cursorRset = this.cursorPstmt.executeQuery();
			initiated = true;
		} catch (SQLException sqle) {
			log.severe("SQLException while getting all paxos IDs " + " : "
					+ sqle);
		}
		return initiated;
	}

	public synchronized PaxosPacket readNextMessage() {
		PaxosPacket packet = null;
		try {
			if (cursorRset != null && cursorRset.next()) {
				String msg = (!getLogMessageClobOption() ? cursorRset
						.getString(1) : clobToString(cursorRset.getClob(1)));
				packet = PaxosPacket.getPaxosPacket(msg);
			}
		} catch (SQLException | JSONException | IOException e) {
			log.severe(this + " got " + e.getClass().getSimpleName()
					+ " in readNextMessage while reading: " + " : " + packet);
			e.printStackTrace();
		}
		return packet;
	}

	public synchronized void closeReadAll() {
		log.log(Level.INFO, "{0}{1}", new Object[] { this,
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

	/**
	 * Gets the list of logged messages for the paxosID. The static method
	 * PaxosLogger.rollForward(.) can be directly invoked to replay these
	 * messages without explicitly invoking this method.
	 */
	public synchronized ArrayList<PaxosPacket> getLoggedMessages(
			String paxosID, String fieldConstraints) {
		ArrayList<PaxosPacket> messages = new ArrayList<PaxosPacket>();
		PreparedStatement pstmt = null;
		ResultSet messagesRS = null;
		Connection conn = null;
		try {
			conn = this.getDefaultConn();
			pstmt = this.getPreparedStatement(conn, getMTable(), paxosID,
					"message", fieldConstraints);
			messagesRS = pstmt.executeQuery();

			assert (!messagesRS.isClosed());
			while (messagesRS.next()) {
				String msg = (!getLogMessageClobOption() ? messagesRS
						.getString(1) : clobToString(messagesRS.getClob(1)));
				PaxosPacket packet = PaxosPacket.getPaxosPacket(msg);
				messages.add(packet);
			}
		} catch (SQLException | JSONException | IOException e) {
			log.severe(e.getClass().getSimpleName()
					+ " while getting slot for " + paxosID + ":");
			e.printStackTrace();
		} finally {
			cleanup(pstmt, messagesRS);
			cleanup(conn);
		}
		return messages;
	}

	public ArrayList<PaxosPacket> getLoggedMessages(String paxosID) {
		return this.getLoggedMessages(paxosID, null);
	}

	/*
	 * Acceptors remove decisions right after executing them. So they need to
	 * fetch logged decisions from the disk to handle synchronization requests.
	 */
	public synchronized ArrayList<PValuePacket> getLoggedDecisions(
			String paxosID, int minSlot, int maxSlot) {
		ArrayList<PValuePacket> decisions = new ArrayList<PValuePacket>();
		if (maxSlot - minSlot <= 0)
			return decisions;
		ArrayList<PaxosPacket> list = this.getLoggedMessages(paxosID,
				" and packet_type=" + PaxosPacketType.DECISION.getInt()
						+ " and " + getIntegerGTEConstraint("slot", minSlot)
						+ " and " + getIntegerLTConstraint("slot", maxSlot)); // wraparound-arithmetic
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
	public synchronized Map<Integer, PValuePacket> getLoggedAccepts(
			String paxosID, int firstSlot) {
		long t1 = System.currentTimeMillis();
		// fetch all accepts and then weed out those below firstSlot
		ArrayList<PaxosPacket> list = this.getLoggedMessages(paxosID,
				" and packet_type=" + PaxosPacketType.ACCEPT.getInt() + " and "
						+ getIntegerGTEConstraint("slot", firstSlot));
		TreeMap<Integer, PValuePacket> accepted = new TreeMap<Integer, PValuePacket>();
		for (PaxosPacket p : list) {
			int slot = AbstractPaxosLogger.getSlotBallot(p)[0];
			AcceptPacket accept = (AcceptPacket) p;
			if ((slot - firstSlot >= 0)
					&& /* wraparound-arithmetic */
					(!accepted.containsKey(slot) || accepted.get(slot).ballot
							.compareTo(accept.ballot) < 0))
				accepted.put(slot, accept);
		}
		DelayProfiler.updateDelay("getLoggedAccepts", t1);
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
				+ (paxosID != null ? " where paxos_id='" + paxosID
						+ "' and version=" + version : " where true");
		String cmdM = "delete from "
				+ getMTable()
				+ (paxosID != null ? " where paxos_id='" + paxosID
						+ "' and version=" + version : " where true");
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
			conn.commit();
			log.log(Level.INFO,
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
		return this.remove(null, 0);
	}

	public void closeImpl() {
		log.log(Level.INFO, "{0}{1}", new Object[] { this, " closing DB" });
		this.setClosed(true);
		// can not close derby until all instances are done
		if (allClosed() || !isEmbeddedDB())
			this.closeGracefully();
	}

	public String toString() {
		return this.getClass().getSimpleName() + myID;
	}

	private static boolean isEmbeddedDB() {
		return FRAMEWORK.equals("embedded");
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
		 * otherwise. I discovered these symptoms the hard way!
		 * 
		 * The static waitToFinishAll() parent method below ensures that all
		 * derby DB instances have finished processing any pending log or
		 * checkpoint tasks before actually closing the DB. Otherwise, because
		 * it is an embedded DB, invoking shutdonw like below within any
		 * instance will end up ungracefully shutting down the DB for all
		 * instances.
		 */

		if (FRAMEWORK.equals("embedded")) {
			try {
				// the shutdown=true attribute shuts down Derby
				if (!DONT_SHUTDOWN_EMBEDDED)
					DriverManager.getConnection(PROTOCOL + ";shutdown=true");

				// To shut down a specific database only, but keep the
				// databases), specify a database in the connection URL:
				// DriverManager.getConnection("jdbc:derby:" + dbName +
				// ";shutdown=true");
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
		if (/* !loadDriver() || */!connectDB() || !createTables())
			return false;
		setClosed(false); // setting open
		return true;
	}

	/**
	 * Creates a paxosID-primary-key table for checkpoints and another table for
	 * messages that indexes slot, ballotnum, and coordinator. The checkpoint
	 * table also stores the slot, ballotnum, and coordinator of the checkpoint.
	 * The index in the messages table is useful to optimize searching for old
	 * messages and deleting them when the checkpoint in the checkpoint table is
	 * advanced. The test for "old" is based on the slot, ballotnum, and
	 * coordinator fields, so they are indexed.
	 */
	private boolean createTables() {
		boolean createdCheckpoint = false, createdMessages = false, createdPTable = false, createdPrevCheckpoint = false;
		String cmdC = "create table " + getCTable() + " (paxos_id varchar("
				+ PAXOS_ID_SIZE + ") not null, version int, members varchar("
				+ MAX_GROUP_SIZE + "), slot int, "
				+ "ballotnum int, coordinator int, state "
				+ (CHECKPOINT_CLOB_OPTION ? "clob(" : "varchar(")
				+ MAX_CHECKPOINT_SIZE
				+ "), createTime bigint, primary key (paxos_id))";
		/*
		 * It is best not to have a primary key in the log message table as
		 * otherwise batch inserts can create exceptions as derby does not seem
		 * to have an insert if not exists primitive.
		 */
		String cmdM = "create table "
				+ getMTable()
				+ " (paxos_id varchar("
				+ PAXOS_ID_SIZE
				+ ") not null, "
				+ "version int, slot int, ballotnum int, coordinator int, packet_type int, message "
				+ (getLogMessageClobOption() ? "clob(" : "varchar(")
				+ MAX_LOG_MESSAGE_SIZE
				+ ")/*, primary key(paxos_id, slot, ballotnum, coordinator, packet_type)*/)";
		String cmdPC = "create table " + getPCTable() + " (paxos_id varchar("
				+ PAXOS_ID_SIZE + ") not null, version int, members varchar("
				+ MAX_GROUP_SIZE + "), slot int, "
				+ "ballotnum int, coordinator int, state "
				+ (CHECKPOINT_CLOB_OPTION ? "clob(" : "varchar(")
				+ MAX_CHECKPOINT_SIZE
				+ "), createtime bigint, primary key (paxos_id))";

		/*
		 * We create a non-unique-key index below instead of (unique) primary
		 * key (commented out above) as otherwise we will get duplicate key
		 * exceptions during batch inserts.
		 */
		String cmdMI = "create index messages_index on " + getMTable()
				+ "(paxos_id,packet_type,slot,ballotnum,coordinator)";
		String cmdP = "create table " + getPTable() + " (paxos_id varchar("
				+ PAXOS_ID_SIZE + ") not null, serialized varchar("
				+ PAUSE_STATE_SIZE + ") not null, primary key (paxos_id))";
		if (this.dbDirectoryExists())
			// using pause table may slow down recovery
			this.dropTable(getPTable());
		Statement stmt = null;
		Connection conn = null;
		try {
			conn = this.getDefaultConn();
			stmt = conn.createStatement();
			createdCheckpoint = createTable(stmt, cmdC, getCTable());
			createdMessages = createTable(stmt, cmdM, getCTable())
					&& createIndex(stmt, cmdMI, getMTable());
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
		return createdCheckpoint && createdMessages && createdPTable;
	}

	private boolean createTable(Statement stmt, String cmd, String table) {
		boolean created = false;
		try {
			stmt.execute(cmd);
			created = true;
		} catch (SQLException sqle) {
			if (sqle.getSQLState().equals(DerbyPaxosLogger.DUPLICATE_TABLE)) {
				log.log(Level.INFO, "{0}{1}{2}", new Object[] { "Table ",
						table, " already exists" });
				created = true;
			} else {
				log.severe("Could not create table: " + table);
				sqle.printStackTrace();
			}
		}
		return created;
	}

	private boolean createIndex(Statement stmt, String cmd, String table) {
		return createTable(stmt, cmd, table);
	}

	private boolean dropTable(String table) {
		String cmd = "drop table " + getPTable();
		PreparedStatement pstmt = null;
		boolean dropped = false;
		try {
			Connection conn = this.getDefaultConn();
			pstmt = conn.prepareStatement(cmd);
			pstmt.execute();
			conn.commit();
			dropped = true;
			log.log(Level.INFO, "{0}{1}{2}", new Object[] { this,
					" dropped pause table ", table });
		} catch (SQLException sqle) {
			if (!NONEXISTENT_TABLE.contains(sqle.getSQLState())) {
				log.severe(this + " could not drop table " + table + ":"
						+ sqle.getSQLState());
				sqle.printStackTrace();
			}
		}
		return dropped;
	}

	private boolean dbDirectoryExists() {
		File f = new File(this.logDirectory + DATABASE);
		return f.exists() && f.isDirectory();
	}

	/*
	 * This method will connect to the DB while creating it if it did not
	 * already exist. This method is not really needed but exists only because
	 * otherwise c3p0 throws unsuppressable warnings about DB already existing
	 * no matter how you use it. So we now create the DB separately and always
	 * invoke c3p0 without the create flag (default false).
	 */
	private boolean existsDB(String dbCreation, Properties props)
			throws SQLException {
		try {
			Class.forName(DRIVER).newInstance();
		} catch (InstantiationException | IllegalAccessException
				| ClassNotFoundException e) {
			e.printStackTrace();
			return false;
		}
		Connection conn = DriverManager.getConnection(PROTOCOL
				+ this.logDirectory + DATABASE
				+ (!this.dbDirectoryExists() ? ";create=true" : ""));
		cleanup(conn);
		return true;
	}

	private boolean connectDB() {
		boolean connected = false;
		int connAttempts = 0, maxAttempts = 1;
		long interAttemptDelay = 2000; // ms
		Properties props = new Properties(); // connection properties
		// providing a user name and PASSWORD is optional in embedded derby
		props.put("user", DerbyPaxosLogger.USER + this.myID);
		props.put("password", DerbyPaxosLogger.PASSWORD);
		String dbCreation = PROTOCOL + this.logDirectory + DATABASE;

		try {
			if (!this.existsDB(dbCreation, props))
				dbCreation += ";create=true";
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
				log.info("Connected to and created database " + DATABASE);
				connected = true;
				fixURI();
			} catch (SQLException sqle) {
				log.severe("Could not connect to the derby DB: "
						+ sqle.getErrorCode() + ":" + sqle.getSQLState());
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

	private static void addDerbyLogger(DerbyPaxosLogger derbyLogger) {
		synchronized (DerbyPaxosLogger.instances) {
			if (!DerbyPaxosLogger.instances.contains(derbyLogger))
				DerbyPaxosLogger.instances.add(derbyLogger);
		}
	}

	private static boolean allClosed() {
		synchronized (DerbyPaxosLogger.instances) {
			for (DerbyPaxosLogger logger : instances) {
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
				+ coordinator + " and message=?";
		boolean logged = false;

		try {
			conn = this.getDefaultConn();
			pstmt = conn.prepareStatement(cmd);
			pstmt.setString(1, msg); // will not work for clobs
			messagesRS = pstmt.executeQuery();
			while (messagesRS.next() && !logged) {
				String insertedMsg = (!getLogMessageClobOption() ? messagesRS
						.getString(2) : clobToString(messagesRS.getClob(2)));
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

	public static DataSource setupDataSourceC3P0(String connectURI,
			Properties props) throws SQLException {

		ComboPooledDataSource cpds = new ComboPooledDataSource();
		try {
			cpds.setDriverClass(DRIVER);
			cpds.setJdbcUrl(connectURI);
			cpds.setUser(props.getProperty("user"));
			cpds.setPassword(props.getProperty("password"));
			cpds.setAutoCommitOnClose(AUTO_COMMIT);
			cpds.setMaxPoolSize(MAX_POOL_SIZE);
		} catch (PropertyVetoException pve) {
			pve.printStackTrace();
		}

		return cpds;
	}

	private void fixURI() {
		this.dataSource.setJdbcUrl(PROTOCOL + this.logDirectory + DATABASE);
	}

	/**
	 * Gets a map of all paxosIDs and their corresponding group members. Used
	 * only for testing.
	 */
	public ArrayList<RecoveryInfo> getAllPaxosInstances() {
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
			conn.commit();
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
		DerbyPaxosLogger.setLogMessageClobOption(false);
		DerbyPaxosLogger logger = new DerbyPaxosLogger(23, null, null);

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
			int reqClientID = 25;
			String reqValue = "26";
			int nodeID = coordinator;
			int k = 1;
			for (int j = 0; j < packets.length; j++) {
				RequestPacket req = new RequestPacket(reqClientID, 0, reqValue,
						false);
				ProposalPacket prop = new ProposalPacket(i, req);
				PValuePacket pvalue = new PValuePacket(ballot, prop);
				AcceptPacket accept = new AcceptPacket(nodeID, pvalue, -1);
				pvalue = pvalue.makeDecision(-1);
				PreparePacket prepare = new PreparePacket(new Ballot(i,
						ballot.coordinatorID));
				if (j % 3 == 0) { // prepare
					packets[j] = prepare;
				} else if (j % 3 == 1) { // accept
					accept.setCreateTime(0);
					packets[j] = accept;
				} else if (j % 3 == 2) { // decision
					pvalue.setCreateTime(0);
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
			logger.logBatch(packets);
			System.out.println("Average log time = "
					+ Util.df((System.currentTimeMillis() - t1) * 1.0
							/ packets.length));

			System.out.print("Checking logged messages...");
			for (int j = 0; j < packets.length; j++) {
				if (AUTO_COMMIT)
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
