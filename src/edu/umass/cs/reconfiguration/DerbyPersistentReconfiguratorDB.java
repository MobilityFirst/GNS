package edu.umass.cs.reconfiguration;

import java.beans.PropertyVetoException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.json.JSONException;
import org.json.JSONObject;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import edu.umass.cs.gigapaxos.InterfaceRequest;
import edu.umass.cs.nio.IntegerPacketType;
import edu.umass.cs.reconfiguration.examples.AppRequest;
import edu.umass.cs.reconfiguration.examples.ReconfigurableSampleNodeConfig;
import edu.umass.cs.reconfiguration.examples.TestConfig;
import edu.umass.cs.reconfiguration.json.reconfigurationpackets.DemandReport;
import edu.umass.cs.reconfiguration.reconfigurationutils.AbstractDemandProfile;
import edu.umass.cs.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.DemandProfile;
import edu.umass.cs.reconfiguration.reconfigurationutils.ReconfigurationRecord;
import edu.umass.cs.reconfiguration.reconfigurationutils.StringLocker;
import edu.umass.cs.reconfiguration.reconfigurationutils.ReconfigurationRecord.RCStates;
import edu.umass.cs.utils.Util;
import edu.umass.cs.utils.MyLogger;

/**
 * @author V. Arun
 */

public class DerbyPersistentReconfiguratorDB<NodeIDType> extends
		AbstractReconfiguratorDB<NodeIDType> implements
		InterfaceReconfiguratorDB<NodeIDType> {
	// private static final String FRAMEWORK = "embedded";
	private static final String DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
	private static final String PROTOCOL = "jdbc:derby:";
	private static final boolean CONN_POOLING = true;
	private static final String DUPLICATE_KEY = "23505";
	private static final String DUPLICATE_TABLE = "X0Y32";
	// private static final String NONEXISTENT_TABLE = "42Y07";
	private static final String USER = "user";
	private static final String PASSWORD = "user";
	private static final String DATABASE = "reconfiguration_DB";
	private static final String RECONFIGURATION_RECORD_TABLE = "checkpoint";
	private static final String PENDING_TABLE = "messages";
	private static final String DEMAND_PROFILE_TABLE = "demand";
	private static final String NODE_CONFIG_TABLE = "nodeconfig";
	// private static final boolean DISABLE_LOGGING = false;
	private static final boolean AUTO_COMMIT = true;
	private static final int MAX_POOL_SIZE = 100;
	private static final int MAX_NAME_SIZE = 40;
	private static final int MAX_RC_RECORD_SIZE = 4096;
	private static final int MAX_DEMAND_PROFILE_SIZE = 4096;
	private static final boolean RC_RECORD_CLOB_OPTION = MAX_RC_RECORD_SIZE > 4096;
	private static final boolean DEMAND_PROFILE_CLOB_OPTION = MAX_DEMAND_PROFILE_SIZE > 4096;
	private static final boolean COMBINE_STATS = false;
	private static final String CHECKPOINT_TRANSFER_DIR = "paxos_large_checkpoints";
	private static final boolean LARGE_CHECKPOINTS_OPTION = true;
	private static final int MAX_FILENAME_LENGTH = 128;
	private static final String CHARSET = "ISO-8859-1";

	private static enum Columns {
		SERVICE_NAME, EPOCH, RC_GROUP_NAME, ACTIVES, NEW_ACTIVES, RC_STATE, STRINGIFIED_RECORD, DEMAND_PROFILE, INET_ADDRESS, PORT, NODE_CONFIG_VERSION, RC_NODE_ID
	};

	private static enum Keys {
		INET_SOCKET_ADDRESS, FILENAME, FILESIZE
	};

	private static final ArrayList<DerbyPersistentReconfiguratorDB<?>> instances = new ArrayList<DerbyPersistentReconfiguratorDB<?>>();

	public String logDirectory;

	private ComboPooledDataSource dataSource = null;

	private ServerSocket serverSock = null;

	private StringLocker stringLocker = new StringLocker();

	private boolean closed = true;

	public static final Logger log = Logger.getLogger(Reconfigurator.class
			.getName());

	public DerbyPersistentReconfiguratorDB(NodeIDType myID,
			ConsistentReconfigurableNodeConfig<NodeIDType> nc, String logDir) {
		super(myID, nc);
		logDirectory = (logDir == null ? "." : logDir) + "/";
		addDerbyPersistentReconfiguratorDB(this);
		initialize();
	}

	public DerbyPersistentReconfiguratorDB(NodeIDType myID,
			ConsistentReconfigurableNodeConfig<NodeIDType> nc) {
		this(myID, nc, null);
	}

	/******************** Start of overridden methods *********************/

	@Override
	public synchronized ReconfigurationRecord<NodeIDType> getReconfigurationRecord(
			String name) {
		PreparedStatement pstmt = null;
		ResultSet recordRS = null;
		Connection conn = null;
		ReconfigurationRecord<NodeIDType> rcRecord = null;
		try {
			conn = this.getDefaultConn();
			pstmt = this.getPreparedStatement(conn, getRCRecordTable(), name,
					Columns.STRINGIFIED_RECORD.toString());
			recordRS = pstmt.executeQuery();

			assert (!recordRS.isClosed());
			while (recordRS.next()) {
				String msg = recordRS.getString(1);
				rcRecord = new ReconfigurationRecord<NodeIDType>(
						new JSONObject(msg), this.consistentNodeConfig);
			}
		} catch (SQLException | JSONException e) {
			log.severe((e instanceof SQLException ? "SQL" : "JSON")
					+ "Exception while getting slot for " + name + ":");
			e.printStackTrace();
		} finally {
			cleanup(pstmt, recordRS);
			cleanup(conn);
		}
		return rcRecord;
	}

	@Override
	public synchronized boolean updateDemandStats(
			DemandReport<NodeIDType> report) {
		JSONObject update = report.getStats();
		JSONObject historic = getDemandStatsJSON(report.getServiceName());
		JSONObject combined = null;
		String insertCmd = "insert into " + getDemandTable() + " ("
				+ Columns.DEMAND_PROFILE.toString() + ", "
				+ Columns.SERVICE_NAME.toString() + " ) values (?,?)";
		String updateCmd = "update " + getDemandTable() + " set "
				+ Columns.DEMAND_PROFILE.toString() + "=? where "
				+ Columns.SERVICE_NAME.toString() + "=?";
		String cmd = historic != null ? updateCmd : insertCmd;
		combined = update;
		if (historic != null && shouldCombineStats())
			combined = combineStats(historic, update);

		PreparedStatement insertCP = null;
		Connection conn = null;
		try {
			conn = this.getDefaultConn();
			insertCP = conn.prepareStatement(cmd);
			if (DEMAND_PROFILE_CLOB_OPTION)
				insertCP.setClob(1, new StringReader(combined.toString()));
			else
				insertCP.setString(1, combined.toString());
			insertCP.setString(2, report.getServiceName());
			insertCP.executeUpdate();
			conn.commit();
		} catch (SQLException sqle) {
			log.severe("SQLException while updating stats using " + cmd);
			sqle.printStackTrace();
		} finally {
			cleanup(insertCP);
			cleanup(conn);
		}
		return true;
	}

	/* One of two methods that changes state. Generally, this method will change
	 * the state to READY, while setStateInitReconfiguration sets the state from
	 * READY usually to WAIT_ACK_STOP.
	 */

	@Override
	public synchronized boolean setState(String name, int epoch,
			ReconfigurationRecord.RCStates state) {
		ReconfigurationRecord<NodeIDType> record = this
				.getReconfigurationRecord(name);
		if (record == null)
			return false;

		log.log(Level.INFO, MyLogger.FORMAT[8], new Object[] {
				"==============================> ", this, record.getName(),
				record.getEpoch(), record.getState(), " ->", epoch, state,
				record.getNewActives() });
		record.setState(name, epoch, state);
		// setStateInitReconfiguration used for intent
		assert(state.equals(RCStates.READY)); 
		if (state.equals(RCStates.READY)) {
			record.setActivesToNewActives();
			/*
			 * the list of pending reconfiguraitons is stored persistently so
			 * that we can resume the most recent incomplete reconfiguration
			 * upon recovery. The ones before the most recent will be handled by
			 * paxos roll forward automatically. The reason paxos is not enough
			 * for the most recent one is because reconfiguration is a two step
			 * process consisting of an "intent" followed by a "complete"
			 * operation. Paxos will blindly replay all committed operations but
			 * has no way of knowing application-specific information like the
			 * fact that an intent has to be followed by complete. So if the
			 * node crashes after an intent but before the corresponding
			 * complete, the app has to redo just that last step.
			 */
			this.setPending(name, false);
			/*
			 * Trimming RC epochs is needed only for the NODE_CONFIG record. It
			 * removed entries for deleted RC nodes. The entries maintain the
			 * current epoch number for the group corresponding to each RC node
			 * in NODE_CONFIG. This information is needed in order for nodes to
			 * know what epoch to reconfigure to what when the corresponding RC
			 * groups may be out of date locally or may not even exist locally.
			 * A simpler alternative is to force all RC groups to reconfigure
			 * upon the addition or deletion of any RC nodes so that RC group
			 * epoch numbers are always identical to the NODE_CONFIG epoch
			 * number, but this is unsatisfying as it does not preserve the
			 * "consistent hashing" like property for reconfigurations. Note
			 * that even a "trivial" reconfiguration, i.e., when there is no
			 * change in an RC group, must go through the stop, start, drop
			 * sequence for correctness, and that process involves checkpointing
			 * and restoring locally from the checkpoint. Even though the
			 * checkpoints are local, it can take a long time for a large number
			 * of records and is better avoided when not needed.
			 * 
			 * A downside of allowing different epoch numbers for different
			 * groups is that manual intervention if ever needed will be
			 * harrowing. It is much simpler to track out of date RC nodes when
			 * all RC group epoch numbers are known to be identical to the
			 * NODE_CONFIG epoch number.
			 */
			record.trimRCEpochs();
		}
		this.putReconfigurationRecord(record);
		return true;
	}

	/*
	 * state can be changed only if the current state is READY and if so, it can
	 * only be changed to WAIT_ACK_STOP. The epoch argument must also match the
	 * current epoch number.
	 */
	@Override
	public synchronized boolean setStateInitReconfiguration(String name,
			int epoch, RCStates state, Set<NodeIDType> newActives,
			NodeIDType primary) {
		ReconfigurationRecord<NodeIDType> record = this
				.getReconfigurationRecord(name);
		assert (record != null && (epoch - record.getEpoch() == 0)) : epoch
				+ "!=" + record.getEpoch() + " at " + myID;
		if (!record.getState().equals(RCStates.READY)) {
			log.info(this + " " + record.getName() + ":" + record.getEpoch()
					+ " not ready for " + name + ":" + epoch + ":" + state);
			return false;
		}
		assert (state.equals(RCStates.WAIT_ACK_STOP));
		log.log(Level.INFO, MyLogger.FORMAT[8], new Object[] {
				"==============================> ", this, record.getName(),
				record.getEpoch(), record.getState(), " ->", epoch, state,
				newActives });
		record.setState(name, epoch, state, newActives);
		this.setPending(name, true);
		this.putReconfigurationRecord(record);
		return true;
	}

	private synchronized void putReconfigurationRecord(
			ReconfigurationRecord<NodeIDType> rcRecord) {
		ReconfigurationRecord<NodeIDType> prev = this
				.getReconfigurationRecord(rcRecord.getName());
		String insertCmd = "insert into " + getRCRecordTable() + " ("
				+ Columns.RC_GROUP_NAME.toString() + ", "
				+ Columns.STRINGIFIED_RECORD.toString() + ", "
				+ Columns.SERVICE_NAME.toString() + " ) values (?,?,?)";
		String updateCmd = "update " + getRCRecordTable() + " set "
				+ Columns.RC_GROUP_NAME.toString() + "=?, "
				+ Columns.STRINGIFIED_RECORD.toString() + "=? where "
				+ Columns.SERVICE_NAME.toString() + "=?";
		String cmd = prev != null ? updateCmd : insertCmd;

		PreparedStatement insertCP = null;
		Connection conn = null;
		try {
			conn = this.getDefaultConn();
			insertCP = conn.prepareStatement(cmd);
			insertCP.setString(1, this.getRCGroupName(rcRecord.getName()));
			if (RC_RECORD_CLOB_OPTION)
				insertCP.setClob(2, new StringReader(rcRecord.toString()));
			else
				insertCP.setString(2, rcRecord.toString());
			insertCP.setString(3, rcRecord.getName());
			insertCP.executeUpdate();
			conn.commit();
		} catch (SQLException sqle) {
			log.severe("SQLException while inserting RC record using " + cmd);
			sqle.printStackTrace();
		} finally {
			cleanup(insertCP);
			cleanup(conn);
		}
		// printRCTable();
	}

	protected void printRCTable() {
		String cmd = "select * from " + this.getRCRecordTable();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Connection conn = null;
		try {
			conn = this.getDefaultConn();
			pstmt = conn.prepareStatement(cmd);
			rs = pstmt.executeQuery();
			String s = "[";
			while (rs.next()) {
				s += "\n  " + rs.getString(Columns.SERVICE_NAME.toString())
						+ " " + rs.getString(Columns.RC_GROUP_NAME.toString())
						+ " "
						+ rs.getString(Columns.STRINGIFIED_RECORD.toString());
			}
			System.out.println(myID + ":\n" + s + "\n]");
		} catch (SQLException e) {
			log.severe("SQLException while print RC records table " + cmd);
			e.printStackTrace();
		} finally {
			cleanup(pstmt, rs);
			cleanup(conn);
		}
	}

	// Should put RC records only for non-RC group names
	private synchronized void putReconfigurationRecordIfNotName(
			ReconfigurationRecord<NodeIDType> record, String rcGroupName) {
		/*
		 * FIXME: The documentation and commented code below are outdated.
		 * 
		 * First check if record name and group name are same. This check is
		 * quick. It needs that we maintain the invariant that RC group records
		 * always have the RC group as the record name. But we also need to be
		 * able to initially create these records including through paxos, so we
		 * allow such records to be inserted if no record for that group name
		 * exists in the DB. Note that we need both parts of the conjunction in
		 * the first clause below because in the case of node additions,
		 * record.getName() will equal rcGroupName but this node may not yet
		 * know about the existence of this node and that it corresponds to an
		 * RC group name.
		 */
		/*
		if ((!record.getName().equals(rcGroupName) && !this
				.isRCGroupName(record.getName()))
				|| (record.getName().equals(rcGroupName) && this
						.getReconfigurationRecord(rcGroupName) == null))
						*/
		this.putReconfigurationRecord(record);
	}

	@Override
	public synchronized boolean deleteReconfigurationRecord(String name,
			int epoch) {
		ReconfigurationRecord<NodeIDType> record = this
				.getReconfigurationRecord(name);
		if (record == null || record.getEpoch() != epoch)
			return false;

		log.log(Level.INFO, MyLogger.FORMAT[4],
				new Object[] { "==============================> ", this, name,
						" ->", "DELETE" });
		boolean deleted = this.deleteReconfigurationRecord(name,
				this.getRCRecordTable());
		this.deleteReconfigurationRecord(name, this.getPendingTable());
		this.deleteReconfigurationRecord(name, this.getDemandTable());
		return deleted;
	}

	@Override
	public synchronized Integer getEpoch(String name) {
		ReconfigurationRecord<NodeIDType> record = this
				.getReconfigurationRecord(name);
		return (record != null ? record.getEpoch() : null);
	}

	// This also sets newActives
	@Override
	public synchronized ReconfigurationRecord<NodeIDType> createReconfigurationRecord(
			ReconfigurationRecord<NodeIDType> record) {
		assert (this.getReconfigurationRecord(record.getName()) == null);
		this.putReconfigurationRecord(record);
		return record;
	}

	/******************** Incomplete paxos methods below **************/

	// write records to a file and return filename
	@Override
	public String getState(String rcGroup) {
		synchronized (this.stringLocker.get(rcGroup)) {
			String cpFilename = getCheckpointFile(rcGroup);
			if (!this.createCheckpointFile(cpFilename))
				return null;

			PreparedStatement pstmt = null;
			ResultSet recordRS = null;
			Connection conn = null;

			FileWriter writer = null;
			FileOutputStream fos = null;
			boolean exceptions = false;
			String debug = "";

			try {
				conn = this.getDefaultConn();
				pstmt = this.getPreparedStatement(conn, getRCRecordTable(),
						null, Columns.STRINGIFIED_RECORD.toString(), " where "
								+ Columns.RC_GROUP_NAME.toString() + "='"
								+ rcGroup + "'");
				recordRS = pstmt.executeQuery();

				// first wipe out the file
				(writer = new FileWriter(cpFilename, false)).close();
				;
				// then start appending to the clean slate
				fos = new FileOutputStream(new File(cpFilename));
				// writer = new FileWriter(cpFilename, true);

				while (recordRS.next()) {
					String msg = recordRS.getString(1);
					ReconfigurationRecord<NodeIDType> rcRecord = new ReconfigurationRecord<NodeIDType>(
							new JSONObject(msg), this.consistentNodeConfig);
					fos.write((rcRecord.toString() + "\n").getBytes(CHARSET));
					debug += rcRecord;
				}
			} catch (SQLException | JSONException | IOException e) {
				log.severe(e.getClass().getSimpleName()
						+ " while creating checkpoint for " + rcGroup + ":");
				e.printStackTrace();
				exceptions = true;
			} finally {
				cleanup(pstmt, recordRS);
				cleanup(conn);
				cleanup(writer);
				cleanup(fos);
			}
			log.info(myID + " returning state for " + rcGroup + ":"
					+ this.getCheckpointDir() + rcGroup + " ; wrote to file:\n"
					+ debug);
			// return filename, not actual checkpoint
			return !exceptions
					&& this.deleteOldCheckpoints(rcGroup, cpFilename) ? this
					.getCheckpointURL(cpFilename) : null;
		}
	}

	/*
	 * The second argument "state" here is implemented as the name of the file
	 * where the checkpoint state is actually stored. We assume that each RC
	 * record in the checkpoint state is separated by a newline.
	 */
	@Override
	public synchronized boolean updateState(String rcGroup, String state) {
		synchronized (this.stringLocker.get(rcGroup)) {
			// treat rcGroup as filename and read records from it
			BufferedReader br = null;
			if ((state = this.getRemoteCheckpoint(rcGroup, state)) == null)
				return false;
			try {
				// read state from file
				if (LARGE_CHECKPOINTS_OPTION
						&& state.length() < MAX_FILENAME_LENGTH
						&& (new File(state)).exists()) {
					br = new BufferedReader(new InputStreamReader(
							new FileInputStream(state)));
					String line = null;
					while ((line = br.readLine()) != null) {
						this.putReconfigurationRecordIfNotName(
								new ReconfigurationRecord<NodeIDType>(
										new JSONObject(line),
										this.consistentNodeConfig), rcGroup);
					}
				} else { // state is actually the state
					String[] lines = state.split("\n");
					for (String line : lines) {
						this.putReconfigurationRecordIfNotName(
								new ReconfigurationRecord<NodeIDType>(
										new JSONObject(line),
										this.consistentNodeConfig), rcGroup);
					}
				}
			} catch (IOException | JSONException e) {
				log.severe("Node" + myID + " unable to insert checkpoint");
				e.printStackTrace();
			} finally {
				try {
					if (br != null)
						br.close();
				} catch (IOException e) {
					log.severe("Node" + myID
							+ " unable to close checkpoint file");
					e.printStackTrace();
				}
			}

			return true;
		}
	}

	@Override
	public synchronized String getDemandStats(String name) {
		JSONObject stats = this.getDemandStatsJSON(name);
		return stats != null ? stats.toString() : null;
	}

	@Override
	public String[] getPendingReconfigurations() {
		PreparedStatement pstmt = null;
		ResultSet recordRS = null;
		Connection conn = null;
		Set<String> pending = new HashSet<String>();
		try {
			conn = this.getDefaultConn();
			pstmt = this.getPreparedStatement(conn, getPendingTable(), null,
					Columns.SERVICE_NAME.toString());
			recordRS = pstmt.executeQuery();

			while (recordRS.next()) {
				String name = recordRS.getString(1);
				pending.add(name);
			}
		} catch (SQLException e) {
			log.severe("Node" + this.myID
					+ " SQLException while getting pending reconfigurations");
			e.printStackTrace();
		} finally {
			cleanup(pstmt, recordRS);
			cleanup(conn);
		}
		return pending.toArray(new String[0]);
	}

	/******************** End of overridden methods *********************/

	/************************ Private methods below *********************/

	private boolean setPending(String name, boolean set) {
		String cmd = "insert into " + getPendingTable() + " ("
				+ Columns.SERVICE_NAME.toString() + " ) values (?)";
		if (!set)
			cmd = "delete from " + getPendingTable() + " where "
					+ Columns.SERVICE_NAME.toString() + "=?";

		PreparedStatement insertCP = null;
		Connection conn = null;
		try {
			conn = this.getDefaultConn();
			insertCP = conn.prepareStatement(cmd);
			insertCP.setString(1, name);
			insertCP.executeUpdate();
			conn.commit();
		} catch (SQLException sqle) {
			log.severe("SQLException while modifying pending table using "
					+ cmd);
			sqle.printStackTrace();
		} finally {
			cleanup(insertCP);
			cleanup(conn);
		}
		return true;
	}

	private boolean deleteReconfigurationRecord(String name, String table) {
		String cmd = "delete from " + table + " where "
				+ Columns.SERVICE_NAME.toString() + "=?";
		PreparedStatement insertCP = null;
		Connection conn = null;
		int rowcount = 0;
		try {
			conn = this.getDefaultConn();
			insertCP = conn.prepareStatement(cmd);
			insertCP.setString(1, name);
			rowcount = insertCP.executeUpdate();
			conn.commit();
		} catch (SQLException sqle) {
			log.severe("SQLException while deleting RC record using " + cmd);
			sqle.printStackTrace();
		} finally {
			cleanup(insertCP);
			cleanup(conn);
		}
		return rowcount == 1;
	}

	private synchronized JSONObject getDemandStatsJSON(String name) {
		PreparedStatement pstmt = null;
		ResultSet recordRS = null;
		Connection conn = null;
		JSONObject demandStats = null;
		try {
			conn = this.getDefaultConn();
			pstmt = this.getPreparedStatement(conn, getDemandTable(), name,
					Columns.DEMAND_PROFILE.toString());
			recordRS = pstmt.executeQuery();

			assert (!recordRS.isClosed());
			while (recordRS.next()) {
				String msg = recordRS.getString(1);
				demandStats = new JSONObject(msg);
			}
		} catch (SQLException | JSONException e) {
			log.severe((e instanceof SQLException ? "SQL" : "JSON")
					+ "Exception while getting slot for " + name + ":");
			e.printStackTrace();
		} finally {
			cleanup(pstmt, recordRS);
			cleanup(conn);
		}
		return demandStats;
	}

	private boolean shouldCombineStats() {
		return COMBINE_STATS;
	}

	private synchronized Connection getDefaultConn() throws SQLException {
		return dataSource.getConnection();
	}

	// synchronized coz it should be called just onece
	private synchronized boolean initialize() {
		if (!isClosed())
			return true;
		if (!connectDB() || !createTables())
			return false;
		if (!this.makeCheckpointTransferDir())
			return false;
		setClosed(false); // setting open
		initAdjustSoftNodeConfig();
		return initCheckpointServer();
	}

	private boolean initCheckpointServer() {
		try {
			this.serverSock = new ServerSocket();
			this.serverSock.bind(new InetSocketAddress(
					this.consistentNodeConfig.getNodeAddress(myID), 0));
			;
			(new Thread(new CheckpointServer())).start();
			return true;
		} catch (IOException e) {
			log.severe(this
					+ " unable to open server socket for large checkpoint transfers");
			e.printStackTrace();
		}
		return false;
	}

	private class CheckpointServer implements Runnable {

		@Override
		public void run() {
			Socket sock = null;
			try {
				while ((sock = serverSock.accept()) != null) {
					(new Thread(new CheckpointTransporter(sock))).start();
				}
			} catch (IOException e) {
				log.severe(myID
						+ " incurred IOException while processing checkpoint transfer request");
				e.printStackTrace();
			}
		}
	}

	private class CheckpointTransporter implements Runnable {

		final Socket sock;

		CheckpointTransporter(Socket sock) {
			this.sock = sock;
		}

		@Override
		public void run() {
			transferCheckpoint(sock);
		}
	}

	// synchronized prevents concurrent file delete
	private synchronized void transferCheckpoint(Socket sock) {
		BufferedReader brSock = null, brFile = null;
		try {
			brSock = new BufferedReader(new InputStreamReader(
					sock.getInputStream()));
			// first and only line is request
			String request = brSock.readLine();
			if ((new File(request).exists())) {
				// request is filename
				brFile = new BufferedReader(new InputStreamReader(
						new FileInputStream(request)));
				// file successfully open if here
				OutputStream outStream = sock.getOutputStream();
				String line = null; // each line is a record
				while ((line = brFile.readLine()) != null)
					outStream.write(line.getBytes(CHARSET));
				outStream.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (brSock != null)
					brSock.close();
				if (brFile != null)
					brFile.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	private String getRemoteCheckpoint(String rcGroup, String url) {
		if (url == null)
			return url;
		String filename = url;
		JSONObject jsonUrl;
		try {
			jsonUrl = new JSONObject(url);
			if (jsonUrl.has(Keys.INET_SOCKET_ADDRESS.toString())
					&& jsonUrl.has(Keys.FILENAME.toString())) {
				filename = jsonUrl.getString(Keys.FILENAME.toString());
				File file = new File(filename);
				if (!file.exists()) { // fetch from remote
					InetSocketAddress sockAddr = Util
							.getInetSocketAddressFromString(jsonUrl
									.getString(Keys.INET_SOCKET_ADDRESS
											.toString()));
					assert (sockAddr != null);
					filename = this.getRemoteCheckpoint(rcGroup, sockAddr, filename, jsonUrl.getLong(Keys.FILESIZE.toString()));
				}
			}
		} catch (JSONException e) {
			// do nothing, will return filename
		}
		return filename;
	}

	// FIXME: crash may result in half written file
	private synchronized String getRemoteCheckpoint(String rcGroupName,
			InetSocketAddress sockAddr, String remoteFilename, long fileSize) {
		String request = remoteFilename + "\n";
		Socket sock = null;
		FileOutputStream fos = null;
		String localCPFilename = null;
		try {
			sock = new Socket(sockAddr.getAddress(), sockAddr.getPort());
			sock.getOutputStream().write(request.getBytes(CHARSET));
			InputStream inStream = (sock.getInputStream());
			if (!this.createCheckpointFile(localCPFilename = this
					.getCheckpointFile(rcGroupName)))
				return null;
			fos = new FileOutputStream(new File(localCPFilename));
			byte[] buf = new byte[1024];
			int nread = 0;
			int nTotalRead = 0;
			// read from sock, write to file
			while ((nread = inStream.read(buf)) >= 0) {
				nTotalRead += nread;
				fos.write(buf, 0, nread);
			}
			// check exact expected file size
			if (nTotalRead != fileSize)
				localCPFilename = null;
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (fos != null)
					fos.close();
				if (sock != null)
					sock.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return localCPFilename;
	}

	private String getCheckpointURL(String localFilename) {
		String url = null;
		try {
			JSONObject jsonUrl = new JSONObject();
			jsonUrl.put(Keys.INET_SOCKET_ADDRESS.toString(),
					new InetSocketAddress(this.serverSock.getInetAddress(),
							this.serverSock.getLocalPort()));
			jsonUrl.put(Keys.FILENAME.toString(), localFilename);
			long fileSize = (new File(localFilename)).length();
			jsonUrl.put(Keys.FILESIZE.toString(), fileSize);
			url = jsonUrl.toString();
		} catch (JSONException je) {
			log.severe(this
					+ " incurred JSONException while encoding URL for filename "
					+ localFilename);
		}
		return url;
	}

	private boolean makeCheckpointTransferDir() {
		File cpDir = new File(getCheckpointDir());
		if (!cpDir.exists())
			return cpDir.mkdirs();
		return true;
	}

	private String getCheckpointDir() {
		return this.logDirectory + CHECKPOINT_TRANSFER_DIR + "/" + myID + "/";
	}

	private String getCheckpointFile(String rcGroupName) {
		return this.getCheckpointDir() + rcGroupName + "."
				+ System.currentTimeMillis(); // + ":" + epoch;
	}

	/*
	 * Deletes all but the most recent checkpoint for the RC group name. We
	 * could track recency based on timestamps using either the timestamp in the
	 * filename or the OS file creation time. Here, we just supply the latest
	 * checkpoint filename explicitly as we know it when this method is called
	 * anyway.
	 */
	private boolean deleteOldCheckpoints(final String rcGroupName,
			String latestCPFilename) {
		File dir = new File(getCheckpointDir());
		assert (dir.exists());
		// get files matching the prefix for this rcGroupName's checkpoints
		File[] foundFiles = dir.listFiles(new FilenameFilter() {
			public boolean accept(File dir, String name) {
				return name.startsWith(rcGroupName);
			}
		});
		// delete all but the most recent
		boolean allDeleted = true;
		for (Filename f : this.getAllButLatestTwo(foundFiles))
			allDeleted = allDeleted && this.deleteFile(f.file);
		return allDeleted;
	}

	private synchronized boolean deleteFile(File f) {
		return f.delete();
	}

	private Set<Filename> getAllButLatestTwo(File[] files) {
		TreeSet<Filename> allFiles = new TreeSet<Filename>();
		TreeSet<Filename> oldFiles = new TreeSet<Filename>();
		for (File file : files)
			allFiles.add(new Filename(file));
		if (allFiles.size() <= 2)
			return oldFiles;
		Iterator<Filename> iter = allFiles.iterator();
		for (int i = 0; i < allFiles.size() - 2; i++)
			oldFiles.add(iter.next());

		return oldFiles;
	}

	private class Filename implements Comparable<Filename> {
		final File file;

		Filename(File f) {
			this.file = f;
		}

		@Override
		public int compareTo(
				DerbyPersistentReconfiguratorDB<NodeIDType>.Filename o) {
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

	public Map<String, Set<NodeIDType>> getRCGroups() {
		Set<String> rcGroupNames = this.getRCGroupNames();
		Map<String, Set<NodeIDType>> rcGroups = new HashMap<String, Set<NodeIDType>>();
		for (String rcGroupName : rcGroupNames) {
			Set<NodeIDType> groupMembers = this.getReconfigurationRecord(
					rcGroupName).getActiveReplicas();
			rcGroups.put(rcGroupName, groupMembers);
		}
		return rcGroups;
	}

	public Set<String> getRCGroupNames() {
		Set<String> groups = null;
		PreparedStatement pstmt = null;
		ResultSet recordRS = null;
		Connection conn = null;
		try {
			conn = this.getDefaultConn();
			/*
			 * Selects all RC group name records by asking for records
			 * where service name equals RC group name. The index on RC group
			 * names should make this query efficient but this performance needs
			 * to be tested at scale.
			 */
			pstmt = this.getPreparedStatement(
					conn,
					this.getRCRecordTable(),
					null,
					Columns.SERVICE_NAME.toString(),
					" where " + Columns.SERVICE_NAME.toString()
							+ " in (select distinct "
							+ Columns.RC_GROUP_NAME.toString() + " from "
							+ this.getRCRecordTable() + ")");
			recordRS = pstmt.executeQuery();
			while (recordRS.next()) {
				(groups == null ? (groups = new HashSet<String>()) : groups)
						.add(recordRS.getString(1));
			}
		} catch (SQLException e) {
			log.severe("SQLException while getting RC group names: ");
			e.printStackTrace();
		} finally {
			cleanup(pstmt, recordRS);
			cleanup(conn);
		}
		// must not return the nodeConfig record itself as an RC group
		groups.remove(AbstractReconfiguratorDB.RecordNames.NODE_CONFIG
				.toString());
		return groups;
	}

	/*
	 * A reconfigurator needs two tables: one for all records and one for all
	 * records with ongoing reconfigurations. A record contains the serviceName,
	 * rcGroupName, epoch, actives, newActives, state, and demandProfile. We
	 * could also move demandProfile to a separate table. We need to store
	 * demandProfile persistently as otherwise we will run out of memory.
	 */
	private boolean createTables() {
		boolean createdRCRecordTable = false, createdPendingTable = false, createdDemandTable = false, createdNodeConfigTable = false;
		// simply store everything in stringified form and pull all when needed
		String cmdRCR = "create table " + getRCRecordTable() + " ("
				+ Columns.SERVICE_NAME.toString() + " varchar(" + MAX_NAME_SIZE
				+ ") not null, " + Columns.RC_GROUP_NAME.toString()
				+ " varchar(" + MAX_NAME_SIZE + ") not null,  "
				+ Columns.STRINGIFIED_RECORD.toString()
				+ (RC_RECORD_CLOB_OPTION ? " clob(" : " varchar(")
				+ MAX_RC_RECORD_SIZE + "), primary key ("
				+ Columns.SERVICE_NAME + "))";
		// index based on rc group name for optimizing checkpointing
		String cmdRCRCI = "create index rc_group_index on "
				+ getRCRecordTable() + "(" + Columns.RC_GROUP_NAME.toString()
				+ ")";
		// records with ongoing reconfigurations
		String cmdP = "create table " + getPendingTable() + " ("
				+ Columns.SERVICE_NAME.toString() + " varchar(" + MAX_NAME_SIZE
				+ ") not null, primary key(" + Columns.SERVICE_NAME.toString()
				+ "))";
		// demand profiles of records, need not be fault-tolerant or precise
		String cmdDP = "create table " + getDemandTable() + " ("
				+ Columns.SERVICE_NAME.toString() + " varchar(" + MAX_NAME_SIZE
				+ ") not null, " + Columns.DEMAND_PROFILE.toString()
				+ (DEMAND_PROFILE_CLOB_OPTION ? " clob(" : " varchar(")
				+ MAX_DEMAND_PROFILE_SIZE + "), primary key("
				+ Columns.SERVICE_NAME.toString() + "))";
		// node config information, needs to be correct under faults
		String cmdNC = "create table " + getNodeConfigTable() + " ("
				+ Columns.RC_NODE_ID.toString() + " varchar(" + MAX_NAME_SIZE
				+ ") not null, " + Columns.INET_ADDRESS.toString()
				+ " varchar(256), " + Columns.PORT.toString() + " int, "
				+ Columns.NODE_CONFIG_VERSION.toString() + " int, primary key("
				+ Columns.RC_NODE_ID.toString() + ", "
				+ Columns.NODE_CONFIG_VERSION + "))";

		log.info(cmdDP);
		Statement stmt = null;
		Connection conn = null;
		try {
			conn = this.getDefaultConn();
			stmt = conn.createStatement();
			createdRCRecordTable = createTable(stmt, cmdRCR, getRCRecordTable())
					&& createIndex(stmt, cmdRCRCI, getRCRecordTable());
			createdPendingTable = createTable(stmt, cmdP, getRCRecordTable());
			createdDemandTable = createTable(stmt, cmdDP, getDemandTable());
			createdNodeConfigTable = createTable(stmt, cmdNC,
					getNodeConfigTable());
			log.log(Level.INFO, "{0}{1}{2}{3}", new Object[] {
					"Created tables ", getRCRecordTable(), " and ",
					getPendingTable() });
		} catch (SQLException sqle) {
			log.severe("Could not create table(s): "
					+ (createdRCRecordTable ? "" : getRCRecordTable()) + " "
					+ (createdPendingTable ? "" : getPendingTable())
					+ (createdDemandTable ? "" : getDemandTable()) + " "
					+ (createdNodeConfigTable ? "" : getNodeConfigTable())
					+ " ");
			sqle.printStackTrace();
		} finally {
			cleanup(stmt);
			cleanup(conn);
		}
		return createdRCRecordTable && createdPendingTable;
	}

	private boolean createTable(Statement stmt, String cmd, String table) {
		boolean created = false;
		try {
			stmt.execute(cmd);
			created = true;
		} catch (SQLException sqle) {
			if (sqle.getSQLState().equals(DUPLICATE_TABLE)) {
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
		props.put("user", USER + this.myID);
		props.put("password", PASSWORD);
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
			Connection conn = null;
			try {
				connAttempts++;
				log.info("Attempting getDefaultConn() to db " + dbCreation);
				getDefaultConn(); // open first connection
				log.info("Connected to and created database " + DATABASE);
				connected = true;
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
				cleanup(conn); // close the test connection
			}
		}
		return connected;
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

	private static void addDerbyPersistentReconfiguratorDB(
			DerbyPersistentReconfiguratorDB<?> rcDB) {
		synchronized (DerbyPersistentReconfiguratorDB.instances) {
			if (!DerbyPersistentReconfiguratorDB.instances.contains(rcDB))
				DerbyPersistentReconfiguratorDB.instances.add(rcDB);
		}
	}

	private static boolean allClosed() {
		synchronized (DerbyPersistentReconfiguratorDB.instances) {
			for (DerbyPersistentReconfiguratorDB<?> rcDB : instances) {
				if (!rcDB.isClosed())
					return false;
			}
			return true;
		}
	}

	public void close() {
		setClosed(true);
		if (allClosed())
			closeGracefully();
	}

	/*
	 * This method is empty because embedded derby can be close exactly once. We
	 * also use it inside paxos, so it is best to let that do the close.
	 */
	private void closeGracefully() {
		// do nothing
	}

	private synchronized boolean isClosed() {
		return closed;
	}

	private synchronized void setClosed(boolean c) {
		closed = c;
	}

	private String getRCRecordTable() {
		return RECONFIGURATION_RECORD_TABLE + this.myID;
	}

	private String getPendingTable() {
		return PENDING_TABLE + this.myID;
	}

	private String getDemandTable() {
		return DEMAND_PROFILE_TABLE + this.myID;
	}

	private String getNodeConfigTable() {
		return NODE_CONFIG_TABLE + this.myID;
	}

	private void cleanup(FileWriter writer) {
		try {
			if (writer != null)
				writer.close();
		} catch (IOException ioe) {
			log.severe("Could not close file writer " + writer);
			ioe.printStackTrace();
		}
	}

	private void cleanup(FileOutputStream fos) {
		try {
			if (fos != null)
				fos.close();
		} catch (IOException ioe) {
			log.severe("Could not close file writer " + fos);
			ioe.printStackTrace();
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

	private PreparedStatement getPreparedStatement(Connection conn,
			String table, String name, String column, String fieldConstraints)
			throws SQLException {
		String cmd = "select "
				+ column
				+ " from "
				+ table
				+ (name != null ? " where " + Columns.SERVICE_NAME.toString()
						+ "=?" : "");
		cmd += (fieldConstraints != null ? fieldConstraints : "");
		PreparedStatement getCPState = (conn != null ? conn : this
				.getDefaultConn()).prepareStatement(cmd);
		if (name != null)
			getCPState.setString(1, name);
		return getCPState;
	}

	private PreparedStatement getPreparedStatement(Connection conn,
			String table, String name, String column) throws SQLException {
		return this.getPreparedStatement(conn, table, name, column, "");
	}

	private static void testRCRecordReadAfterWrite(String name,
			DerbyPersistentReconfiguratorDB<Integer> rcDB) {
		int groupSize = (int) Math.random() * 10;
		Set<Integer> newActives = new HashSet<Integer>();
		for (int i = 0; i < groupSize; i++)
			newActives.add((int) Math.random() * 100);
		ReconfigurationRecord<Integer> rcRecord = new ReconfigurationRecord<Integer>(
				name, 0, newActives);
		rcDB.putReconfigurationRecord(rcRecord);
		ReconfigurationRecord<Integer> retrievedRecord = rcDB
				.getReconfigurationRecord(name);
		assert (retrievedRecord.equals(rcRecord)) : rcRecord + " != "
				+ retrievedRecord;
	}

	private static InterfaceRequest getRandomInterfaceRequest(final String name) {
		return new InterfaceRequest() {

			@Override
			public IntegerPacketType getRequestType()
					throws RequestParseException {
				return AppRequest.PacketType.DEFAULT_APP_REQUEST;
			}

			@Override
			public String getServiceName() {
				return name;
			}
		};
	}

	private static InetAddress getRandomIPAddress() throws UnknownHostException {
		return InetAddress.getByName((int) Math.random() * 256 + "."
				+ (int) Math.random() * 256 + "." + (int) Math.random() * 256
				+ "." + (int) Math.random() * 256);
	}

	private static void testDemandProfileUpdate(String name,
			DerbyPersistentReconfiguratorDB<Integer> rcDB) {
		AbstractDemandProfile demandProfile = new DemandProfile(name);
		int numRequests = 20;
		// fake random request demand
		for (int i = 0; i < numRequests; i++) {
			try {
				demandProfile.register(getRandomInterfaceRequest(name),
						getRandomIPAddress(), null);
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}
		DemandReport<Integer> demandReport = new DemandReport<Integer>(
				(int) Math.random() * 100, name, 0, demandProfile);
		// insert demand profile
		rcDB.updateDemandStats(demandReport);
	}

	private static JSONObject combineStats(JSONObject historic,
			JSONObject update) {
		AbstractDemandProfile historicProfile = AbstractDemandProfile
				.createDemandProfile(historic);
		AbstractDemandProfile updateProfile = AbstractDemandProfile
				.createDemandProfile(update);
		historicProfile.combine(updateProfile);
		return historicProfile.getStats();
	}

	private synchronized boolean createCheckpointFile(String filename) {
		File file = new File(filename);
		try {
			file.createNewFile(); // will create only if not exists
		} catch (IOException e) {
			log.severe("Unable to create checkpoint file for " + filename);
			e.printStackTrace();
		}
		return (file.exists());
	}

	public String toString() {
		return "DerbyRCDB" + myID;
	}

	public static void main(String[] args) {
		Util.assertAssertionsEnabled();
		ReconfigurableSampleNodeConfig nc = new ReconfigurableSampleNodeConfig();
		nc.localSetup(TestConfig.getNodes());
		ConsistentReconfigurableNodeConfig<Integer> consistentNodeConfig = new ConsistentReconfigurableNodeConfig<Integer>(
				nc);
		DerbyPersistentReconfiguratorDB<Integer> rcDB = new DerbyPersistentReconfiguratorDB<Integer>(
				consistentNodeConfig.getReconfigurators().iterator().next(),
				consistentNodeConfig);
		String name = "name0";

		int numTests = 100;
		for (int i = 0; i < numTests; i++)
			testRCRecordReadAfterWrite(name, rcDB);
		for (int i = 0; i < numTests; i++)
			testDemandProfileUpdate(name, rcDB);

		System.out.println(rcDB.getRCGroupNames());
	}

	@Override
	public boolean addReconfigurator(NodeIDType node,
			InetSocketAddress sockAddr, int version) {
		String cmd = "insert into " + getNodeConfigTable() + " ("
				+ Columns.RC_NODE_ID.toString() + ", "
				+ Columns.INET_ADDRESS.toString() + ", "
				+ Columns.PORT.toString() + ", "
				+ Columns.NODE_CONFIG_VERSION.toString()
				+ " ) values (?,?,?,?)";

		PreparedStatement insertCP = null;
		Connection conn = null;
		boolean added = false;
		try {
			conn = this.getDefaultConn();
			insertCP = conn.prepareStatement(cmd);
			insertCP.setString(1, node.toString());
			insertCP.setString(2, sockAddr.getAddress().getHostAddress());
			insertCP.setInt(3, sockAddr.getPort());
			insertCP.setInt(4, version);
			insertCP.executeUpdate();
			conn.commit();
			added = true;
		} catch (SQLException sqle) {
			if (!sqle.getSQLState().equals(DUPLICATE_KEY)) {
				log.severe("SQLException while inserting RC record using "
						+ cmd);
				sqle.printStackTrace();
			}
		} finally {
			cleanup(insertCP);
			cleanup(conn);
		}
		return added;
	}

	@Override
	public boolean garbageCollectOldReconfigurators(int version) {
		String cmd = "delete from " + getNodeConfigTable() + " where "
				+ Columns.NODE_CONFIG_VERSION.toString() + "=?";

		PreparedStatement insertCP = null;
		Connection conn = null;
		boolean removed = false;
		try {
			conn = this.getDefaultConn();
			insertCP = conn.prepareStatement(cmd);
			insertCP.setInt(1, version);
			insertCP.executeUpdate();
			conn.commit();
			removed = true;
		} catch (SQLException sqle) {
			log.severe("SQLException while deleting node config version "
					+ version);
			sqle.printStackTrace();
		} finally {
			cleanup(insertCP);
			cleanup(conn);
		}
		return removed;
	}

	private Integer getMaxNodeConfigVersion() {
		String cmd = "select max(" + Columns.NODE_CONFIG_VERSION.toString()
				+ ") from " + this.getNodeConfigTable();

		Integer maxVersion = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Connection conn = null;
		try {
			conn = this.getDefaultConn();
			pstmt = conn.prepareStatement(cmd);
			rs = pstmt.executeQuery();

			while (rs.next()) {
				maxVersion = rs.getInt(1);
			}
		} catch (SQLException e) {
			log.severe("SQLException while getting max node config version");
			e.printStackTrace();
		} finally {
			cleanup(pstmt, rs);
			cleanup(conn);
		}
		return maxVersion;
	}

	@Override
	public Map<NodeIDType, InetSocketAddress> getRCNodeConfig(boolean maxOnly) {
		Integer version = this.getMaxNodeConfigVersion();
		if (version == null)
			return null;

		Map<NodeIDType, InetSocketAddress> rcMap = new HashMap<NodeIDType, InetSocketAddress>();
		String cmd = "select " + Columns.RC_NODE_ID.toString() + ", "
				+ Columns.INET_ADDRESS.toString() + " from "
				+ this.getNodeConfigTable() + " where "
				+ Columns.NODE_CONFIG_VERSION.toString() + (maxOnly ? "=?" : ">=?");

		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Connection conn = null;
		try {
			conn = this.getDefaultConn();
			pstmt = conn.prepareStatement(cmd);
			pstmt.setInt(1, (maxOnly ? version : (version-1)));
			rs = pstmt.executeQuery();

			while (rs.next()) {
				NodeIDType node = this.consistentNodeConfig.valueOf(rs
						.getString(1));
				InetSocketAddress sockAddr = Util
						.getInetSocketAddressFromString(rs.getString(2));
				rcMap.put(node, sockAddr);
			}
		} catch (SQLException e) {
			log.severe("SQLException while getting RC node config");
			e.printStackTrace();
		} finally {
			cleanup(pstmt, rs);
			cleanup(conn);
		}
		return rcMap;
	}

	/* This method imports consistentNodeConfig from the persistent copy.
	 * The persistent copy has both the latest (unstable) version and
	 * the previous version. At the end of this method, 
	 * consistentNodeConfig has the latest version nodes as well as the
	 * previous version nodes with nodes present in the latter but not
	 * in the former slated for removal.
	 */
	private void initAdjustSoftNodeConfig() {
		Map<NodeIDType, InetSocketAddress> rcMapAll = this
				.getRCNodeConfig(false);
		ReconfigurationRecord<NodeIDType> ncRecord = this
				.getReconfigurationRecord(AbstractReconfiguratorDB.RecordNames.NODE_CONFIG
						.toString());
		if(ncRecord==null) return;
		// else
		Set<NodeIDType> latestRCs = ncRecord.getNewActives();
		if (rcMapAll == null || rcMapAll.isEmpty())
			return;
		// add all nodes
		for (NodeIDType node : rcMapAll.keySet())
			this.consistentNodeConfig.addReconfigurator(node,
					rcMapAll.get(node));
		for (NodeIDType node : this.consistentNodeConfig.getReconfigurators())
			// remove outdated consistentNodeConfig entries
			if (!rcMapAll.containsKey(node))
				this.consistentNodeConfig.removeReconfigurator(node);
			// slate for removal the difference between all and latest
			else if (!latestRCs.contains(node))
				this.consistentNodeConfig.slateForRemovalReconfigurator(node);
	}

	/*
	 * The double synchronized is because we need to synchronize over "this" to
	 * perform testAndSet checks over record, but we have to do that before,
	 * never after, stringLocker lock.
	 */
	@Override
	public boolean mergeState(String rcGroupName, int epoch, String mergee,
			String state) {
		synchronized (this.stringLocker.get(rcGroupName)) {
			synchronized (this) {
				ReconfigurationRecord<NodeIDType> record = this
						.getReconfigurationRecord(rcGroupName);

				if (record.getEpoch() == epoch
						&& !record.mergedContains(mergee)) 
					if (this.updateState(rcGroupName, state)) {
						record.insertMerged(mergee);
						this.putReconfigurationRecord(record);
					}
				return false;
			}
		}
	}

	@Override
	public synchronized void clearMerged(String rcGroupName, int epoch) {
		ReconfigurationRecord<NodeIDType> record = this
				.getReconfigurationRecord(rcGroupName);
		record.clearMerged();
		this.putReconfigurationRecord(record);
	}

	@Override
	public void setRCEpochs(ReconfigurationRecord<NodeIDType> ncRecord) {
		if (!ncRecord.getName().equals(
				AbstractReconfiguratorDB.RecordNames.NODE_CONFIG.toString()))
			return;
		this.putReconfigurationRecord(ncRecord);
	}
}
