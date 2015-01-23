package edu.umass.cs.gns.reconfiguration;

import java.beans.PropertyVetoException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.json.JSONException;
import org.json.JSONObject;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import edu.umass.cs.gns.nio.IntegerPacketType;
import edu.umass.cs.gns.reconfiguration.examples.AppRequest;
import edu.umass.cs.gns.reconfiguration.examples.ReconfigurableSampleNodeConfig;
import edu.umass.cs.gns.reconfiguration.examples.TestConfig;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.DemandReport;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.AbstractDemandProfile;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.DemandProfile;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.ReconfigurationRecord;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.ReconfigurationRecord.RCStates;
import edu.umass.cs.gns.util.MyLogger;
import edu.umass.cs.gns.util.Util;

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
	// private static final String DUPLICATE_KEY = "23505";
	private static final String DUPLICATE_TABLE = "X0Y32";
	// private static final String NONEXISTENT_TABLE = "42Y07";
	private static final String USER = "user";
	private static final String PASSWORD = "user";
	private static final String DATABASE = "reconfiguration_DB";
	private static final String RECONFIGURATION_RECORD_TABLE = "checkpoint";
	private static final String PENDING_TABLE = "messages";
	private static final String DEMAND_PROFILE_TABLE = "demand";
	// private static final boolean DISABLE_LOGGING = false;
	private static final boolean AUTO_COMMIT = true;
	private static final int MAX_POOL_SIZE = 100;
	private static final int MAX_NAME_SIZE = 40;
	private static final int MAX_RC_RECORD_SIZE = 4096;
	private static final int MAX_DEMAND_PROFILE_SIZE = 4096;
	private static final boolean RC_RECORD_CLOB_OPTION = MAX_RC_RECORD_SIZE > 4096;
	private static final boolean DEMAND_PROFILE_CLOB_OPTION = MAX_DEMAND_PROFILE_SIZE > 4096;
	private static final boolean COMBINE_STATS = false;
	private static final String CHECKPOINT_TRANSFER_DIR = "checkpoints";

	private static enum Columns {
		SERVICE_NAME, EPOCH, RC_GROUP_NAME, ACTIVES, NEW_ACTIVES, RC_STATE, STRINGIFIED_RECORD, DEMAND_PROFILE
	};

	private static final ArrayList<DerbyPersistentReconfiguratorDB<?>> instances = new ArrayList<DerbyPersistentReconfiguratorDB<?>>();

	public String logDirectory;

	private ComboPooledDataSource dataSource = null;

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
				// + Columns.RC_GROUP_NAME.toString() + ", "
				+ Columns.DEMAND_PROFILE.toString() + ", "
				+ Columns.SERVICE_NAME.toString() + " ) values (?,?)";
		String updateCmd = "update " + getDemandTable() + " set "
				// + Columns.RC_GROUP_NAME.toString() + "=?, "
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
			// insertCP.setString(1,
			// this.consistentNodeConfig.getFirstReconfigurator(report.getServiceName()).toString());
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
		// FIXME: might as well return void
		return true;
	}

	// The one method where all state changes are done.
	@Override
	public synchronized boolean setState(String name, int epoch,
			ReconfigurationRecord.RCStates state) {
		ReconfigurationRecord<NodeIDType> record = this
				.getReconfigurationRecord(name);
		if (record == null)
			return false;

		log.log(Level.INFO,
				MyLogger.FORMAT[8],
				new Object[] { "==============================> DerbyRCDB",
						myID, record.getName(), record.getEpoch(),
						record.getState(), " ->", epoch, state,
						record.getNewActives() });
		record.setState(name, epoch, state);
		if (state.equals(RCStates.READY)) {
			record.setActivesToNewActives();
			this.setPending(name, false);
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
		if (!record.getState().equals(RCStates.READY))
			return false;
		assert (state.equals(RCStates.WAIT_ACK_STOP));
		log.log(Level.INFO,
				MyLogger.FORMAT[8],
				new Object[] { "==============================> DerbyRCDB",
						myID, record.getName(), record.getEpoch(),
						record.getState(), " ->", epoch, state,
						record.getNewActives() });
		record.setState(name, epoch, state);
		this.setPending(name, true);
		this.putReconfigurationRecord(record);
		return true;
	}

	public synchronized void putReconfigurationRecord(
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
			insertCP.setString(1, this.consistentNodeConfig
					.getFirstReconfigurator(rcRecord.getName()).toString());
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
	}

	@Override
	public ReconfigurationRecord<NodeIDType> deleteReconfigurationRecord(
			String name) {
		ReconfigurationRecord<NodeIDType> record = this
				.deleteReconfigurationRecord(name, this.getRCRecordTable());
		this.deleteReconfigurationRecord(name, this.getPendingTable());
		this.deleteReconfigurationRecord(name, this.getDemandTable());
		return record;
	}

	@Override
	public synchronized Integer getEpoch(String name) {
		ReconfigurationRecord<NodeIDType> record = this
				.getReconfigurationRecord(name);
		return record.getEpoch();
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
	/*
	 * FIXME: Complete methods below. We probably need to dump the table to a
	 * file and then read it back from a dump file to implement these methods in
	 * a robust manner.
	 */

	// write records to a file and return filename
	@Override
	public String getState(String rcGroup) {
		if (!this.createCheckpointFile(rcGroup))
			return null;

		PreparedStatement pstmt = null;
		ResultSet recordRS = null;
		Connection conn = null;
		FileWriter writer = null;
		try {
			conn = this.getDefaultConn();
			pstmt = this.getPreparedStatement(conn, getRCRecordTable(),
					rcGroup, Columns.STRINGIFIED_RECORD.toString());
			recordRS = pstmt.executeQuery();

			writer = new FileWriter(getCheckpointFile(rcGroup), true);

			while (recordRS.next()) {
				String msg = recordRS.getString(1);
				ReconfigurationRecord<NodeIDType> rcRecord = new ReconfigurationRecord<NodeIDType>(
						new JSONObject(msg), this.consistentNodeConfig);
				writer.write(rcRecord.toString() + "\n"); // append record to
															// file
			}
		} catch (SQLException | JSONException | IOException e) {
			log.severe(e.getClass().getSimpleName()
					+ " while creating checkpoint for " + rcGroup + ":");
			e.printStackTrace();
		} finally {
			cleanup(pstmt, recordRS);
			cleanup(conn);
			cleanup(writer);
		}
		// return filename, not actual checkpoint
		return this.getCheckpointDir() + rcGroup;
	}

	@Override
	public boolean updateState(String rcGroup, String state) {
		// treat rcGroup as filename and read records from it
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(
					getCheckpointFile(rcGroup))));
			String line = null;
			while ((line = br.readLine()) != null) {
				this.putReconfigurationRecord(new ReconfigurationRecord<NodeIDType>(
						new JSONObject(line), this.consistentNodeConfig));
			}
		} catch (IOException | JSONException e) {
			log.severe("Node" + myID + " unable to insert checkpoint");
			e.printStackTrace();
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				log.severe("Node" + myID + " unable to close checkpoint file");
				e.printStackTrace();
			}
		}

		return true;
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

	private ReconfigurationRecord<NodeIDType> deleteReconfigurationRecord(
			String name, String table) {
		String cmd = "delete from " + table + " where "
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
			log.severe("SQLException while deleting RC record using " + cmd);
			sqle.printStackTrace();
		} finally {
			cleanup(insertCP);
			cleanup(conn);
		}
		return null;
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
		return true;
	}

	private boolean makeCheckpointTransferDir() {
		File cpDir = new File(this.logDirectory + CHECKPOINT_TRANSFER_DIR);
		if (!cpDir.exists())
			return cpDir.mkdirs();
		return true;
	}

	private String getCheckpointDir() {
		return this.logDirectory + CHECKPOINT_TRANSFER_DIR + "/";
	}

	private String getCheckpointFile(String rcGroup) {
		return this.getCheckpointDir() + rcGroup;
	}

	/*
	 * A reconfigurator needs two tables: one for all records and one for all
	 * records with ongoing reconfigurations. A record contains the serviceName,
	 * rcGroupName, epoch, actives, newActives, state, and demandProfile. We
	 * could also move demandProfile to a separate table. We need to store
	 * demandProfile persistently as otherwise we will run out of memory.
	 */
	private boolean createTables() {
		boolean createdRCRecordTable = false, createdPendingTable = false, createdDemandTable = false;
		// simply store everything in stringified form and pull all when needed
		String cmdRCR = "create table " + getRCRecordTable() + " ("
				+ Columns.SERVICE_NAME.toString() + " varchar(" + MAX_NAME_SIZE
				+ ") not null, " + Columns.RC_GROUP_NAME.toString()
				+ " varchar(" + MAX_NAME_SIZE + ") not null,  "
				+ Columns.STRINGIFIED_RECORD.toString()
				+ (RC_RECORD_CLOB_OPTION ? " clob(" : " varchar(")
				+ MAX_RC_RECORD_SIZE + "), primary key ("
				+ Columns.SERVICE_NAME + "))";
		String cmdRCRCI = "create index rc_group_index on "
				+ getRCRecordTable() + "(" + Columns.RC_GROUP_NAME.toString()
				+ ")";
		String cmdP = "create table " + getPendingTable() + " ("
				+ Columns.SERVICE_NAME.toString() + " varchar(" + MAX_NAME_SIZE
				+ ") not null, primary key(" + Columns.SERVICE_NAME.toString()
				+ "))";
		String cmdDP = "create table " + getDemandTable() + " ("
				+ Columns.SERVICE_NAME.toString() + " varchar(" + MAX_NAME_SIZE
				+ ") not null, " + Columns.DEMAND_PROFILE.toString()
				+ (DEMAND_PROFILE_CLOB_OPTION ? " clob(" : " varchar(")
				+ MAX_DEMAND_PROFILE_SIZE + "), primary key("
				+ Columns.SERVICE_NAME.toString() + "))";

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
			log.log(Level.INFO, "{0}{1}{2}{3}", new Object[] {
					"Created tables ", getRCRecordTable(), " and ",
					getPendingTable() });
		} catch (SQLException sqle) {
			log.severe("Could not create table(s): "
					+ (createdRCRecordTable ? "" : getRCRecordTable()) + " "
					+ (createdPendingTable ? "" : getPendingTable())
					+ (createdDemandTable ? "" : getDemandTable()) + " "
					+ cmdDP);
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

	public void closeImpl() {
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

	private void cleanup(FileWriter writer) {
		try {
			if (writer != null)
				writer.close();
		} catch (IOException ioe) {
			log.severe("Could not close file writer " + writer);
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

	private static InterfaceRequest getRandomInterfaceRequest(String name) {
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
						getRandomIPAddress());
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}
		DemandReport<Integer> demandReport = new DemandReport<Integer>(
				(int) Math.random() * 100, name, 0, demandProfile);
		// insert demand profile
		rcDB.updateDemandStats(demandReport);
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

	private boolean createCheckpointFile(String rcGroup) {
		File file = new File(this.getCheckpointDir() + rcGroup);
		try {
			file.createNewFile(); // will create only if exists
		} catch (IOException e) {
			log.severe("Unable to create checkpoint file for " + rcGroup);
			e.printStackTrace();
		}
		return (file.exists());
	}

}
