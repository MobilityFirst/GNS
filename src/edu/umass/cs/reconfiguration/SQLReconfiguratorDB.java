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

import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.gigapaxos.SQLPaxosLogger;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxosutil.SQL;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurationConfig.RC;
import edu.umass.cs.reconfiguration.examples.AppRequest;
import edu.umass.cs.reconfiguration.interfaces.ReconfiguratorDB;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DemandReport;
import edu.umass.cs.reconfiguration.reconfigurationutils.AbstractDemandProfile;
import edu.umass.cs.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.DemandProfile;
import edu.umass.cs.reconfiguration.reconfigurationutils.ReconfigurableSampleNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.ReconfigurationRecord;
import edu.umass.cs.reconfiguration.reconfigurationutils.ReconfigurationRecord.RCStates;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.DiskMap;
import edu.umass.cs.utils.StringLocker;
import edu.umass.cs.utils.Util;
import edu.umass.cs.utils.MyLogger;

/**
 * @author V. Arun
 * @param <NodeIDType>
 */

public class SQLReconfiguratorDB<NodeIDType> extends
		AbstractReconfiguratorDB<NodeIDType> implements
		ReconfiguratorDB<NodeIDType> {
	/* ********************************************************************
	 * DB related parameters to be changed to use a different database service.
	 */
	private static final SQL.SQLType SQL_TYPE = SQL.SQLType.valueOf(Config
			.getGlobalString(RC.SQL_TYPE));
	private static final String DATABASE = Config
			.getGlobalString(RC.RECONFIGURATION_DB_PREFIX); // "reconfiguration_DB";
	/* ************ End of DB service related parameters ************** */

	private static final String RECONFIGURATION_RECORD_TABLE = "checkpoint";
	private static final String PENDING_TABLE = "messages";
	private static final String DEMAND_PROFILE_TABLE = "demand";
	private static final String NODE_CONFIG_TABLE = "nodeconfig";

	private static final String LOG_DIRECTORY = Config
			.getGlobalString(RC.RECONFIGURATION_DB_DIR);// "reconfiguration_DB";
	private static final boolean CONN_POOLING = true; // should just be true
	private static final int MAX_POOL_SIZE = 100;
	private static final int MAX_NAME_SIZE = SQLPaxosLogger.MAX_PAXOS_ID_SIZE;

	/**
	 * May need to increase this if the number of members in a replica group is
	 * very large.
	 */
	private static final int MAX_RC_RECORD_SIZE = Math.max(4096,
			SQLPaxosLogger.MAX_GROUP_STR_LENGTH * 16);
	private static final int MAX_DEMAND_PROFILE_SIZE = Config
			.getGlobalInt(RC.MAX_DEMAND_PROFILE_SIZE); // 4096;
	private static final boolean RC_RECORD_CLOB_OPTION = MAX_RC_RECORD_SIZE > SQL
			.getVarcharSize(SQL_TYPE);
	private static final boolean DEMAND_PROFILE_CLOB_OPTION = MAX_DEMAND_PROFILE_SIZE > SQL
			.getVarcharSize(SQL_TYPE);

	private static final boolean COMBINE_DEMAND_STATS = Config
			.getGlobalBoolean(RC.COMBINE_DEMAND_STATS);// false;
	private static final String CHECKPOINT_TRANSFER_DIR = "paxos_large_checkpoints";
	/**
	 * Should just be true as reconfigurator checkpoint size increases with the
	 * number of service names and can therefore be very large.
	 */
	private static final boolean LARGE_CHECKPOINTS_OPTION = true;
	private static final int MAX_FILENAME_LENGTH = 128;
	private static final String CHARSET = "ISO-8859-1";

	private static enum Columns {
		SERVICE_NAME, EPOCH, RC_GROUP_NAME, ACTIVES, NEW_ACTIVES, RC_STATE, STRINGIFIED_RECORD, DEMAND_PROFILE, INET_ADDRESS, PORT, NODE_CONFIG_VERSION, RC_NODE_ID
	};

	private static enum Keys {
		INET_SOCKET_ADDRESS, FILENAME, FILESIZE
	};
	
	private final DiskMap<String,ReconfigurationRecord<NodeIDType>> rcRecords;

	private static final ArrayList<SQLReconfiguratorDB<?>> instances = new ArrayList<SQLReconfiguratorDB<?>>();

	protected String logDirectory;
	
	private ComboPooledDataSource dataSource = null;

	private ServerSocket serverSock = null;

	private Object fileSystemLock = new Object();

	private StringLocker stringLocker = new StringLocker();

	private boolean closed = true;

	private static final Logger log = (Reconfigurator.getLogger());
	private static final int MAX_DB_BATCH_SIZE = Config.getGlobalInt(PC.MAX_DB_BATCH_SIZE);

	/**
	 * @param myID
	 * @param nc
	 * @param logDir
	 */
	public SQLReconfiguratorDB(NodeIDType myID,
			ConsistentReconfigurableNodeConfig<NodeIDType> nc, String logDir) {
		super(myID, nc);
		logDirectory = (logDir == null ? SQLReconfiguratorDB.LOG_DIRECTORY
				: logDir) + "/";
		addDerbyPersistentReconfiguratorDB(this);
		
		this.rcRecords = USE_DISK_MAP ? new DiskMap<String, ReconfigurationRecord<NodeIDType>>(Config.getGlobalInt(PC.PINSTANCES_CAPACITY)) {

			@Override
			public Set<String> commit(
					Map<String, ReconfigurationRecord<NodeIDType>> toCommit)
					throws IOException {
				return SQLReconfiguratorDB.this.putReconfigurationRecordDB(toCommit);
			}

			@Override
			public ReconfigurationRecord<NodeIDType> restore(String key)
					throws IOException {
				return SQLReconfiguratorDB.this.getReconfigurationRecordDB(key);
			}
		} : null;
		initialize();
	}
	
	private Set<String> putReconfigurationRecordDB(
			Map<String, ReconfigurationRecord<NodeIDType>> toCommit) {
		String updateCmd = "update " + getRCRecordTable() + " set "
				+ Columns.RC_GROUP_NAME.toString() + "=?, "
				+ Columns.STRINGIFIED_RECORD.toString() + "=? where "
				+ Columns.SERVICE_NAME.toString() + "=?";
		String cmd = updateCmd;
		
		PreparedStatement pstmt = null;
		Connection conn = null;
		Set<String> committed = new HashSet<String>();
		ReconfigurationRecord<?>[] values = toCommit.values().toArray(new ReconfigurationRecord[0]);
		try {
			ArrayList<String> batch = new ArrayList<String>();
			for(int i=0; i<values.length; i++) {
				String name = values[i].getName();
				if(conn==null) {
					conn = this.getDefaultConn();
					conn.setAutoCommit(false);
					pstmt = conn.prepareStatement(updateCmd);
				}
				// FIXME: should move to putReconfigurationRecordIndividually
				// removal
				if(toCommit.get(name)==null) {
					this.deleteReconfigurationRecord(name);
					log.log(Level.INFO, "{0} deleted RC record {1}",
							new Object[] { this, name });
					committed.add(name);
					continue;
				}
				// else update/insert
				String rcGroupName = toCommit.get(name).getRCGroupName();
				if(rcGroupName==null) rcGroupName = this.getRCGroupName(name);
				pstmt.setString(1, rcGroupName);
				if (RC_RECORD_CLOB_OPTION)
					pstmt.setClob(2, new StringReader((toCommit.get(name)).toString()));
				else
					pstmt.setString(2, (toCommit.get(name)).toString());
				pstmt.setString(3, name);
				pstmt.addBatch();
				batch.add(name);
				
				int[] executed = new int[batch.size()];
				if ((i+1) % MAX_DB_BATCH_SIZE == 0 || (i+1) == toCommit.size()) {
					executed = pstmt.executeBatch();
					assert(executed.length==batch.size());
					conn.commit();
					pstmt.clearBatch();
					for(int j=0; j<executed.length; j++) {
						if(executed[j]>0) {
							log.log(Level.INFO, "{0} updated RC record {1}", new Object[] {
									this, batch.get(j) });
							committed.add(batch.get(j));
						} else 
							log.log(Level.INFO,
									"{0} unable to update RC record {1} (executed={2}), will try insert",
									new Object[] { this, batch.get(j),
											executed[j] });
					}
					batch.clear();
				}
			}
		} catch (SQLException sqle) {
			log.severe("SQLException while inserting RC record using " + cmd);
			sqle.printStackTrace();
		} finally {
			cleanup(pstmt);
			cleanup(conn);
		}

		log.log(Level.INFO,
				"{0} batch-committed {1}({2}) out of {3}({4})",
				new Object[] { this, committed.size(), committed,
						toCommit.size(), toCommit.keySet() });
		committed.addAll(this.putReconfigurationRecordIndividually(this.diff(
				toCommit, committed)));
		log.log(Level.FINE,
				"{0} committed {1}({2}) out of {3}({4})",
				new Object[] { this, committed.size(), committed,
						toCommit.size(), toCommit.keySet() });
		return committed;
	}

	private Map<String, ReconfigurationRecord<NodeIDType>> diff(Map<String, ReconfigurationRecord<NodeIDType>> map, Set<String> set) {
		Map<String, ReconfigurationRecord<NodeIDType>> diffMap = new HashMap<String, ReconfigurationRecord<NodeIDType>>();
		for(String s : map.keySet())
			if(!set.contains(s) && map.get(s)!=null) diffMap.put(s, map.get(s));
		return diffMap;
	}

	private Set<String> putReconfigurationRecordIndividually(
			Map<String, ReconfigurationRecord<NodeIDType>> toCommit) {
		log.log(Level.INFO, "{0} individually committing names {1} names: {2}", new Object[] {
				this, toCommit.keySet().size(), toCommit.keySet() });
		Set<String> committed = new HashSet<String>();
		for (String name : toCommit.keySet()) {
			String rcGroupName = toCommit.get(name).getRCGroupName();
			if(rcGroupName==null) rcGroupName = this.getRCGroupName(name);
			
			if (toCommit.get(name) == null) {
				this.deleteReconfigurationRecord(name);
				log.log(Level.INFO, "{0} deleted RC record {1}", new Object[] {
						this, name });
			} else {
				this.putReconfigurationRecordDB(toCommit.get(name), rcGroupName);
				log.log(Level.FINE, "{0} individually committed {1}/{2} : {3}",
						new Object[] { this, name,
								toCommit.get(name).getRCGroupName(),
								toCommit.get(name).getSummary() });
			}
			committed.add(name);
		}
		//this.printRCTable();
		return committed;
	}

	/**
	 * @param myID
	 * @param nc
	 */
	public SQLReconfiguratorDB(NodeIDType myID,
			ConsistentReconfigurableNodeConfig<NodeIDType> nc) {
		this(myID, nc, null);
	}

	/******************** Start of overridden methods *********************/

	@Override
	public synchronized ReconfigurationRecord<NodeIDType> getReconfigurationRecord(
			String name) {
		if(USE_DISK_MAP) return this.rcRecords.get(name);
		else return this.getReconfigurationRecordDB(name);
	}
	
	private ReconfigurationRecord<NodeIDType> getReconfigurationRecordDB(
			String name) {
		long t0 = System.currentTimeMillis();
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
					+ "Exception while getting RC record for " + name + ":");
			e.printStackTrace();
		} finally {
			cleanup(pstmt, recordRS);
			cleanup(conn);
		}
		DelayProfiler.updateDelay("DB.getReconfigurationRecord", t0);
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
			// conn.commit();
		} catch (SQLException sqle) {
			log.severe("SQLException while updating stats using " + cmd);
			sqle.printStackTrace();
		} finally {
			cleanup(insertCP);
			cleanup(conn);
		}
		return true;
	}

	@Override
	public synchronized boolean setState(String name, int epoch,
			ReconfigurationRecord.RCStates state) {
		return this.setStateMerge(name, epoch, state, null);
	}

	/*
	 * One of two methods that changes state. Generally, this method will change
	 * the state to READY, while setStateInitReconfiguration sets the state from
	 * READY usually to WAIT_ACK_STOP.
	 */

	@Override
	public synchronized boolean setStateMerge(String name, int epoch,
			ReconfigurationRecord.RCStates state, Set<String> mergees) {
		ReconfigurationRecord<NodeIDType> record = this
				.getReconfigurationRecord(name);
		if (record == null)
			return false;
		if (record.isReady() && state.equals(RCStates.READY_READY)
				&& record.getUnclean() != 1)
			return false;

		log.log(Level.INFO,
				"==============================> {0} {1} {2}:{3} -> {4}:{5} {6} {7} {8}",
				new Object[] { this, record.getName(), record.getEpoch(),
						record.getState(), epoch, state,
						record.getNewActives(),
						mergees != null && !mergees.isEmpty() ? mergees : "",
						isUnclean(record) ? "(unclean)" : "" });

		record.setStateMerge(name, epoch, state, mergees);
		// setStateInitReconfiguration used for intent
		assert (state.equals(RCStates.READY)
				|| state.equals(RCStates.READY_READY) || state
					.equals(RCStates.WAIT_DELETE));
		if (record.isReady()) {
			record.setActivesToNewActives();
			/*
			 * The list of pending reconfigurations is stored persistently so
			 * that we can resume the most recent incomplete reconfiguration
			 * upon recovery. The ones before the most recent will be handled by
			 * paxos roll forward automatically. The reason paxos is not enough
			 * for the most recent one is because reconfiguration is (at least)
			 * a two step process consisting of an "intent" followed by a
			 * "complete" operation. Paxos will blindly replay all committed
			 * operations but has no way of knowing application-specific
			 * information like the fact that an intent has to be followed by
			 * complete. So if the node crashes after an intent but before the
			 * corresponding complete, the app has to redo just that last step.
			 */
			if (record.isReconfigurationReady())
				this.setPending(name, false);
			/*
			 * Trimming RC epochs is needed only for the NODE_CONFIG record. It
			 * removes entries for deleted RC nodes. The entries maintain the
			 * current epoch number for the group corresponding to each RC node
			 * in NODE_CONFIG. This information is needed in order for nodes to
			 * know from what current epoch number to reconfigure to the next
			 * epoch number when the corresponding RC groups may be out of date
			 * locally or may not even exist locally. A simpler alternative is
			 * to force all RC groups to reconfigure upon the addition or
			 * deletion of any RC nodes so that RC group epoch numbers are
			 * always identical to the NODE_CONFIG epoch number, but this is
			 * unsatisfying as it does not preserve the "consistent hashing"
			 * like property for reconfigurations, i.e., ideally only
			 * reconfigurators on the ring near an added or deleted
			 * reconfigurator should be affected. Note that even a "trivial"
			 * reconfiguration, i.e., when there is no actual change in an RC
			 * group, must go through the stop, start, drop sequence for
			 * correctness, and that process involves checkpointing and
			 * restoring locally from the checkpoint. Even though the
			 * checkpoints are local, it can take a long time for a large number
			 * of records, so it is better avoided when not needed.
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

	private boolean isUnclean(ReconfigurationRecord<?> record) {
		return record.getUnclean() > 0 && !this.isRCGroupName(record.getName());
	}

	/*
	 * state can be changed only if the current state is READY and if so, it can
	 * only be changed to WAIT_ACK_STOP. The epoch argument must also match the
	 * current epoch number.
	 */
	@Override
	public synchronized boolean setStateInitReconfiguration(String name,
			int epoch, RCStates state, Set<NodeIDType> newActives) {
		ReconfigurationRecord<NodeIDType> record = this
				.getReconfigurationRecord(name);
		assert (record != null && ((!TWO_PAXOS_RC && epoch - record.getEpoch() >= 0) || epoch
				- record.getEpoch() == 0)) : epoch + "!=" + record.getEpoch()
				+ " at " + myID;
		if (!record.isReady()) {
			log.log(Level.WARNING,
					"{0} {1}:{2} not ready for transition to {3}:{4}:{5}",
					new Object[] { this, record.getName(), record.getEpoch(),
							name, epoch, state });
			return false;
		}
		assert (state.equals(RCStates.WAIT_ACK_STOP));
		log.log(Level.INFO,
				"==============================> {0} {1} {2}:{3} -> {4}:{5} {6}",
				new Object[] { this, record.getName(), record.getEpoch(),
						record.getState(), epoch, state, newActives });
		record.setState(name, epoch, state, newActives);
		this.setPending(name, true);
		this.putReconfigurationRecord(record);

		record = this.getReconfigurationRecord(name);
		assert (!name.equals(AbstractReconfiguratorDB.RecordNames.NODE_CONFIG
				.toString()) || !record.getActiveReplicas().equals(
				record.getNewActives()));
		return true;
	}
	
	private static final boolean USE_DISK_MAP = Config.getGlobalBoolean(RC.USE_DISK_MAP_RCDB);
	
	private synchronized void putReconfigurationRecord(
			ReconfigurationRecord<NodeIDType> rcRecord) {
		if(USE_DISK_MAP) this.rcRecords.put(rcRecord.getName(), rcRecord);
		else this.putReconfigurationRecordDB(rcRecord, this.getRCGroupName(rcRecord.getName()));
	}
	
	private synchronized void putReconfigurationRecord(
			ReconfigurationRecord<NodeIDType> rcRecord, String rcGroupName) {
		if(USE_DISK_MAP) {
			this.rcRecords.put(rcRecord.getName(),
					rcRecord.setRCGroupName(rcGroupName));
		}
		else this.putReconfigurationRecordDB(rcRecord, rcGroupName);
	}

	private void putReconfigurationRecordDB(
			ReconfigurationRecord<NodeIDType> rcRecord, String rcGroupName) {
		ReconfigurationRecord<NodeIDType> prev = this
				.getReconfigurationRecordDB(rcRecord.getName());
		String insertCmd = "insert into " + getRCRecordTable() + " ("
				+ Columns.RC_GROUP_NAME.toString() + ", "
				+ Columns.STRINGIFIED_RECORD.toString() + ", "
				+ Columns.SERVICE_NAME.toString() + " ) values (?,?,?)";
		String updateCmd = "update " + getRCRecordTable() + " set "
				+ Columns.RC_GROUP_NAME.toString() + "=?, "
				+ Columns.STRINGIFIED_RECORD.toString() + "=? where "
				+ Columns.SERVICE_NAME.toString() + "=?";
		String cmd = prev != null ? updateCmd : insertCmd;

		rcRecord.setRCGroupName(rcGroupName);
		
		PreparedStatement insertCP = null;
		Connection conn = null;
		try {
			conn = this.getDefaultConn();
			insertCP = conn.prepareStatement(cmd);
			insertCP.setString(1, rcGroupName);
			if (RC_RECORD_CLOB_OPTION)
				insertCP.setClob(2, new StringReader(rcRecord.toString()));
			else
				insertCP.setString(2, rcRecord.toString());
			insertCP.setString(3, rcRecord.getName());
			insertCP.executeUpdate();
			// conn.commit();
		} catch (SQLException sqle) {
			log.severe("SQLException while inserting RC record using " + cmd);
			sqle.printStackTrace();
		} finally {
			cleanup(insertCP);
			cleanup(conn);
		}
		//printRCTable();
		log.log(Level.FINE, "{0} put {1} into RC group {2}", new Object[] {
				this, rcRecord.getSummary(), rcGroupName });
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
			log.info(this + " RC table " + this.getRCRecordTable() + " :\n" + s + "\n]");
		} catch (SQLException e) {
			log.severe("SQLException while print RC records table " + cmd);
			e.printStackTrace();
		} finally {
			cleanup(pstmt, rs);
			cleanup(conn);
		}
	}

	/*
	 * Should put RC records only for non-RC group names.
	 */
	private synchronized boolean putReconfigurationRecordIfNotName(
			ReconfigurationRecord<NodeIDType> record, String rcGroupName,
			String mergee) {

		// if RC group record, it must match rcGroupName
		if (this.isRCGroupName(record.getName())
				&& !record.getName().equals(rcGroupName))
			return false;
		// special case coz mergee may not be recognized by isRCGroupName
		else if (record.getName().equals(mergee))
			return false;

		// else good to insert and set pending if needed
		this.putReconfigurationRecord(record, rcGroupName);
		if (!record.isReady()) // FIXME: READY
			this.setPending(record.getName(), true, true);
		log.log(Level.INFO, "{0} inserted RC record named {1} to RC group {2}",
				new Object[] { this, record.getName(), rcGroupName });
		return true;
	}

	private synchronized boolean deleteReconfigurationRecord(String name) {
		ReconfigurationRecord<NodeIDType> record = this.getReconfigurationRecord(name);
		return record!=null ? this.deleteReconfigurationRecord(name, record.getEpoch()) : false;
	}
	
	@Override
	public synchronized boolean deleteReconfigurationRecord(String name,
			int epoch) {
		if (USE_DISK_MAP) {
			ReconfigurationRecord<NodeIDType> record = this
					.getReconfigurationRecord(name);
			if (record != null && record.getEpoch() == epoch)
				return this.rcRecords.remove(name) != null;
			else
				return false;
		} else
			return this.deleteReconfigurationRecordDB(name, epoch);
	}
	
	private synchronized boolean deleteReconfigurationRecordDB(String name,
			int epoch) {
		ReconfigurationRecord<NodeIDType> record = this
				.getReconfigurationRecord(name);
		if (record == null || record.getEpoch() != epoch)
			return false;

		log.log(Level.INFO,
				"==============================> {0} {1} -> DELETE",
				new Object[] { this, name });
		boolean deleted = this.deleteReconfigurationRecord(name,
				this.getRCRecordTable());
		this.deleteReconfigurationRecord(name, this.getPendingTable());
		this.deleteReconfigurationRecord(name, this.getDemandTable());
		return deleted;
	}

	@Override
	public synchronized boolean markDeleteReconfigurationRecord(String name,
			int epoch) {
		ReconfigurationRecord<NodeIDType> record = this
				.getReconfigurationRecord(name);
		if (record == null)
			return false;
		assert (record.getEpoch() == epoch);

		// READY possible under merge operations
		assert (record.getState().equals(RCStates.WAIT_ACK_STOP)) : record;
		log.log(Level.INFO, MyLogger.FORMAT[4], new Object[] {
				"==============================> ", this, name, " ->",
				"DELETE PENDING" });

		record.setState(name, epoch, RCStates.WAIT_DELETE);
		this.putReconfigurationRecord(record);

		// not necessary to delete demand right here
		this.deleteReconfigurationRecord(name, this.getDemandTable());
		return true;
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
		if (this.getReconfigurationRecord(record.getName()) != null)
			return null;
		log.log(Level.INFO,
				"==============================> {0} [] -> {1}:{2} {3} {4} ",
				new Object[] { this, record.getName(), record.getEpoch(),
						record.getState(), record.getNewActives() });
		this.putReconfigurationRecord(record);
		// put will be successful or throw an exception
		return record;
	}

	/******************** Incomplete paxos methods below **************/

	// write records to a file and return filename
	@Override
	public String checkpoint(String rcGroup) {
		synchronized (this.stringLocker.get(rcGroup)) {
			String cpFilename = getCheckpointFile(rcGroup);
			if (!this.createCheckpointFile(cpFilename))
				return null;

			if (USE_DISK_MAP) {
				log.log(Level.FINEST,
						"{0} committing {1} in-memory RC records while getting state for RC group {2} : {3}",
						new Object[] { this, this.rcRecords.size(), rcGroup,
								this.rcRecords });
				this.rcRecords.commit();
			}
			
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
					if(debug.length() < 64*1024) debug += "\n"+rcRecord;
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
			log.log(Level.FINE,
					"{0} returning state for {1}:{2}; wrote to file:\n{3}",
					new Object[] { this, rcGroup,
							this.getCheckpointDir() + rcGroup, debug });
			// return filename, not actual checkpoint
			return !exceptions && this.deleteOldCheckpoints(rcGroup, 2) ? this
					.getCheckpointURL(cpFilename) : null;
		}
	}

	@Override
	public boolean restore(String rcGroup, String state) {
		return this.updateState(rcGroup, state, null);
	}

	/*
	 * The second argument "state" here is implemented as the name of the file
	 * where the checkpoint state is actually stored. We assume that each RC
	 * record in the checkpoint state is separated by a newline.
	 * 
	 * FIXME: clean up the design of this method to make it more readable. We
	 * assume here that if state is not decodeable as a remote file handle or a
	 * local file by that name does not exist, state must be the actual state
	 * itself. This design is for having the convenience of treating the state
	 * either as a large checkpoint handle or the state itself as appropriate.
	 * But we need a more systematic way of distinguishing between handles and
	 * actual state.
	 */
	private boolean updateState(String rcGroup, String state, String mergee) {
		synchronized (this.stringLocker.get(rcGroup)) {

			this.wipeOutState(rcGroup);
			if (state == null) // all done already
				return true;

			// else first try treating state as remote file handle
			if ((state = this.getRemoteCheckpoint(rcGroup, state)) == null)
				throw new RuntimeException(this
						+ " unable to fetch checkpoint state");

			BufferedReader br = null;
			try {
				// read state from "state" transformed into a local filename
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
										this.consistentNodeConfig), rcGroup,
								mergee);
					}
				} else { // state is actually the state itself
					String[] lines = state.split("\n");
					for (String line : lines) {
						this.putReconfigurationRecordIfNotName(
								new ReconfigurationRecord<NodeIDType>(
										new JSONObject(line),
										this.consistentNodeConfig), rcGroup,
								mergee);
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

	/*
	 * FIXME: to be implemented. Currently, by design, it makes no difference
	 * whether or not we delete the RC group state before replacing it because
	 * of the nature of the RC group database. But paxos safety semantics
	 * require us to do this, so we should.
	 */
	private void wipeOutState(String rcGroup) {
	}

	@Override
	public String getDemandStats(String name) {
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
		return this.setPending(name, set, false);
	}

	private boolean setPending(String name, boolean set,
			boolean suppressExceptions) {
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
			// conn.commit();
		} catch (SQLException sqle) {
			if (!suppressExceptions) {
				log.severe("SQLException while modifying pending table with "
						+ name + ":" + set);
				sqle.printStackTrace();
			}
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
			// conn.commit();
			// this.setPending(name, false);
		} catch (SQLException sqle) {
			log.severe("SQLException while deleting RC record using " + cmd);
			sqle.printStackTrace();
		} finally {
			cleanup(insertCP);
			cleanup(conn);
		}
		return rowcount == 1;
	}

	private JSONObject getDemandStatsJSON(String name) {
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
		return COMBINE_DEMAND_STATS;
	}

	private Connection getDefaultConn() throws SQLException {
		return dataSource.getConnection();
	}

	private boolean initialize() {
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

	// /////// Start of file system checkpoint methods and classes /////////

	// opens the server thread for file system based checkpoints
	private boolean initCheckpointServer() {
		try {
			this.serverSock = new ServerSocket();
			this.serverSock.bind(new InetSocketAddress(
					this.consistentNodeConfig.getBindAddress(myID), 0));
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

	// spawns off a new thread to process file system based checkpoint request
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

	// sends a requested file system based checkpoint
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

	/*
	 * synchronized is meant to prevent concurrent file delete.
	 * 
	 * Reads request and transfers requested checkpoint.
	 */
	private void transferCheckpoint(Socket sock) {
		synchronized (this.fileSystemLock) {
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
	}

	/*
	 * Sends request for and receives remote checkpoint file if correctly
	 * formatted URL. If so, it returns a local filename. If not, it returns the
	 * url back as-is.
	 */
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
					filename = this
							.getRemoteCheckpoint(rcGroup, sockAddr, filename,
									jsonUrl.getLong(Keys.FILESIZE.toString()));
				}
			}
		} catch (JSONException e) {
			// do nothing, will return filename
		}
		return filename;
	}

	/**
	 * Helper function for getRemoteCheckpoint above that actually fetches the
	 * reads from the socket and writes to a local file.
	 * 
	 * @param rcGroupName
	 * @param sockAddr
	 * @param remoteFilename
	 * @param fileSize
	 * @return
	 */
	private String getRemoteCheckpoint(String rcGroupName,
			InetSocketAddress sockAddr, String remoteFilename, long fileSize) {
		synchronized (this.fileSystemLock) {
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
	}

	// makes a checkpoint URL out of a local filename
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

	// garbage collection methods below for file system based checkpoints
	private boolean deleteOldCheckpoints(final String rcGroupName, int keep) {
		return deleteOldCheckpoints(getCheckpointDir(), rcGroupName, keep, this.fileSystemLock);
	}
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

	private static class Filename implements Comparable<Filename> {
		final File file;

		Filename(File f) {
			this.file = f;
		}

		@Override
		public int compareTo(SQLReconfiguratorDB.Filename o) {
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

	// /////// End of file system based checkpoint transfer methods ///////

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
			 * Selects all RC group name records by asking for records where
			 * service name equals RC group name. The index on RC group names
			 * should make this query efficient but this performance needs to be
			 * tested at scale.
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
		if(groups!=null) groups.remove(AbstractReconfiguratorDB.RecordNames.NODE_CONFIG
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
		String cmdRCR = "create table "
				+ getRCRecordTable()
				+ " ("
				+ Columns.SERVICE_NAME.toString()
				+ " varchar("
				+ MAX_NAME_SIZE
				+ ") not null, "
				+ Columns.RC_GROUP_NAME.toString()
				+ " varchar("
				+ MAX_NAME_SIZE
				+ ") not null,  "
				+ Columns.STRINGIFIED_RECORD.toString()
				+ (RC_RECORD_CLOB_OPTION ? SQL.getBlobString(
						MAX_RC_RECORD_SIZE, SQL_TYPE) : " varchar("
						+ MAX_RC_RECORD_SIZE + ")") + ", primary key ("
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
		String cmdDP = "create table "
				+ getDemandTable()
				+ " ("
				+ Columns.SERVICE_NAME.toString()
				+ " varchar("
				+ MAX_NAME_SIZE
				+ ") not null, "
				+ Columns.DEMAND_PROFILE.toString()
				+ (DEMAND_PROFILE_CLOB_OPTION ? SQL.getBlobString(
						MAX_DEMAND_PROFILE_SIZE, SQL_TYPE) : " varchar("
						+ MAX_DEMAND_PROFILE_SIZE + ")") + ", primary key("
				+ Columns.SERVICE_NAME.toString() + "))";
		// node config information, needs to be correct under faults
		String cmdNC = "create table " + getNodeConfigTable() + " ("
				+ Columns.RC_NODE_ID.toString() + " varchar(" + MAX_NAME_SIZE
				+ ") not null, " + Columns.INET_ADDRESS.toString()
				+ " varchar(256), " + Columns.PORT.toString() + " int, "
				+ Columns.NODE_CONFIG_VERSION.toString() + " int, primary key("
				+ Columns.RC_NODE_ID.toString() + ", "
				+ Columns.NODE_CONFIG_VERSION + "))";

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
			if (SQL.DUPLICATE_TABLE.contains(sqle.getSQLState())) {
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

	private static boolean isEmbeddedDB() {
		return SQL_TYPE.equals(SQL.SQLType.EMBEDDED_DERBY);
	}

	private boolean connectDB() {
		boolean connected = false;
		int connAttempts = 0, maxAttempts = 1;
		long interAttemptDelay = 2000; // ms
		Properties props = new Properties(); // connection properties
		// providing a user name and PASSWORD is optional in embedded derby
		props.put("user", SQL.getUser() + (isEmbeddedDB() ? this.myID : ""));
		props.put("password", SQL.getPassword());
		String dbCreation = SQL.getProtocolOrURL(SQL_TYPE)
				+ (isEmbeddedDB() ? this.logDirectory
						+ DATABASE
						+ this.myID
						+ (!SQLPaxosLogger.existsDB(SQL_TYPE,
								this.logDirectory, DATABASE + this.myID) ? ";create=true"
								: "")
						: DATABASE + this.myID
								+ "?createDatabaseIfNotExist=true");

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
			Connection conn = null;
			try {
				connAttempts++;
				log.info("Attempting getDefaultConn() to DB " + dbCreation);
				getDefaultConn(); // open first connection
				log.info("Connected to and created database " + DATABASE
						+ this.myID);
				connected = true;
				if (isEmbeddedDB())
					fixURI(); // remove create flag
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

	private void fixURI() {
		this.dataSource.setJdbcUrl(SQL.getProtocolOrURL(SQL_TYPE)
				+ this.logDirectory + DATABASE + this.myID);
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

	private static void addDerbyPersistentReconfiguratorDB(
			SQLReconfiguratorDB<?> rcDB) {
		synchronized (SQLReconfiguratorDB.instances) {
			if (!SQLReconfiguratorDB.instances.contains(rcDB))
				SQLReconfiguratorDB.instances.add(rcDB);
		}
	}

	private static boolean allClosed() {
		synchronized (SQLReconfiguratorDB.instances) {
			for (SQLReconfiguratorDB<?> rcDB : instances) {
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
			SQLReconfiguratorDB<Integer> rcDB) {
		int groupSize = (int) Math.random() * 10;
		Set<Integer> newActives = new HashSet<Integer>();
		for (int i = 0; i < groupSize; i++)
			newActives.add((int) Math.random() * 100);
		ReconfigurationRecord<Integer> rcRecord = new ReconfigurationRecord<Integer>(
				name, 0, newActives);
		rcDB.putReconfigurationRecord(rcRecord.setRCGroupName("RC0"));
		ReconfigurationRecord<Integer> retrievedRecord = rcDB
				.getReconfigurationRecord(name);
		assert (retrievedRecord.toString().equals(rcRecord.toString())) : rcRecord + " != "
				+ retrievedRecord;
	}

	private static Request getRandomInterfaceRequest(final String name) {
		return new Request() {

			@Override
			public IntegerPacketType getRequestType() {
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
			SQLReconfiguratorDB<Integer> rcDB) {
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

	private boolean createCheckpointFile(String filename) {
		synchronized (this.fileSystemLock) {
			File file = new File(filename);
			try {
				file.createNewFile(); // will create only if not exists
			} catch (IOException e) {
				log.severe("Unable to create checkpoint file for " + filename);
				e.printStackTrace();
			}
			return (file.exists());
		}
	}

	public String toString() {
		return "DerbyRCDB" + myID;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Util.assertAssertionsEnabled();
		ReconfigurableSampleNodeConfig nc = new ReconfigurableSampleNodeConfig();
		nc.localSetup(3);
		ConsistentReconfigurableNodeConfig<Integer> consistentNodeConfig = new ConsistentReconfigurableNodeConfig<Integer>(
				nc);
		SQLReconfiguratorDB<Integer> rcDB = new SQLReconfiguratorDB<Integer>(
				consistentNodeConfig.getReconfigurators().iterator().next(),
				consistentNodeConfig);
		String name = "name0";

		int numTests = 100;
		for (int i = 0; i < numTests; i++) {
			testRCRecordReadAfterWrite(name, rcDB);
		}
		for (int i = 0; i < numTests; i++) {
			testDemandProfileUpdate(name, rcDB)
			;
		}

		if(USE_DISK_MAP) rcDB.rcRecords.commit();
		rcDB.printRCTable();
		//System.out.println("RC group names = " + rcDB.getRCGroupNames());
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
			insertCP.setString(2, sockAddr.toString());
			insertCP.setInt(3, sockAddr.getPort());
			insertCP.setInt(4, version);
			insertCP.executeUpdate();
			// conn.commit();
			added = true;
		} catch (SQLException sqle) {
			if (!SQL.DUPLICATE_KEY.contains(sqle.getSQLState())) {
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
			// conn.commit();
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
				+ Columns.NODE_CONFIG_VERSION.toString()
				+ (maxOnly ? "=?" : ">=?");

		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Connection conn = null;
		try {
			conn = this.getDefaultConn();
			pstmt = conn.prepareStatement(cmd);
			pstmt.setInt(1, (maxOnly ? version : (version - 1)));
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

	/*
	 * This method imports consistentNodeConfig from the persistent copy. The
	 * persistent copy has both the latest (unstable) version and the previous
	 * version. At the end of this method, consistentNodeConfig has the latest
	 * version nodes as well as the previous version nodes with nodes present in
	 * the latter but not in the former being slated for removal.
	 */
	private void initAdjustSoftNodeConfig() {
		// get node config containing all reconfigurators from DB
		Map<NodeIDType, InetSocketAddress> rcMapAll = this
				.getRCNodeConfig(false);
		// if no hard copy, copy soft to hard and return
		if (rcMapAll.isEmpty() && copySoftNodeConfigToDB())
			return;

		// else get NC record with the current and next set of reconfigurators
		ReconfigurationRecord<NodeIDType> ncRecord = this
				.getReconfigurationRecord(AbstractReconfiguratorDB.RecordNames.NODE_CONFIG
						.toString());
		if (ncRecord == null)
			return; // else we have something to do

		// get only the latest version reconfigurators
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

	// copy soft to hard if no hard copy
	private boolean copySoftNodeConfigToDB() {
		boolean added = true;
		for (NodeIDType reconfigurator : this.consistentNodeConfig
				.getReconfigurators())
			added = added
					&& this.addReconfigurator(reconfigurator,
							this.consistentNodeConfig
									.getNodeSocketAddress(reconfigurator), 0);
		assert (added);
		return added;
	}

	/*
	 * The double synchronized is because we need to synchronize over "this" to
	 * perform testAndSet checks over record, but we have to do that after,
	 * never before, stringLocker lock. The stringLocker lock is so that we
	 * don't have to lock all records in order to just synchronize a single
	 * group's getState or updateState.
	 */
	@Override
	public boolean mergeState(String rcGroupName, int epoch, String mergee,
			int mergeeEpoch, String state) {
		synchronized (this.stringLocker.get(rcGroupName)) {
			synchronized (this) {
				ReconfigurationRecord<NodeIDType> record = this
						.getReconfigurationRecord(rcGroupName);

				assert (record.getEpoch() == epoch);
				if (!record.hasBeenMerged(mergee))
					if (this.updateState(rcGroupName, state, mergee)) {
						record.insertMerged(mergee);
						this.putReconfigurationRecord(record);
						// delete mergee RC record as it must have been stopped
						this.setMergeeStateToWaitDelete(mergee, mergeeEpoch);
						log.log(Level.INFO,
								"{0} merged state from {1}:{2} into {3}:{4}",
								new Object[] { this, mergee, mergeeEpoch,
										rcGroupName, epoch, });
					} else
						log.warning(this + " attempt to merge " + mergee + ":"
								+ mergeeEpoch + " failed.");
				if (record.isReconfigurationReady())
					this.setPending(rcGroupName, false, true);
				// paxos will still always see a true return value
				return record.hasBeenMerged(mergee);
			}
		}
	}

	// wait before delete for safe re-creations
	private boolean setMergeeStateToWaitDelete(String mergee, int mergeEpoch) {
		ReconfigurationRecord<NodeIDType> record = this
				.getReconfigurationRecord(mergee, mergeEpoch);
		if (record == null)
			return false;
		/*
		 * Fast forward to WAIT_DELETE as the paxos group must have already been
		 * stopped. The only reason to not simply delete this record is to let
		 * it pend in WAIT_DELETE state in order to prevent it from getting
		 * recreated before MAX_FINAL_STATE_AGE.
		 */
		this.setStateInitReconfiguration(mergee, mergeEpoch,
				RCStates.WAIT_ACK_STOP, new HashSet<NodeIDType>());
		this.setState(mergee, mergeEpoch + 1, RCStates.WAIT_DELETE);
		return true;
	}

	@Override
	public synchronized void clearMerged(String rcGroupName, int epoch) {
		ReconfigurationRecord<NodeIDType> record = this
				.getReconfigurationRecord(rcGroupName, epoch);
		if (record == null)
			return;
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

	@Override
	public boolean mergeIntent(String name, int epoch, String mergee) {
		ReconfigurationRecord<NodeIDType> record = this
				.getReconfigurationRecord(name);
		boolean added = record.addToMerge(mergee);
		this.putReconfigurationRecord(record);
		return added;
	}

	private boolean recordLongPendingDelete(
			ReconfigurationRecord<NodeIDType> record) {
		return ((System.currentTimeMillis() - record.getDeleteTime()) > ReconfigurationConfig
				.getMaxFinalStateAge());
	}

	@Override
	public void delayedDeleteComplete() {
		String deletee = null;
		try {
			String[] pending = getPendingReconfigurations();
			for (String name : pending) {
				deletee = name;
				ReconfigurationRecord<NodeIDType> record = this
						.getReconfigurationRecord(name);
				if (record != null
						&& record.getState().equals(RCStates.WAIT_DELETE)
						&& recordLongPendingDelete(record)) {
					this.deleteReconfigurationRecord(name, record.getEpoch());
					log.log(Level.INFO, "{0} deleted record {1}", new Object[] {
							this, record.getSummary() });
				}
			}
		} catch (Exception e) {
			// need to catch everything just to stay alive
			log.severe(this + "encountered exception while delayedDeleting : "
					+ deletee + " : " + e.getMessage());
			e.printStackTrace();
		}
	}

	@Override
	public void garbageCollectedDeletedNode(NodeIDType node) {
		try {
			this.deleteOldCheckpoints(this.getRCGroupName(node), 0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public synchronized boolean createReconfigurationRecords(
			Map<String, String> nameStates, Set<NodeIDType> newActives) {
		if (USE_DISK_MAP) {
			boolean insertedAll = true;
			Set<String> inserted = new HashSet<String>();
			for (String name : nameStates.keySet())
				/*
				 * We just directly initialize with WAIT_ACK_STOP:-1 instead of
				 * starting with READY:-1 and pretending to go through the whole
				 * reconfiguration protocol sequence.
				 */
				if (insertedAll = insertedAll
						&& (this.rcRecords.put(name,
								new ReconfigurationRecord<NodeIDType>(name, -1,
										newActives).setState(name, -1,
										RCStates.WAIT_ACK_STOP)) == null))
					inserted.add(name);
			
			if (!insertedAll)
				// rollback
				for (String name : nameStates.keySet())
					this.deleteReconfigurationRecord(name, 0);
			return insertedAll;
		} else
			return this.createReconfigurationRecordsDB(nameStates, newActives);
	}

	private boolean createReconfigurationRecordsDB(Map<String, String> nameStates,
			Set<NodeIDType> newActives) {
		String insertCmd = "insert into " + getRCRecordTable() + " ("
				+ Columns.RC_GROUP_NAME.toString() + ", "
				+ Columns.STRINGIFIED_RECORD.toString() + ", "
				+ Columns.SERVICE_NAME.toString() + " ) values (?,?,?)";

		PreparedStatement insertRC = null;
		Connection conn = null;
		boolean insertedAll = true;
		Set<String> batch = new HashSet<String>();
		Set<String> committed = new HashSet<String>();
		try {
			if(conn==null) {
				conn = this.getDefaultConn();
				conn.setAutoCommit(false);
				insertRC = conn.prepareStatement(insertCmd);
			}
			assert (nameStates != null && !nameStates.isEmpty());
			String rcGroupName = this.getRCGroupName(nameStates.keySet()
					.iterator().next());
			int i = 0;
			long t1 = System.currentTimeMillis();
			for (String name : nameStates.keySet()) {
				ReconfigurationRecord<NodeIDType> record = new ReconfigurationRecord<NodeIDType>(
						name, -1, newActives);
				/*
				 * We just directly initialize with WAIT_ACK_STOP:-1 instead of
				 * starting with READY:-1 and pretending to go through the whole
				 * reconfiguration protocol sequence.
				 */
				record.setState(name, -1, RCStates.WAIT_ACK_STOP);
				insertRC.setString(1, rcGroupName);
				if (RC_RECORD_CLOB_OPTION)
					insertRC.setClob(2, new StringReader(record.toString()));
				else
					insertRC.setString(2, record.toString());
				insertRC.setString(3, name);
				insertRC.addBatch();
				batch.add(name);
				i++;
				if ((i + 1) % MAX_DB_BATCH_SIZE == 0 || (i + 1) == nameStates.size()) {
					int[] executed = insertRC.executeBatch();
					conn.commit();
					insertRC.clearBatch();
					committed.addAll(batch);
					batch.clear();
					for (int j : executed)
						insertedAll = insertedAll && (j > 0);
					if (insertedAll)
						log.log(Level.FINE,
								"{0} successfully logged the last {1} messages in {2} ms",
								new Object[] { this, (i + 1),
										(System.currentTimeMillis() - t1) });
					t1 = System.currentTimeMillis();
				}
			}
		} catch (SQLException sqle) {
			log.severe("SQLException while inserting batched RC records using "
					+ insertCmd);
			sqle.printStackTrace();
		} finally {
			cleanup(insertRC);
			cleanup(conn);
		}
		
		// rollback
		if (!insertedAll) {
			for (String name : nameStates.keySet())
				if (committed.contains(name))
				this.deleteReconfigurationRecord(name, 0);
		}
		
		return insertedAll;
	}

	@Override
	public boolean setStateInitReconfiguration(Map<String, String> nameStates,
			int epoch, RCStates state, Set<NodeIDType> newActives) {
		// do nothing coz we already initialized batch with WAIT_ACK_STOP:-1
		return true;
	}

	@Override
	public boolean setStateMerge(Map<String, String> nameStates, int epoch,
			RCStates state, Set<NodeIDType> newActives) {
		if(USE_DISK_MAP) {
			for(String name : nameStates.keySet()) {
				ReconfigurationRecord<NodeIDType> record = this.rcRecords.get(name);
				assert(record!=null);
				record.setState(name, epoch, state);
			}
			return true;
		}
		else return this.setStateMergeDB(nameStates, epoch, state, newActives);
	}
	
	private boolean setStateMergeDB(Map<String, String> nameStates, int epoch,
			RCStates state, Set<NodeIDType> newActives) {
		String updateCmd = "update " + getRCRecordTable() + " set "
				+ Columns.RC_GROUP_NAME.toString() + "=?, "
				+ Columns.STRINGIFIED_RECORD.toString() + "=? where "
				+ Columns.SERVICE_NAME.toString() + "=?";

		PreparedStatement updateRC = null;
		Connection conn = null;
		boolean updatedAll = true;
		try {
			if(conn==null) {
				conn = this.getDefaultConn();
				conn.setAutoCommit(false);
				updateRC = conn.prepareStatement(updateCmd);
			}
			assert (nameStates != null && !nameStates.isEmpty());
			String rcGroupName = this.getRCGroupName(nameStates.keySet()
					.iterator().next());
			int i = 0;
			long t1 = System.currentTimeMillis();
			for (String name : nameStates.keySet()) {
				ReconfigurationRecord<NodeIDType> record = new ReconfigurationRecord<NodeIDType>(
						name, 0, newActives);
				record.setState(name, 0, RCStates.READY_READY);
				updateRC.setString(1, rcGroupName);
				if (RC_RECORD_CLOB_OPTION)
					updateRC.setClob(2, new StringReader(record.toString()));
				else
					updateRC.setString(2, record.toString());
				updateRC.setString(3, name);
				updateRC.addBatch();
				i++;
				if ((i + 1) % MAX_DB_BATCH_SIZE == 0 || (i + 1) == nameStates.size()) {
					int[] executed = updateRC.executeBatch();
					conn.commit();
					updateRC.clearBatch();
					for (int j : executed)
						updatedAll = updatedAll && (j > 0);
					if (updatedAll)
						log.log(Level.FINE,
								"{0} successfully logged the last {1} messages in {2} ms",
								new Object[] { this, (i + 1),
										(System.currentTimeMillis() - t1) });
					t1 = System.currentTimeMillis();
				}
			}
		} catch (SQLException sqle) {
			log.severe("SQLException while inserting batched RC records using "
					+ updateCmd);
			sqle.printStackTrace();
		} finally {
			cleanup(updateRC);
			cleanup(conn);
		}
		return updatedAll;
	}
}
