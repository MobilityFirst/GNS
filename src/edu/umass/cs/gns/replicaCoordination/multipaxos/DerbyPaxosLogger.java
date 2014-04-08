package edu.umass.cs.gns.replicaCoordination.multipaxos;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nsdesign.packet.PaxosPacket;
import edu.umass.cs.gns.nsdesign.packet.PaxosPacket.PaxosPacketType;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.AcceptPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.PValuePacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.PreparePacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.ProposalPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.RequestPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.StatePacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.Ballot;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.RecoveryInfo;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.SlotBallotState;
import edu.umass.cs.gns.util.Util;

/**
@author V. Arun
 */

/* This logger uses an embedded database for persistent storage.
 * It is easily scalable to a very large number of paxos 
 * instances as the scale is only limited by the available
 * disk space and the disk space needed increases as the number
 * of paxos instances, the size of application state, and the
 * inter-checkpointing interval. 
 * 
 * Concurrency: There is very little concurrency support here.
 * All methods that touch the database are synchronized. We
 * need to have connection pooling for better performance. 
 * The methods are synchronized coz we are mostly reusing a 
 * single connection, so we can not have prepared statements 
 * overlap while they are executing.
 * 
 * Testing: Can be unit-tested using main.
 */
public class DerbyPaxosLogger extends AbstractPaxosLogger {
	public static final boolean DEBUG=PaxosManager.DEBUG;

	private static final String FRAMEWORK = "embedded";
	private static final String DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
	private static final String PROTOCOL = "jdbc:derby:";
	private static final String DUPLICATE_KEY = "23505";
	private static final String DUPLICATE_TABLE = "X0Y32";
	private static final String USER = "user"; 
	private static final String PASSWORD = "user";
	
	private static final String DATABASE = "paxos_logs";
	private static final String CHECKPOINT_TABLE = "checkpoint";
	private static final String MESSAGES_TABLE = "messages";
	
	/* If true, the prepare statement for log messages will be reused
	 * instead of being created and torn down inside the log(.) method
	 * upon each invocation. Re-using prepare statements seems about
	 * 10-20% faster than not re-using for paxos workload tests.
	 */
	private static final boolean GLOBAL_LOG_STMT = true; 
	private static final int PAXOS_ID_SIZE = 20; // GUID is 20 bytes
	private static final int MAX_STATE_SIZE = 8192; // FIXME: Taken from NIOTransport.readBuffer
	private static final int MAX_GROUP_SIZE = 256; // maximum size of a paxos replica group
	private static final int MAX_LOG_MESSAGE_SIZE = 256; // maximum size of a log message
	private static final boolean AUTO_COMMIT = true; // False for testing, true for production
	private static final int MAX_LOGGED_DECISIONS = 100;

	private Connection defaultConn = null;
	private Connection incrReadConn = null; // FIXME: We should really use a connection pool

	private boolean closed = true;
	
	/* The global statements are not really need and can be replaced
	 * by local variables in log(.) and duplicateOrOutdated(.)
	 * but are supposedly more efficient. But they don't seem
	 * to speed it up much. But at some point, they did, so
	 * these are still being used.
	 */
	private PreparedStatement logMsgStmt=null;
	private PreparedStatement checkpointStmt=null;
	private PreparedStatement pstmtGlobal = null;
	private ResultSet rsetGlobal=null;
	private static Logger log = Logger.getLogger(DerbyPaxosLogger.class.getName()); // GNS.getLogger();

	DerbyPaxosLogger(int id, String dbPath) {
		super(id, dbPath);
		initialize(); // will set up db, connection, tables, etc. as needed
	}

	/**
	 * Puts given checkpoint state for paxosID. 'state' could be anything
	 * that allows PaxosInterface to later restore the corresponding 
	 * state. For example, 'state' could be the name of a file where the
	 * app maintains a checkpoint of all of its state. It could of course
	 * be the stringified form of the actual state if the state is at most
	 * MAX_STATE_SIZE. 
	 */
	public synchronized void putCheckpointState(String paxosID, short version, int[] group, int slot, Ballot ballot, String state, int acceptedGCSlot) {
		if(isClosed()) return;
		// Stupid derby doesn't have an insert if not exist command
		String insertCmd = "insert into " + getCTable() + " (version,members,slot,ballotnum,coordinator,state,paxos_id) values (?,?,?,?,?,?,?)";
		String updateCmd = "update " + getCTable() + " set version=?,members=?, slot=?, ballotnum=?, coordinator=?, state=? where paxos_id=?";
		String cmd = this.getCheckpointState(paxosID,"paxos_id")!=null ? updateCmd : insertCmd; 
		PreparedStatement insertCP=null;
		try {
			insertCP = this.defaultConn.prepareStatement(cmd);
			insertCP.setShort(1, version);
			insertCP.setString(2, Util.arrayToSet(group).toString());
			insertCP.setInt(3, slot);
			insertCP.setInt(4, ballot.ballotNumber);
			insertCP.setInt(5, ballot.coordinatorID);
			insertCP.setString(6, state);
			insertCP.setString(7, paxosID);
			insertCP.executeUpdate(); 
			// why can't insertCP.toString() return the query string? :/
			log.info("Node " + this.myID + " DB inserted checkpoint ("+paxosID+","+Util.arrayToSet(group).toString()+","+
					slot+","+ballot+","+state+","+acceptedGCSlot+")");
			// Delete logged messages from before the checkpoint
			this.deleteOutdatedMessages(paxosID, slot, ballot.ballotNumber, ballot.coordinatorID, acceptedGCSlot);
		} catch(SQLException sqle) {
			log.severe("SQLException while checkpointing as " + cmd); sqle.printStackTrace();
		} finally {cleanup(insertCP);}
	}
	/**
	 * Deletes logged messages from before the most recent checkpoint.
	 * Called upon each putCheckpointState call above.
	 * @param paxosID 
	 */
	private synchronized void deleteOutdatedMessages(String paxosID, int slot, int ballotnum, int coordinator, int acceptedGCSlot) {
		if(isClosed()) return;

		PreparedStatement pstmt=null,dstmt=null; ResultSet checkpointRS=null;
		try {
			// Create delete command using the slot, ballot, and gcSlot
			String dcmd = "delete from " + getMTable() + " where paxos_id='"+paxosID+"' and " +
					"(packet_type="+PaxosPacketType.DECISION.getNumber() + " and slot<"+(slot-MAX_LOGGED_DECISIONS)+") or " +
					"(packet_type="+PaxosPacketType.PREPARE.getNumber() + " and (ballotnum<"+ballotnum + 
					" or (ballotnum="+ballotnum + " and coordinator<"+coordinator+"))) or" +
							"(packet_type="+PaxosPacketType.ACCEPT.getNumber() + " and slot<="+Math.min(acceptedGCSlot, slot)+")"; 
			dstmt = this.defaultConn.prepareStatement(dcmd);
			dstmt.execute();
		} catch(SQLException sqle) {
			log.severe("SQLException while deleting outdated messages for " + paxosID); sqle.printStackTrace();
		} finally {cleanup(pstmt,checkpointRS);cleanup(dstmt);}
	}

	/**
	 * Gets current checkpoint. There can be only one checkpoint for a
	 * paxosID at any time. 
	 */
	public String getCheckpointState(String paxosID) {return this.getCheckpointState(paxosID, "state");}
	private synchronized String getCheckpointState(String paxosID, String column) {
		if(isClosed()) return null;

		String state = null;
		PreparedStatement pstmt=null; ResultSet stateRS=null;
		try {
			pstmt = getPreparedStatement(null, getCTable(), paxosID, column);
			stateRS = pstmt.executeQuery();
			while(stateRS.next()) {
				assert(state==null); // single result
				state = stateRS.getString(1);
			}

		} catch(SQLException sqle) {
			log.severe("SQLException while getting state " + " : " + sqle);
		} finally {cleanup(pstmt,stateRS);}

		return state;
	}
	/* Methods to get slot, ballotnum, and coordinator of checkpoint */
	public synchronized SlotBallotState getSlotBallotState(String paxosID) {
		if(isClosed()) return null;

		SlotBallotState sb=null;
		ResultSet stateRS=null;
		try {
			//pstmt = this.getPreparedStatement(getCTable(), paxosID, "slot, ballotnum, coordinator");
			if(this.checkpointStmt==null) this.checkpointStmt = 
					this.getPreparedStatement(null, getCTable(), paxosID, "slot, ballotnum, coordinator, state");
			this.checkpointStmt.setString(1, paxosID);
			//stateRS = pstmt.executeQuery();
			stateRS = this.checkpointStmt.executeQuery();
			while(stateRS.next()) {
				assert(sb==null); // single result
				sb = new SlotBallotState(
				stateRS.getInt(1),
				stateRS.getInt(2),
				stateRS.getInt(3), 
				stateRS.getString(4));
			}
		} catch(SQLException sqle) {
			log.severe("SQLException while getting slot " + " : " + sqle);
		} finally {cleanup(stateRS);}
		return sb;
	}
	public int getCheckpointSlot(String paxosID) {
		SlotBallotState sb = getSlotBallotState(paxosID);
		return (sb!=null ? sb.slot : -1);
	}
	public Ballot getCheckpointBallot(String paxosID) {
		SlotBallotState sb = getSlotBallotState(paxosID);
		return (sb!=null ? new Ballot(sb.ballotnum, sb.coordinator) : null);
	}
	public StatePacket getStatePacket(String paxosID) {
		SlotBallotState sbs = this.getSlotBallotState(paxosID);
		StatePacket statePacket = null;
		if(sbs!=null) statePacket = new StatePacket(new Ballot(sbs.ballotnum, sbs.coordinator), sbs.slot, sbs.state);
		return statePacket;
	}
	/** 
	 * Gets a map of all paxosIDs and their corresponding group members.
	 */
	public synchronized ArrayList<RecoveryInfo> getAllPaxosInstances() {
		if(isClosed()) return null;

		ArrayList<RecoveryInfo> allPaxosInstances = new ArrayList<RecoveryInfo>();
		PreparedStatement pstmt=null; ResultSet stateRS=null;
		try {
			pstmt = this.getPreparedStatement(null, getCTable(), null, "paxos_id, version, members");
			stateRS = pstmt.executeQuery();
			while(stateRS!=null && stateRS.next()) {
				String paxosID = stateRS.getString(1);
				short version = stateRS.getShort(2);
				String members = stateRS.getString(3);
				String[] pieces = members.replaceAll("\\[|\\]", "").split(",");
				int[] group = new int[pieces.length];
				for(int i=0; i<group.length; i++) group[i] = Integer.parseInt(pieces[i].trim());
				allPaxosInstances.add(new RecoveryInfo(paxosID, version, group));
			}
		} catch(SQLException sqle) {
			log.severe("SQLException while getting all paxos IDs " + " : " + sqle);
		} finally {cleanup(pstmt,stateRS);}
		return allPaxosInstances;
	}
	
	/************* Start of incremental checkpoint read methods **********************/
	protected synchronized boolean initiateReadCheckpoints() {
		if(isClosed() || this.pstmtGlobal!=null || this.rsetGlobal!=null) return false;

		boolean initiated = false;
		try {
			this.pstmtGlobal = this.getPreparedStatement(this.incrReadConn, getCTable(), null, "paxos_id, version, members, state");
			this.rsetGlobal = this.pstmtGlobal.executeQuery();
			initiated = true;
		} catch(SQLException sqle) {
			log.severe("SQLException while getting all paxos IDs " + " : " + sqle);
		}
		return initiated;
	}
	protected synchronized RecoveryInfo readNextCheckpoint() {
		RecoveryInfo pri = null;
		try {
			if(rsetGlobal!=null && rsetGlobal.next()) {
				String paxosID = rsetGlobal.getString(1);
				short version = rsetGlobal.getShort(2);
				String members = rsetGlobal.getString(3);
				String[] pieces = members.replaceAll("\\[|\\]", "").split(",");
				int[] group = new int[pieces.length];
				for(int i=0; i<group.length; i++) group[i] = Integer.parseInt(pieces[i].trim());
				String state = rsetGlobal.getString(4);
				pri = new RecoveryInfo(paxosID, version, group, state);
			}
		} catch(SQLException sqle) {
			log.severe("SQLException in readNextCheckpoint: " + " : " + sqle);
		}
		return pri;
	}
	/* FIXME: Don't use this method coz it will run into concurrency problems.
	 */
	protected synchronized boolean initiateReadLogMessages(String paxosID) {
		if(isClosed() || this.pstmtGlobal!=null || this.rsetGlobal!=null) return false;

		boolean initiatd = false;
		try {
			this.pstmtGlobal = this.getPreparedStatement(null, getMTable(), paxosID, "message", null);
			this.rsetGlobal = this.pstmtGlobal.executeQuery();
			initiatd = true;
		} catch(SQLException sqle) {
			log.severe("SQLException in initiateReadLoggedMessages for paxosID " + paxosID +":" + sqle);
		}
		return initiatd;
	}
	/* FIXME: Don't use this method coz it will run into concurrency problems.
	 */
	protected synchronized PaxosPacket readNextLogMessage() {
		PaxosPacket packet=null;
		try {
			if(this.rsetGlobal.next()) {
				String msg = this.rsetGlobal.getString(1); 
				packet = getPaxosPacket(msg);
			}
		} catch(SQLException sqle) {
			log.severe("SQLException in readNextCheckpoint: " + " : " + sqle);
		}
		return packet;
	}
	protected synchronized void closeReadAll() {
		cleanup(this.pstmtGlobal,this.rsetGlobal);
	}
	/************* End of incremental checkpoint read methods **********************/


	/** 
	 * Convenience method invoked by a number of other methods. Should
	 * be called only from a self-synchronized method.
	 * @param table
	 * @param paxosID
	 * @param column
	 * @return PreparedStatement to lookup the specified table, paxosID and column(s)
	 * @throws SQLException
	 */
	private PreparedStatement getPreparedStatement(Connection conn, String table, String paxosID, String column, String fieldConstraints) throws SQLException {
		String cmd = "select " + column + " from " + table + (paxosID!=null ? " where paxos_id=?" : "");
		cmd += (fieldConstraints!=null ? fieldConstraints : "");
		PreparedStatement getCPState = (conn!=null ? conn : this.defaultConn).prepareStatement(cmd);
		if(paxosID!=null) getCPState.setString(1, paxosID);
		return getCPState;
	}
	private PreparedStatement getPreparedStatement(Connection conn, String table, String paxosID, String column) throws SQLException {
		return this.getPreparedStatement(conn, table, paxosID, column, "");
	}
	
	public synchronized boolean log(String paxosID, short version, int slot, int ballotnum, int coordinator, PaxosPacketType type, String message) {
		if(isClosed()) return false;

		boolean logged=false;
		//if(duplicateOrOutdated(paxosID, slot, ballotnum, coordinator, type, message)) return false;

		String cmd = "insert into " + getMTable() +  " values (?, ?, ?, ?, ?, ?, ?)";
		PreparedStatement localLogMsgStmt=null;
		try {
			//Re-using prepared statement supposedly reduces per-statement latency
			if(this.logMsgStmt==null) this.logMsgStmt = this.defaultConn.prepareStatement(cmd);
			if(DerbyPaxosLogger.GLOBAL_LOG_STMT) localLogMsgStmt=this.logMsgStmt;  // re-use option
			else localLogMsgStmt = this.defaultConn.prepareStatement(cmd); // no re-use option

			localLogMsgStmt.setString(1, paxosID);
			localLogMsgStmt.setShort(2, version);
			localLogMsgStmt.setInt(3, slot);
			localLogMsgStmt.setInt(4, ballotnum);
			localLogMsgStmt.setInt(5, coordinator);
			localLogMsgStmt.setInt(6, type.getNumber());
			localLogMsgStmt.setString(7, message);
			int rowcount = localLogMsgStmt.executeUpdate();
			assert(rowcount==1);
			logged = true;
			if(DEBUG) log.finest("Inserted (" +paxosID+","+slot+","+ballotnum+","+coordinator+":"+message);
		} catch(SQLException sqle) {
			if(sqle.getSQLState().equals(DerbyPaxosLogger.DUPLICATE_KEY)) {
				if(DEBUG) log.fine("Node " + this.myID + " log message "+message+" previously logged");
				logged=true;
			} else {
				log.severe("SQLException while logging as " + cmd + " : " + sqle);
			}
		} finally {if(!GLOBAL_LOG_STMT) {cleanup(localLogMsgStmt); this.logMsgStmt=null;}} // no cleanup if statement is re-used
		return logged;
	}

	/**
	 * Logs the given packet. The packet must have a paxosID
	 * in it already for this method to be useful.
	 * @param packet
	 */
	public boolean log(PaxosPacket packet) {
		int[] slotballot = PaxosLogTask.getSlotBallot(packet); assert(slotballot.length==3);
		return log(packet.getPaxosID(), packet.getVersion(), slotballot[0], slotballot[1], slotballot[2], packet.getType(), packet.toString());
	}
	/**
	 * Gets the list of logged messages for the paxosID. The static method
	 * PaxosLogger.rollForward(.) can be directly invoked to replay these
	 * messages without explicitly invoking this method.
	 */
	public synchronized ArrayList<PaxosPacket> getLoggedMessages(String paxosID, String fieldConstraints) {
		ArrayList<PaxosPacket> messages = new ArrayList<PaxosPacket>();
		PreparedStatement pstmt=null; ResultSet messagesRS=null;
		try {
			pstmt = this.getPreparedStatement(null, getMTable(), paxosID, "message", fieldConstraints);
			messagesRS = pstmt.executeQuery();
			assert(!messagesRS.isClosed());
			while(messagesRS.next()) {
				String msg = messagesRS.getString(1); 
				PaxosPacket packet = getPaxosPacket(msg);
				messages.add(packet);
			}
		} catch(SQLException sqle) {
			log.severe("SQLException while getting slot for "+paxosID+":"); sqle.printStackTrace();
		} finally {cleanup(pstmt,messagesRS);}
		return messages;
	}
	public ArrayList<PaxosPacket> getLoggedMessages(String paxosID) {
		return this.getLoggedMessages(paxosID, null);
	}
	/* Acceptors remove decisions right after executing them. So they need to fetch
	 * logged decisions from the disk to handle synchronization requests.
	 */
	public ArrayList<PValuePacket> getLoggedDecisions(String paxosID, int minSlot, int maxSlot) {
		ArrayList<PaxosPacket> list = this.getLoggedMessages(paxosID, " and packet_type=" + 
				PaxosPacketType.DECISION.getNumber() + " and slot>="+minSlot + " and slot<"+maxSlot);
		assert(list!=null);
		ArrayList<PValuePacket> decisions = new ArrayList<PValuePacket>();
		for(PaxosPacket p : list) decisions.add((PValuePacket)p);
		return decisions;
	}
	/* Called by an acceptor to return accepted proposals to the new potential 
	 * coordinator. We store and return these from disk to reduce memory
	 * pressure. This allows us to remove accepted proposals once they have
	 * been committed.
	 */
	public Map<Integer,PValuePacket> getLoggedAccepts(String paxosID, int firstSlot) {
		ArrayList<PaxosPacket> list = this.getLoggedMessages(paxosID, " and packet_type=" + 
				PaxosPacketType.ACCEPT.getNumber() + " and slot>"+firstSlot);
		TreeMap<Integer,PValuePacket> accepted = new TreeMap<Integer,PValuePacket>();
		for(PaxosPacket p : list) {
			int slot = PaxosLogTask.getSlotBallot(p)[0];
			AcceptPacket accept = (AcceptPacket)p;
			if(slot>=0 && (!accepted.containsKey(slot) || accepted.get(slot).ballot.compareTo(accept.ballot) < 0))
				accepted.put(slot, accept);
		}
		return accepted;
		
	}
	
	/**
	 * Removes all state for paxosID.
	 */
	public synchronized boolean remove(String paxosID) {
		boolean removedCP=false, removedM=false;
		Statement stmt=null; 
		String cmdC = "delete from " + getCTable() + " where paxos_id='" + paxosID + "'";
		String cmdM = "delete from " + getMTable() + " where paxos_id='" + paxosID + "'";
		try {
			stmt = this.defaultConn.createStatement();
			stmt.execute(cmdC);
			removedCP=true;
			stmt.execute(cmdM);
			removedM=true;
		} catch(SQLException sqle) {
			log.severe("Could not remove table " +(removedCP?getMTable():getCTable())); sqle.printStackTrace();
		} finally{cleanup(stmt);}
		return removedCP && removedM;
	}

	/**
	 * Closes the database and the connection. Must be invoked 
	 * by anyone creating a DerbyPaxosLogger object, otherwise
	 * recovery will take longer upon the next bootup.
	 * @return
	 */
	public synchronized boolean closeGracefully() {

		if (FRAMEWORK.equals("embedded")) {
			try {
				// the shutdown=true attribute shuts down Derby
				DriverManager.getConnection("jdbc:derby:;shutdown=true");

				// To shut down a specific database only, but keep the
				// databases), specify a database in the connection URL:
				//DriverManager.getConnection("jdbc:derby:" + dbName + ";shutdown=true");
			} catch (SQLException sqle) {
				if (( (sqle.getErrorCode() == 50000)
						&& ("XJ015".equals(sqle.getSQLState()) ))) {
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
		setClosed(true);
		try {
			// Close statements
			this.cleanup(logMsgStmt);
			this.cleanup(checkpointStmt);
			this.cleanup(pstmtGlobal);
			this.cleanup(rsetGlobal);
			// Close connection
			if (this.defaultConn != null) {
				defaultConn.close();
				defaultConn = null;
			}
		} catch (SQLException sqle) {
			log.severe("Could not close connection gracefully");sqle.printStackTrace();
		}
		return isClosed();
	}


	/***************** End of public methods ********************/

	/**
	 * Returns a PaxosPacket if parseable from a string.
	 *  FIXME: Utility method should probably be moved elsewhere.
	 */
	protected static PaxosPacket getPaxosPacket(String msg) {
		PaxosPacket paxosPacket = null;
		try {
			JSONObject jsonMsg = new JSONObject(msg);
			PaxosPacketType type = PaxosPacket.getPaxosPacketType(jsonMsg);
			switch(type) {
			case PREPARE:
				paxosPacket = new PreparePacket(jsonMsg);
				break;
			case ACCEPT:
				paxosPacket = new AcceptPacket(jsonMsg);
				break;
			case DECISION:
				paxosPacket = new PValuePacket(jsonMsg);
				break;
			default:
				assert(false);
			}
		} catch(JSONException e) {
			log.severe("Could not parse as JSON: " + msg); e.printStackTrace();
		}
		return paxosPacket;
	}
	private synchronized boolean initialize() {
		if(!isClosed()) return true;
		if(!loadDriver() || !connectDB() || !createTables()) return false;
		setClosed(false);
		return true;
	}

	/**
	 *  Creates a paxosID-primary-key table for checkpoints and another table
	 *  for messages that indexes slot, ballotnum, and coordinator. The checkpoint
	 *  table also stores the slot, ballotnum, and coordinator of the checkpoint.
	 *  The index in the messages table is useful to optimize searching for 
	 *  old messages and deleting them when the checkpoint in the checkpoint 
	 *  table is advanced. The test for "old" is based on the slot, ballotnum, 
	 *  and coordinator fields, so they are indexed.
	 */
	private synchronized boolean createTables() {
		boolean createdCheckpoint=false, createdMessages=false, createdMIndex=true;
		String cmdC = "create table " + getCTable() + " (paxos_id varchar(" + PAXOS_ID_SIZE + 
				") not null, version smallint, members varchar(" + MAX_GROUP_SIZE +  "), slot int, " + 
				"ballotnum int, coordinator int, state varchar(" + MAX_STATE_SIZE + "), " +
				"primary key (paxos_id))";
		String cmdM = "create table " + getMTable() + " (paxos_id varchar(" + PAXOS_ID_SIZE + ") not null, " +
				"version smallint, slot int, ballotnum int, coordinator int, packet_type int, message varchar(" +
				MAX_LOG_MESSAGE_SIZE + "), primary key(paxos_id, slot, ballotnum, coordinator, packet_type))";
		//String cmdMIndex = "create index slot_ballotnum_coordinator on " + getMTable() + " (slot, ballotnum, coordinator)";
		Statement stmt=null;
		try {
			stmt = this.defaultConn.createStatement();
			createdCheckpoint=createTable(stmt, cmdC, getCTable());
			createdMessages=createTable(stmt, cmdM, getCTable());
			//createdMIndex=createTable(stmt, cmdMIndex, getCTable()); 
			log.info("Created tables " + getCTable() + " and "  + getMTable());
		} catch(SQLException sqle) {
			log.severe("Could not create table: "+getMTable() + (createdCheckpoint?"":" and "+getCTable())); 
			sqle.printStackTrace();
		} finally{cleanup(stmt);}
		return createdCheckpoint && createdMessages && createdMIndex;
	}
	private boolean createTable(Statement stmt, String cmd, String table) {
		boolean created = false;
		try {
			stmt.execute(cmd);
			created = true;
		} catch(SQLException sqle) {
			if(sqle.getSQLState().equals(DerbyPaxosLogger.DUPLICATE_TABLE)) {
				log.info("Table "+table+" already exists");
				created = true;
			}
			else {log.severe("Could not create table: "+table); sqle.printStackTrace();}
		}
		return created;
	}

	private boolean connectDB() {
		boolean connected = false; 
		int connAttempts = 0, maxAttempts=5; long interAttemptDelay = 2000; //ms
		Properties props = new Properties(); // connection properties
		// providing a user name and PASSWORD is optional in the embedded
		// and derbyclient frameworks
		props.put("user", DerbyPaxosLogger.USER+this.myID);
		props.put("PASSWORD", DerbyPaxosLogger.PASSWORD);
		System.setProperty("derby.system.home", this.logDirectory); // doesn't seem to work

		while(!connected && connAttempts<maxAttempts) {
			try {
				connAttempts++;
				if(defaultConn==null)
					defaultConn = DriverManager.getConnection(PROTOCOL + this.logDirectory+DATABASE + ";create=true", props);
				if(incrReadConn==null)
					incrReadConn = DriverManager.getConnection(PROTOCOL + this.logDirectory+DATABASE + ";create=true", props);
				log.info("Connected to and created database " + DATABASE);
				connected = true;
				defaultConn.setAutoCommit(AUTO_COMMIT); // NOTE: we want this for paxos correctness
			} catch(SQLException sqle) {
				log.severe("Could not connect to the derby DB: " + sqle);
				try {
					Thread.sleep(interAttemptDelay);
				} catch(InterruptedException ie) {ie.printStackTrace();}
			}
		}
		return connected;
	}

	/**
	 * Loads the appropriate JDBC driver for this environment/framework. For
	 * example, if we are in an embedded environment, we load Derby's
	 * embedded Driver, <code>org.apache.derby.jdbc.EmbeddedDriver</code>.
	 */
	private boolean loadDriver() {
		/*
		 *  The JDBC driver is loaded by loading its class.
		 *  If you are using JDBC 4.0 (Java SE 6) or newer, JDBC drivers may
		 *  be automatically loaded, making this code optional.
		 *
		 *  In an embedded environment, this will also start up the Derby
		 *  engine (though not any databases), since it is not already
		 *  running. In a client environment, the Derby engine is being run
		 *  by the network server framework.
		 *
		 *  In an embedded environment, any static Derby system properties
		 *  must be set before loading the driver to take effect.
		 */
		boolean loaded = false;
		try {
			Class.forName(DRIVER).newInstance();
			loaded = true;
			log.info("Loaded the appropriate driver");
		} catch (ClassNotFoundException cnfe) {
			log.severe("\nUnable to load the JDBC driver " + DRIVER + 
					"\nPlease check your CLASSPATH.");
			cnfe.printStackTrace(System.err);
		} catch (InstantiationException ie) {
			log.severe("\nUnable to instantiate the JDBC driver " + DRIVER);
			ie.printStackTrace(System.err);
		} catch (IllegalAccessException iae) {
			log.severe("\nNot allowed to access the JDBC driver " + DRIVER);
			iae.printStackTrace(System.err);
		}
		return loaded;
	}
	private synchronized boolean isClosed() {return closed;}
	private synchronized void setClosed(boolean c) {closed = c;}

	private String getCTable() {return CHECKPOINT_TABLE+this.myID;}
	private String getMTable() {return MESSAGES_TABLE+this.myID;}

	private synchronized void cleanup(Statement stmt) {
		try {
			if(stmt!=null) {stmt.close();}
		} catch(SQLException sqle) {
			log.severe("Could not clean up statement " + stmt);sqle.printStackTrace();
		}
	}
	private synchronized void cleanup(ResultSet rs) {
		try {
			if(rs!=null) {rs.close();}
		} catch(SQLException sqle) {
			log.severe("Could not close result set " + rs); sqle.printStackTrace();
		}
	}
	private synchronized void cleanup(PreparedStatement pstmt, ResultSet rset) {
		cleanup(pstmt);
		cleanup(rset);
	}
	
	
	/******************** Start of testing methods ***********************/
	// Convenient for testing and debugging
	protected String toString(String paxosID) {
		String print="";
		ArrayList<RecoveryInfo> recoveries = getAllPaxosInstances();
		for(RecoveryInfo pri : recoveries) {
			String s = pri.getPaxosID();
			String state = getCheckpointState(s);
			Ballot b = getCheckpointBallot(s);
			int slot = getCheckpointSlot(s);
			print += (s + " " + Util.arrayToSet(pri.getMembers()) + " " + slot  + " " + b + " " + state+"\n");
			ArrayList<PaxosPacket> loggedMsgs = getLoggedMessages(paxosID);
			if(loggedMsgs!=null) for(PaxosPacket pkt : loggedMsgs) print += (pkt+"\n");
		}
		return print;
	}
	protected boolean isInserted(String paxosID, int[] group, int slot, Ballot ballot, String state) {
		return 
				(getCheckpointState(paxosID, "members").equals(Util.arrayToSet(group).toString())) &&
				(getCheckpointState(paxosID, "slot").equals(""+slot)) &&
				(getCheckpointState(paxosID, "ballotnum").equals(""+ballot.ballotNumber)) &&
				(getCheckpointState(paxosID, "coordinator").equals(""+ballot.coordinatorID)) &&
				(getCheckpointState(paxosID, "state").equals(""+state));
	}
	protected boolean isLogged(String paxosID, int slot, int ballotnum, int coordinator, String msg) {
		PreparedStatement pstmt=null; ResultSet messagesRS=null;
		String cmd = "select paxos_id from " + getMTable() + " where paxos_id='" +paxosID+"' " +
				" and slot="+slot+" and ballotnum="+ballotnum+" and coordinator="+coordinator+" and message=?";
		try {
			pstmt = this.defaultConn.prepareStatement(cmd);
			pstmt.setString(1, msg);
			messagesRS = pstmt.executeQuery();
			while(messagesRS.next()) {
				String insertedPaxosID = messagesRS.getString(1); 
				return insertedPaxosID.equals(paxosID);
			}
		} catch(SQLException sqle) {
			log.severe("SQLException while getting slot " + " : " + sqle);
			cleanup(pstmt,messagesRS);
		}
		return false;
	}
	protected boolean isLogged(PaxosPacket packet) throws JSONException{
		int[] sb = PaxosLogTask.getSlotBallot(packet); assert(sb.length==3);
		return this.isLogged(packet.getPaxosID(), sb[0], sb[1], 
				sb[2], packet.toString());
	}
	private double createCheckpoints(int size) {
		int[] group = {2, 4, 5, 11, 23, 34, 56, 78, 80, 83, 85, 96, 97, 98, 99};
		System.out.println("\nStarting " + size + " writes: ");
		long t1 = System.currentTimeMillis(), t2=t1;
		int k=1; DecimalFormat df = new DecimalFormat("#.##");
		for(int i=0; i<size; i++) {
			this.putCheckpointState("paxos"+i, (short)0, group, 0, new Ballot(0, i%34), "hello"+i, 0);
			t2 = System.currentTimeMillis();
			if(i%k==0 && i>0) {System.out.print("[" + i+" : " + df.format(((double)(t2-t1))/i) + "ms]\n"); k*=2;}
		}
		return (double)(t2-t1)/size;
	}
	private double readCheckpoints(int size) {
		System.out.println("\nStarting " + size + " reads: ");
		long t1 = System.currentTimeMillis(), t2=t1;
		int k=1; DecimalFormat df = new DecimalFormat("#.##");
		for(int i=0; i<size; i++) {
			this.getStatePacket("paxos"+i);
			t2 = System.currentTimeMillis();
			if(i%k==0 && i>0) {System.out.print("[" + i+" : " + df.format(((double)(t2-t1))/i) + "ms]\n"); k*=2;}
		}
		return (double)(t2-t1)/size;
	}


	/**
	 * @param args
	 */
	public static void main(String[] args) {
		DerbyPaxosLogger logger = new DerbyPaxosLogger(23,null);
		//System.out.println("Current database state for paxos0:\n" + logger.toString("paxos0"));
		
		int[] group = {32, 43, 54};
		String paxosID = "paxos0";
		int slot = 0;
		int ballotnum=1;
		int coordinator=2;
		String state = "Hello World";
		Ballot ballot = new Ballot(ballotnum,coordinator);
		logger.putCheckpointState(paxosID, (short)0, group, slot, ballot, state, 0);
		assert(logger.isInserted(paxosID, group, slot, ballot, state));
		DecimalFormat df = new DecimalFormat("#.##");

		int million=1000000;
		int size = (int)(0.1*million);

		double avg_write_time = logger.createCheckpoints(size);
		System.out.println("Average time to write " + size + " checkpoints = " + df.format(avg_write_time));
		double avg_read_time = logger.readCheckpoints(size);
		System.out.println("Average time to read " + size + " checkpoints = " + df.format(avg_read_time));

		try {
			int numPackets = 1000;
			System.out.println("\nStarting " + numPackets + " message logs: ");

			PaxosPacket[] packets = new PaxosPacket[numPackets];
			long time =0;
			int i=0;
			int reqClientID=25; 
			String reqValue = "26";
			int nodeID = coordinator;
			int k=1;
			for(int j=0; j<packets.length; j++) {
				RequestPacket req = new RequestPacket(reqClientID,  reqValue, false);
				ProposalPacket prop = new ProposalPacket(i, req);
				PValuePacket pvalue = new PValuePacket(ballot, prop);
				AcceptPacket accept = new AcceptPacket(nodeID, pvalue, -1);
				pvalue = pvalue.getDecisionPacket(-1);
				PreparePacket prepare = new PreparePacket(coordinator, nodeID, ballot);
				if(j%3==0) { // prepare
					packets[j] = prepare;
				} else if(j%3==1) { // accept
					packets[j] = accept;
				} else if(j%3==2) { // decision
					packets[j] = pvalue; 
				}
				if(j%3==2) i++;
				packets[j].putPaxosID(paxosID, (short)0);
				long t1 = System.currentTimeMillis();
				logger.log(packets[j]);
				long t2 = System.currentTimeMillis();
				time += t2-t1;
				if(j>0 && (j%k==0 || j%million==0)) {System.out.println("[" + j+" : " + df.format(((double)time)/j) + "ms] "); k*=2;}
			}
			System.out.print("Checking logged messages...");
			for(int j=0; j<packets.length;j++) {
				assert(logger.isLogged(packets[j]));
			}
			System.out.println("checked");

			int newSlot = 2;
			Ballot newBallot = new Ballot(0,2);
			logger.putCheckpointState(paxosID, (short)0, group, 2, newBallot, "Hello World",0);
			System.out.println("Printing logger state after checkpointing:");
			logger.initiateReadCheckpoints();
			RecoveryInfo pri=null;
			while((pri = logger.readNextCheckpoint())!=null) {
				assert(pri!=null); //System.out.println(pri);
			}
			System.out.print("Checking deletion of logged messages...");
			for(int j=0; j<packets.length;j++) {
				int[] sbc = PaxosLogTask.getSlotBallot(packets[j]);
				if(!((sbc[0] <= newSlot && sbc[0]>=0) || ((sbc[1]<newBallot.ballotNumber || 
						(sbc[1]==newBallot.ballotNumber && sbc[2]<newBallot.coordinatorID) )))) {
					assert(logger.isLogged(packets[j]));
				}
			}
			System.out.println("checked");
			
			logger.closeGracefully();
			System.out.println("SUCCESS: No exceptions or assertion violations were triggered. " +
					"Average log time over " + numPackets + " packets = " + ((double)time)/numPackets+" ms");
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

}
