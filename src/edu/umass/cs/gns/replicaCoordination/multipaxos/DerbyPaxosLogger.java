package edu.umass.cs.gns.replicaCoordination.multipaxos;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.AcceptPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.PValuePacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.PaxosPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.PaxosPacket.PaxosPacketType;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.PreparePacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.ProposalPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.RequestPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.StatePacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.Ballot;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.HotRestoreInfo;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.Messenger;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.RecoveryInfo;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.SlotBallotState;
import edu.umass.cs.gns.util.Util;
import org.json.JSONException;
import org.json.JSONObject;

import javax.sql.DataSource;
import java.beans.PropertyVetoException;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
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
	private static final boolean CONN_POOLING= true;
	private static final String DUPLICATE_KEY = "23505";
	private static final String DUPLICATE_TABLE = "X0Y32";
	private static final String NONEXISTENT_TABLE = "42Y07";
	private static final String USER = "user"; 
	private static final String PASSWORD = "user";
	private static final String DATABASE = "paxos_logs";
	private static final String CHECKPOINT_TABLE = "checkpoint";
	private static final String PAUSE_TABLE = "pause";
	private static final String MESSAGES_TABLE = "messages";
	private static final boolean DISABLE_LOGGING = false;
	private static final boolean AUTO_COMMIT = true; 
	private static final int MAX_POOL_SIZE = 100;
	private static final int PAXOS_ID_SIZE = 40; // FIXME: GUID is 20 bytes, but its hex-byte representation is bloated
	private static final int PAUSE_STATE_SIZE = 256;
	private static final int MAX_GROUP_SIZE = 256; // maximum size of a paxos replica group
	private static final int MAX_LOG_MESSAGE_SIZE = 4096; // maximum size of a log message
	private static final int MAX_OLD_DECISIONS = PaxosInstanceStateMachine.INTER_CHECKPOINT_INTERVAL;

	private ComboPooledDataSource dataSource=null;
	private Connection defaultConn = null;
	private Connection cursorConn = null; 

	private boolean closed = true;
	private boolean processing = false;
	private synchronized boolean getProcessing() {return this.processing;}
	private synchronized void setProcessing(boolean b) {this.processing=b; if(!b) notify();}
	protected static boolean isLoggingEnabled() {return !DISABLE_LOGGING;}

	/* The global statements are not really need and can be replaced
	 * by local variables in log(.) and duplicateOrOutdated(.)
	 * but are supposedly more efficient. But they don't seem
	 * to speed it up much. But at some point, they did, so
	 * these are still being used.
	 */
	private PreparedStatement logMsgStmt=null;
	private PreparedStatement checkpointStmt=null;
	private PreparedStatement cursorPstmt = null;
	private ResultSet cursorRset=null;

	private static Logger log = Logger.getLogger(DerbyPaxosLogger.class.getName()); // GNS.getLogger();

	DerbyPaxosLogger(int id, String dbPath, Messenger messenger) {
		super(id, dbPath, messenger);
		initialize(); // will set up db, connection, tables, etc. as needed
	}
	private synchronized Connection getDefaultConn() throws SQLException {return dataSource.getConnection();}
	private synchronized Connection getCursorConn() throws SQLException {
		return (this.cursorConn = this.dataSource.getConnection());
	}

	/* The entry point for checkpointing. Puts given checkpoint state 
	 * for paxosID. 'state' could be anything that allows PaxosInterface 
	 * to later restore the corresponding state. For example, 'state' 
	 * could be the name of a file where the app maintains a checkpoint 
	 * of all of its state. It could of course be the stringified form 
	 * of the actual state if the state is at most MAX_STATE_SIZE. 
	 */
	public void putCheckpointState(String paxosID, short version, int[] group, int slot, Ballot ballot, String state, int acceptedGCSlot) {
		if(isClosed() || DISABLE_LOGGING) return;

		long t1=System.currentTimeMillis(),t2=0;
		// Stupid derby doesn't have an insert if not exist command
		String insertCmd = "insert into " + getCTable() + " (version,members,slot,ballotnum,coordinator,state,paxos_id) values (?,?,?,?,?,?,?)";
		String updateCmd = "update " + getCTable() + " set version=?,members=?, slot=?, ballotnum=?, coordinator=?, state=? where paxos_id=?";
		String cmd = this.getCheckpointState(paxosID,"paxos_id")!=null ? updateCmd : insertCmd; 
		PreparedStatement insertCP=null; Connection conn=null;
		try {
			conn = this.getDefaultConn();
			insertCP = conn.prepareStatement(cmd);
			insertCP.setShort(1, version);
			insertCP.setString(2, Util.arrayToSet(group).toString());
			insertCP.setInt(3, slot);
			insertCP.setInt(4, ballot.ballotNumber);
			insertCP.setInt(5, ballot.coordinatorID);
      insertCP.setClob(6, new StringReader(state));
			insertCP.setString(7, paxosID);
			insertCP.executeUpdate(); 
			conn.commit();
			t2=System.currentTimeMillis();
		} catch(SQLException sqle) {
			log.severe("SQLException while checkpointing as " + cmd); sqle.printStackTrace();
		} finally {cleanup(insertCP);cleanup(conn);}

		/* Delete logged messages from before the checkpoint. Note: Putting this before cleanup(conn) 
		 * above can cause deadlock if we don't have at least 2x the number of connections as 
		 * concurrently active paxosIDs. Realized this the hard way. :)
		 */
		this.deleteOutdatedMessages(paxosID, slot, ballot.ballotNumber, ballot.coordinatorID, acceptedGCSlot);
		// why can't insertCP.toString() return the query string? :/
    // Abhigyan: logging a huge string would add much overhead, so truncate to 8KB
    String logState = (state.length() < 8192) ? state: state.substring(0, 8192);

		log.info("Node " + this.myID + " DB inserted checkpoint ("+paxosID+","+Util.arrayToSet(group).toString()+","+
				slot+","+ballot+","+logState+","+acceptedGCSlot+"); took " + (t2-t1) + "ms; garbage collection took " +
				(System.currentTimeMillis()-t2)+"ms");
	}
	/* Called by putCheckpointState to delete logged messages from before the checkpoint. */
	private void deleteOutdatedMessages(String paxosID, int slot, int ballotnum, int coordinator, int acceptedGCSlot) {
		if(isClosed()) return;
		if(slot<PaxosInstanceStateMachine.INTER_CHECKPOINT_INTERVAL) return; // an optimization to avoid GC at slot 0

		PreparedStatement pstmt=null,dstmt=null; ResultSet checkpointRS=null; Connection conn=null;
		try {
			// Create delete command using the slot, ballot, and gcSlot
			String dcmd = "delete from " + getMTable() + " where paxos_id='"+paxosID+"' and " +
					"(packet_type="+PaxosPacketType.DECISION.getNumber() + " and slot<"+(slot-MAX_OLD_DECISIONS)+") or " +
					"(packet_type="+PaxosPacketType.PREPARE.getNumber() + " and (ballotnum<"+ballotnum + 
					" or (ballotnum="+ballotnum + " and coordinator<"+coordinator+"))) or" +
					"(packet_type="+PaxosPacketType.ACCEPT.getNumber() + " and slot<="+Math.min(acceptedGCSlot, slot)+")"; 
			conn = getDefaultConn();
			dstmt = conn.prepareStatement(dcmd);
			dstmt.execute();
			conn.commit();
		} catch(SQLException sqle) {
			log.severe("SQLException while deleting outdated messages for " + paxosID); sqle.printStackTrace();
		} finally {cleanup(pstmt,checkpointRS);cleanup(dstmt); cleanup(conn);}
	}

	/* Used to be the entry point for message logging. Replaced by batchLog now */
	public boolean log(String paxosID, short version, int slot, int ballotnum, int coordinator, PaxosPacketType type, String message) {
		if(isClosed() || DISABLE_LOGGING) return false;

		boolean logged=false;
		//if(duplicateOrOutdated(paxosID, slot, ballotnum, coordinator, type, message)) return false;

		String cmd = "insert into " + getMTable() +  " values (?, ?, ?, ?, ?, ?, ?)";
		PreparedStatement localLogMsgStmt=null; Connection conn=null;
		try {
			conn=this.getDefaultConn();
			localLogMsgStmt = conn.prepareStatement(cmd); // no re-use option

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
				log.severe("SQLException while logging as " + cmd + " : " + sqle); 	sqle.printStackTrace();
			}
		} 
		finally {cleanup(localLogMsgStmt); cleanup(conn);} // no cleanup if statement is re-used
		return logged;
	}
	/* Can not start pause or unpause after close has been called. For other 
	 * operations like checkpointing or logging, we need to be able to do 
	 * them even after close has been called as waitToFinish needs that.
	 */
	public synchronized boolean pause(String paxosID, String serializedState) {
		if(isCloseCalled() || DISABLE_LOGGING) return false; else this.setProcessing(true);

		boolean paused = false;
		String insertCmd = "insert into " + getPTable() + " (serialized, paxos_id) values (?,?)";
		//String updateCmd = "update " + getPTable() + " set serialized=? where paxos_id=?";
		//this.deletePaused(paxosID); // ignore return value
		String cmd = insertCmd; //this.unpause(paxosID)!=null ? updateCmd : insertCmd; 
		PreparedStatement insertP=null; Connection conn=null;
		try {
			conn = this.getDefaultConn();
			insertP = conn.prepareStatement(cmd);
			insertP.setString(1, serializedState);
			insertP.setString(2, paxosID);
			insertP.executeUpdate();
			paused = true;
		} catch(SQLException sqle) {
			log.severe("Node "+myID+" failed to pause instance "+paxosID);
			this.deletePaused(paxosID); 
			sqle.printStackTrace();
		} finally {cleanup(insertP); cleanup(conn);};
		this.setProcessing(false);
		return paused;
	}
	public synchronized HotRestoreInfo unpause(String paxosID) {
		if(isCloseCalled() || DISABLE_LOGGING) return null; else this.setProcessing(true);

		HotRestoreInfo hri = null;
		PreparedStatement pstmt=null; ResultSet rset=null; Connection conn=null;
		try {
			conn = this.getDefaultConn();
			pstmt =this.getPreparedStatement(conn, getPTable(), paxosID, "serialized"); //conn.prepareStatement(cmd);
			rset = pstmt.executeQuery();
			while(rset.next()) {
				assert(hri==null); // exactly onece
				String serialized = rset.getString(1);
				hri = new HotRestoreInfo(serialized);
			}
		} catch(SQLException sqle) {
			log.severe("Node "+myID+" failed to pause instance "+paxosID);
			sqle.printStackTrace();
		} finally {cleanup(pstmt,rset); cleanup(conn);};
		if(hri!=null) this.deletePaused(paxosID); // unpause will also delete paused state
		this.setProcessing(false);
		return hri;
	}
	private void deletePaused(String paxosID) {
		if(isClosed() || DISABLE_LOGGING) return;
		PreparedStatement pstmt=null; ResultSet rset=null; Connection conn=null;
		try {
			conn = this.getDefaultConn();
			pstmt = conn.prepareStatement("delete from " + getPTable() + " where paxos_id='" + paxosID+"'");
			pstmt.executeUpdate();
			conn.commit();
		}  catch(SQLException sqle) {
			log.severe("Node "+myID+" failed to delete paused state for "+paxosID);
			sqle.printStackTrace();
		} finally {cleanup(pstmt,rset); cleanup(conn);};
	}
	
	public synchronized void waitToFinish() {
		try {
			while(this.getProcessing()) wait();
		} catch(InterruptedException ie) {ie.printStackTrace();}
	}

	/**
	 * Gets current checkpoint. There can be only one checkpoint for a
	 * paxosID at any time. 
	 */
	public String getCheckpointState(String paxosID) {return this.getCheckpointState(paxosID, "state");}
	private synchronized String getCheckpointState(String paxosID, String column) {
		if(isClosed()) return null;

		String state = null;
		PreparedStatement pstmt=null; ResultSet stateRS=null; Connection conn=null;
		try {
			conn = getDefaultConn();
			pstmt = getPreparedStatement(conn, getCTable(), paxosID, column);
			stateRS = pstmt.executeQuery();
			while(stateRS.next()) {
				assert(state==null); // single result
        // convert from clob to string
        StringBuilder sb = new StringBuilder();
        BufferedReader br = new BufferedReader(stateRS.getClob(1).getCharacterStream());
        while (true) {
          String s = br.readLine();
          if (s == null) break;
          sb.append(s);
        }
        state = sb.toString();
			}
    } catch (IOException e) {
      log.severe("IOException while getting state " + " : " + e);
      e.printStackTrace();
    } catch(SQLException sqle) {
			log.severe("SQLException while getting state " + " : " + sqle);
		} finally {cleanup(pstmt,stateRS);cleanup(conn);}

		return state;
	}
	/* Methods to get slot, ballotnum, and coordinator of checkpoint */
	public synchronized SlotBallotState getSlotBallotState(String paxosID, short version, boolean matchVersion) {
		if(isClosed()) return null;

		SlotBallotState sb=null;
		ResultSet stateRS=null;
		Connection conn=null;
		boolean versionMismatch=false;
		try {
			conn = this.getDefaultConn(); assert(conn!=null);
			if(this.checkpointStmt==null || this.checkpointStmt.isClosed()) this.checkpointStmt = 
					this.getPreparedStatement(conn, getCTable(), paxosID, "slot, ballotnum, coordinator, state, version");
			this.checkpointStmt.setString(1, paxosID);
			stateRS = this.checkpointStmt.executeQuery();
			while(stateRS.next()) {
				assert(sb==null); // single result
				sb = new SlotBallotState(
						stateRS.getInt(1),
						stateRS.getInt(2),
						stateRS.getInt(3), 
						stateRS.getString(4));
				versionMismatch = (matchVersion && version!=stateRS.getShort(5));
			}
		} catch(SQLException sqle) {
			log.severe("SQLException while getting slot " + " : " + sqle); sqle.printStackTrace();
		} finally {cleanup(stateRS);cleanup(conn);}
		return versionMismatch ? null : sb;
	}
	public synchronized SlotBallotState getSlotBallotState(String paxosID) {
		return this.getSlotBallotState(paxosID, (short)0, false);
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
		PreparedStatement pstmt=null; ResultSet stateRS=null; Connection conn=null;
		try {
			conn = this.getDefaultConn();
			pstmt = this.getPreparedStatement(conn, getCTable(), null, "paxos_id, version, members");
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
		} finally {cleanup(pstmt,stateRS);cleanup(conn);}
		return allPaxosInstances;
	}
	public synchronized RecoveryInfo getRecoveryInfo(String paxosID) {
		if(isClosed()) return null;

		RecoveryInfo pri = null;
		PreparedStatement pstmt=null; ResultSet stateRS=null;
		Connection conn=null;

		try {
			conn = this.getDefaultConn();
			pstmt = this.getPreparedStatement(conn, getCTable(), paxosID, "version, members");
			stateRS = pstmt.executeQuery();
			while(stateRS!=null && stateRS.next()) {
				short version = stateRS.getShort(1);
				String members = stateRS.getString(2);
				String[] pieces = members.replaceAll("\\[|\\]", "").split(",");
				int[] group = new int[pieces.length];
				for(int i=0; i<group.length; i++) group[i] = Integer.parseInt(pieces[i].trim());
				pri = new RecoveryInfo(paxosID, version, group);
			}
		} catch(SQLException sqle) {
			log.severe("SQLException while getting all paxos IDs " + " : " + sqle);
		} finally {cleanup(pstmt,stateRS);cleanup(conn);}
		return pri;
	}

	/************* Start of incremental checkpoint read methods **********************/
	protected synchronized boolean initiateReadCheckpoints(boolean readState) {
		if(isClosed() || this.cursorPstmt!=null || this.cursorRset!=null || this.cursorConn!=null) return false;

		log.info("Node "+this.myID + " initiatedReadCheckpoints"); 
		boolean initiated = false;
		try {
			this.cursorPstmt = this.getPreparedStatement(this.getCursorConn(), getCTable(), null, 
					"paxos_id, version, members" + (readState? ", state" : ""));
			this.cursorRset = this.cursorPstmt.executeQuery();
			initiated = true;
		} catch(SQLException sqle) {
			log.severe("SQLException while getting all paxos IDs " + " : " + sqle);
		}
		return initiated;
	}
	protected synchronized RecoveryInfo readNextCheckpoint(boolean readState) {
		RecoveryInfo pri = null;
		try {
			if(cursorRset!=null && cursorRset.next()) {
				String paxosID = cursorRset.getString(1);
				short version = cursorRset.getShort(2);
				String members = cursorRset.getString(3);
				String[] pieces = members.replaceAll("\\[|\\]", "").split(",");
				int[] group = new int[pieces.length];
				for(int i=0; i<group.length; i++) group[i] = Integer.parseInt(pieces[i].trim());
				String state = (readState ? cursorRset.getString(4) : null);
				pri = new RecoveryInfo(paxosID, version, group, state);
			}
		} catch(SQLException sqle) {
			log.severe("SQLException in readNextCheckpoint: " + " : " + sqle);
		}
		return pri;
	}
	protected synchronized boolean initiateReadMessages() {
		if(isClosed() || this.cursorPstmt!=null || this.cursorRset!=null || this.cursorConn!=null) return false;

		log.info("Node "+this.myID + " initiatedReadMessages"); 
		boolean initiated = false;
		try {
			this.cursorPstmt = this.getPreparedStatement(this.getCursorConn(), getMTable(), null, "message");
			this.cursorRset = this.cursorPstmt.executeQuery();
			initiated = true;
		} catch(SQLException sqle) {
			log.severe("SQLException while getting all paxos IDs " + " : " + sqle);
		}
		return initiated;
	}
	protected synchronized PaxosPacket readNextMessage() {
		PaxosPacket packet = null;
		try {
			if(cursorRset!=null && cursorRset.next()) {
				String msg = cursorRset.getString(1);
				packet = DerbyPaxosLogger.getPaxosPacket(msg);
			}
		} catch(SQLException sqle) {
			log.severe("Node " + this.myID + " got SQLException in readNextMessage: " + " : " + sqle); sqle.printStackTrace();
		}
		return packet;	
	}
	protected synchronized void closeReadAll() {
		log.info("Node " + myID + " invoking closeReadAll");
		this.cleanupCursorConn();
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
		PreparedStatement getCPState = (conn!=null ? conn : this.getDefaultConn()).prepareStatement(cmd);
		if(paxosID!=null) getCPState.setString(1, paxosID);
		return getCPState;
	}
	private PreparedStatement getPreparedStatement(Connection conn, String table, String paxosID, String column) throws SQLException {
		return this.getPreparedStatement(conn, table, paxosID, column, "");
	}

	/**
	 * Logs the given packet. The packet must have a paxosID
	 * in it already for this method to be useful.
	 * @param packet
	 */
	public boolean log(PaxosPacket packet) {
		int[] slotballot = AbstractPaxosLogger.getSlotBallot(packet); assert(slotballot.length==3);
		return log(packet.getPaxosID(), packet.getVersion(), slotballot[0], slotballot[1], slotballot[2], packet.getType(), packet.toString());
	}
	/**
	 * Gets the list of logged messages for the paxosID. The static method
	 * PaxosLogger.rollForward(.) can be directly invoked to replay these
	 * messages without explicitly invoking this method.
	 */
	public synchronized ArrayList<PaxosPacket> getLoggedMessages(String paxosID, String fieldConstraints) {
		ArrayList<PaxosPacket> messages = new ArrayList<PaxosPacket>();
		PreparedStatement pstmt=null; ResultSet messagesRS=null; Connection conn=null;
		try {
			conn = this.getDefaultConn();
			pstmt = this.getPreparedStatement(conn, getMTable(), paxosID, "message", fieldConstraints);
			messagesRS = pstmt.executeQuery();

			assert(!messagesRS.isClosed());
			while(messagesRS.next()) {
				String msg = messagesRS.getString(1); 
				PaxosPacket packet = getPaxosPacket(msg);
				messages.add(packet);
			}
		} catch(SQLException sqle) {
			log.severe("SQLException while getting slot for "+paxosID+":"); sqle.printStackTrace();
		} finally {cleanup(pstmt,messagesRS);cleanup(conn);}
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
			int slot = AbstractPaxosLogger.getSlotBallot(p)[0];
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
		boolean removedCP=false, removedM=false, removedP=false;
		Statement stmt=null; 
		String cmdC = "delete from " + getCTable() + (paxosID!=null ? " where paxos_id='" + paxosID + "'" : " where true");
		String cmdM = "delete from " + getMTable() + (paxosID!=null ? " where paxos_id='" + paxosID + "'" : " where true");
		String cmdP = "delete from " + getPTable() + (paxosID!=null ? " where paxos_id='" + paxosID + "'" : " where true");
		Connection conn=null;
		try {
			conn = this.getDefaultConn();
			stmt = conn.createStatement();
			stmt.execute(cmdC);
			removedCP=true;
			stmt.execute(cmdM);
			removedM=true;
			stmt.execute(cmdP);
			removedP=true;
			conn.commit();
		} catch(SQLException sqle) {
			if(!removedP) log.severe("Could not remove table " +(removedCP?(removedM?getPTable():
				getMTable()):getCTable())); sqle.printStackTrace();
		} finally{cleanup(stmt);cleanup(conn);}
		return removedCP && removedM;
	}
	public synchronized boolean removeAll() {
		return this.remove(null);
	}
	public void close() {
		log.info("Node "+this.myID+" closing DB");
		if(!firstClose()) return; 
		this.closeGracefully();
	}

	/**
	 * Closes the database and the connection. Must be invoked 
	 * by anyone creating a DerbyPaxosLogger object, otherwise
	 * recovery will take longer upon the next bootup.
	 * @return
	 */
	public boolean closeGracefully() {

		/* If there are pending tasks and the DB is closed ungracefully, 
		 * this can cause problems upon recovery. The DB is consistent 
		 * as per its design but it takes some time upon recovery for it 
		 * to rollback or roll forward incomplete tasks. What this means 
		 * is that some logged messages may not be available when a 
		 * recovering node reads them to roll forward but may suddenly 
		 * become available a little while later when the logged messages 
		 * finally get committed. This triggers some assert violations in 
		 * the paxos code as prepare replies contain positive replies even 
		 * though the list of contained accepts implies otherwise.
		 */
		waitToFinishAll(); // parent method needs to be called to finish gracefully

		if (FRAMEWORK.equals("embedded")) {
			try {
				// the shutdown=true attribute shuts down Derby
				DriverManager.getConnection(PROTOCOL + ";shutdown=true");

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
			log.severe("Could not close connection gracefully");sqle.printStackTrace();
		}
		return isClosed();
	}


	/***************** End of public methods ********************/
	/**
	 * Returns a PaxosPacket if parseable from a string.
	 *  FIXME: Utility method should probably be moved elsewhere.
	 */
	private static PaxosPacket getPaxosPacket(String msg) {
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
		if(/*!loadDriver() ||*/ !connectDB() || !createTables()) return false;
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
		boolean createdCheckpoint=false, createdMessages=false, createdPTable=false;
		String cmdC = "create table " + getCTable() + " (paxos_id varchar(" + PAXOS_ID_SIZE + 
				") not null, version smallint, members varchar(" + MAX_GROUP_SIZE +  "), slot int, " + 
				"ballotnum int, coordinator int, state clob, " +
				"primary key (paxos_id))";
		String cmdM = "create table " + getMTable() + " (paxos_id varchar(" + PAXOS_ID_SIZE + ") not null, " +
				"version smallint, slot int, ballotnum int, coordinator int, packet_type int, message varchar(" +
				MAX_LOG_MESSAGE_SIZE + ")/*, primary key(paxos_id, slot, ballotnum, coordinator, packet_type)*/)";
		// FIXME: How to batch while avoiding duplicate key exceptions with indexing??? Aargh!
		String cmdMI = "create index messages_index on " + getMTable() + "(paxos_id,packet_type,slot,ballotnum,coordinator)";
		String cmdP = "create table " + getPTable() + " (paxos_id varchar(" + PAXOS_ID_SIZE + 
				") not null, serialized varchar("+PAUSE_STATE_SIZE+") not null, primary key (paxos_id))";
		if(this.dbDirectoryExists()) this.dropTable(getPTable()); // using pause table unnecessarily slows down recovery significantly
		Statement stmt=null; Connection conn=null;
		try {
			conn = this.getDefaultConn();
			stmt = conn.createStatement();
			createdCheckpoint=createTable(stmt, cmdC, getCTable());
			createdMessages=createTable(stmt, cmdM, getCTable()) && createIndex(stmt, cmdMI, getMTable());
			createdPTable=createTable(stmt, cmdP, getPTable());
			log.info("Created tables " + getCTable() + " and "  + getMTable() + " and " + getPTable());
		} catch(SQLException sqle) {
			log.severe("Could not create table(s): "+getPTable() + (createdMessages ? 
					(createdCheckpoint ? "" : " and "+getMTable()+" and "+getCTable()) : getMTable())); 
			sqle.printStackTrace();
		} finally{cleanup(stmt);cleanup(conn);}
		return createdCheckpoint && createdMessages && createdPTable;
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
			log.info("Node " +myID+ " dropped pause table " + table);
		} catch(SQLException sqle) {
			if(!sqle.getSQLState().equals(NONEXISTENT_TABLE)) {
				log.severe("Node " + this.myID + " could not drop table " + table + ":"+sqle.getSQLState());
				sqle.printStackTrace();
			}
		}
		return dropped;
	}

	private boolean dbDirectoryExists() {
		File f = new File(this.logDirectory+DATABASE);
		return f.exists() && f.isDirectory();
	}
	private boolean connectDB() {
		boolean connected = false; 
		int connAttempts = 0, maxAttempts=1; long interAttemptDelay = 2000; //ms
		Properties props = new Properties(); // connection properties
		// providing a user name and PASSWORD is optional in the embedded
		// and derbyclient frameworks
		props.put("user", DerbyPaxosLogger.USER+this.myID);
		props.put("password", DerbyPaxosLogger.PASSWORD);
		System.setProperty("derby.system.home", this.logDirectory); // doesn't seem to work

		String dbCreation = PROTOCOL + this.logDirectory+DATABASE + (!dbDirectoryExists() /*&& isFirstConnect()*/ ? ";create=true" : "");

		try {
			dataSource = (ComboPooledDataSource)setupDataSourceC3P0(dbCreation, props);
		} catch(SQLException e) {
			log.severe("Could not build pooled data source"); e.printStackTrace();
		}

		while(!connected && connAttempts<maxAttempts) {
			try {
				connAttempts++;
				log.info("Attempting getCursorConn() to db " + dbCreation);
				if(getCursorConn()==null) this.cursorConn = dataSource.getConnection(); //test opening a connection
				log.info("Connected to and created database " + DATABASE);
				connected = true;
				fixURI();
			} catch(SQLException sqle) {
				log.severe("Could not connect to the derby DB: " + sqle.getErrorCode()+":"+sqle.getSQLState());
				sqle.printStackTrace();
				try {
					Thread.sleep(interAttemptDelay);
				} catch(InterruptedException ie) {ie.printStackTrace();}
			} finally {cleanupCursorConn();} // close the test connection
		}
		return connected;
	}

	private synchronized boolean isClosed() {return closed;}
	private synchronized void setClosed(boolean c) {closed = c;}

	private String getCTable() {return CHECKPOINT_TABLE+this.myID;}
	private String getMTable() {return MESSAGES_TABLE+this.myID;}
	private String getPTable() {return PAUSE_TABLE+this.myID;}

	private synchronized void cleanupCursorConn() {
		try {
			if(this.cursorConn!=null && CONN_POOLING) {this.cursorConn.close(); this.cursorConn=null;}
			if(this.cursorPstmt!=null) {this.cursorPstmt.close(); this.cursorPstmt=null;}
			if(this.cursorRset!=null) {this.cursorRset.close(); this.cursorRset=null;}
		} catch(SQLException sqle) {
			log.severe("Could not close connection " + this.cursorConn);sqle.printStackTrace();
		}
	}

	private void cleanup(Connection conn) {
		try {
			if(conn!=null && CONN_POOLING) {conn.close();}
		} catch(SQLException sqle) {
			log.severe("Could not close connection " + conn);sqle.printStackTrace();
		}
	}
	private void cleanup(Statement stmt) {
		try {
			if(stmt!=null) {stmt.close();}
		} catch(SQLException sqle) {
			log.severe("Could not clean up statement " + stmt);sqle.printStackTrace();
		}
	}
	private void cleanup(ResultSet rs) {
		try {
			if(rs!=null) {rs.close();}
		} catch(SQLException sqle) {
			log.severe("Could not close result set " + rs); sqle.printStackTrace();
		}
	}
	private void cleanup(PreparedStatement pstmt, ResultSet rset) {
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
		PreparedStatement pstmt=null; ResultSet messagesRS=null; Connection conn=null;
		String cmd = "select paxos_id, message from " + getMTable() + " where paxos_id='" +paxosID+"' " +
				" and slot="+slot+" and ballotnum="+ballotnum+" and coordinator="+coordinator +" and message=?";
		boolean logged = false;
		try {
			conn = this.getDefaultConn();
			pstmt = conn.prepareStatement(cmd);
			pstmt.setString(1, msg);
			messagesRS = pstmt.executeQuery();
			while(messagesRS.next() && !logged) {
				String insertedMsg = messagesRS.getString(2);
				logged =  msg.equals(insertedMsg);
			}
		} catch(SQLException sqle) {
			log.severe("SQLException while getting slot " + " : " + sqle);

		} finally{cleanup(pstmt,messagesRS);cleanup(conn);}
		return logged;
	}
	protected boolean isLogged(PaxosPacket packet) throws JSONException{
		int[] sb = AbstractPaxosLogger.getSlotBallot(packet); assert(sb.length==3);
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
	public static DataSource setupDataSourceC3P0(String connectURI, Properties props) throws SQLException {

		ComboPooledDataSource cpds = new ComboPooledDataSource();
		try {
			cpds.setDriverClass(DRIVER);
			cpds.setJdbcUrl(connectURI);
			cpds.setUser(props.getProperty("user"));
			cpds.setPassword(props.getProperty("password"));
			cpds.setAutoCommitOnClose(AUTO_COMMIT);
			cpds.setMaxPoolSize(MAX_POOL_SIZE);
		} catch(PropertyVetoException pve) {pve.printStackTrace();}

		return cpds;
	}
	private void fixURI() {this.dataSource.setJdbcUrl(PROTOCOL + this.logDirectory+DATABASE);}

	public synchronized boolean logBatch(PaxosPacket[] packets) {
		if(isClosed() || DISABLE_LOGGING) return false;
		boolean logged = true;
		PreparedStatement pstmt = null; Connection conn = null;
		String cmd = "insert into " + getMTable() +  " values (?, ?, ?, ?, ?, ?, ?)";
		long t1=System.currentTimeMillis();
		for(int i=0; i<packets.length; i++) {
			try {
				if(conn==null) {
					conn = this.getDefaultConn();
					conn.setAutoCommit(false);
					pstmt = conn.prepareStatement(cmd);
				}
				PaxosPacket packet = packets[i];
				String packetString=null;
				try {
					packetString = PaxosPacket.setRecovery(packet);
				} catch(JSONException je) {je.printStackTrace(); continue;}
				int[] sb = AbstractPaxosLogger.getSlotBallot(packet);

				pstmt.setString(1, packet.getPaxosID());
				pstmt.setShort(2, packet.getVersion());
				pstmt.setInt(3, sb[0]);
				pstmt.setInt(4, sb[1]);
				pstmt.setInt(5, sb[2]);
				pstmt.setInt(6, packet.getType().getNumber());
				pstmt.setString(7, packetString);
				pstmt.addBatch();
				if((i+1)%1000==0 || (i+1)==packets.length) {
					int[] executed = pstmt.executeBatch();
					conn.commit();
					pstmt.clearBatch();
					for(int j : executed) logged = logged && (j>0);
					if(DEBUG) if(logged) log.info("Node " + this.myID + " successfully logged the " +
							"last "+ (i+1)+" messages in "+ (System.currentTimeMillis()-t1)+ " ms");
					t1=System.currentTimeMillis();
				}
			} catch(SQLException sqle) {
				sqle.printStackTrace();
				log.severe("Node "+myID+" incurred SQLException while logging:"+packets[i]);
				cleanup(pstmt);cleanup(conn);conn=null; // refresh connection
				continue;
			} 
		}
		cleanup(pstmt);cleanup(conn);
		return logged;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		DerbyPaxosLogger logger = new DerbyPaxosLogger(23,null,null);
		//DerbyPaxosLogger logger1 = new DerbyPaxosLogger(23,null,null);
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
		int size = 1024;//(int)(0.001*million);

		double avg_write_time = logger.createCheckpoints(size);
		System.out.println("Average time to write " + size + " checkpoints = " + df.format(avg_write_time));
		double avg_read_time = logger.readCheckpoints(size);
		System.out.println("Average time to read " + size + " checkpoints = " + df.format(avg_read_time));

		try {
			int numPackets =65536;
			System.out.println("\nStarting " + numPackets + " message logs: ");

			PaxosPacket[] packets = new PaxosPacket[numPackets];
			long time =0;
			int i=0;
			int reqClientID=25; 
			String reqValue = "26";
			int nodeID = coordinator;
			int k=1;
			for(int j=0; j<packets.length; j++) {
				RequestPacket req = new RequestPacket(reqClientID,  0, reqValue, false);
				ProposalPacket prop = new ProposalPacket(i, req);
				PValuePacket pvalue = new PValuePacket(ballot, prop);
				AcceptPacket accept = new AcceptPacket(nodeID, pvalue, -1);
				pvalue = pvalue.makeDecision(-1);
				PreparePacket prepare = new PreparePacket(coordinator, nodeID, new Ballot(i, ballot.coordinatorID));
				if(j%3==0) { // prepare
					packets[j] = prepare;
				} else if(j%3==1) { // accept
					accept.setCreateTime(0);
					packets[j] = accept;
				} else if(j%3==2) { // decision
					pvalue.setCreateTime(0);
					packets[j] = pvalue; 
				}
				if(j%3==2) i++;
				packets[j].putPaxosID(paxosID, (short)0);
				long t1 = System.currentTimeMillis();
				///////////////////////////////////////////logger.log(packets[j]);
				long t2 = System.currentTimeMillis();
				time += t2-t1;
				if(j>0 && (j%k==0 || j%million==0)) {System.out.println("[" + j+" : " + df.format(((double)time)/j) + "ms] "); k*=2;}
			}

			long t1=System.currentTimeMillis();
			logger.logBatch(packets);
			System.out.println("Average log time = " + Util.df((System.currentTimeMillis() - t1)*1.0/packets.length));
			System.exit(1);

			System.out.print("Checking logged messages...");
			for(int j=0; j<packets.length;j++) {
				if(AUTO_COMMIT) assert(logger.isLogged(packets[j])) : packets[j];
			}
			System.out.println("checked");

			int newSlot = 2;
			Ballot newBallot = new Ballot(0,2);
			logger.putCheckpointState(paxosID, (short)0, group, 2, newBallot, "Hello World",0);
			System.out.println("Printing logger state after checkpointing:");
			logger.initiateReadCheckpoints(true);
			RecoveryInfo pri=null;
			while((pri = logger.readNextCheckpoint(true))!=null) {
				assert(pri!=null); 
			}
			System.out.print("Checking deletion of logged messages...");
			for(int j=0; j<packets.length;j++) {
				int[] sbc = AbstractPaxosLogger.getSlotBallot(packets[j]);
				if(!((sbc[0] <= newSlot && sbc[0]>=0) || ((sbc[1]<newBallot.ballotNumber || 
						(sbc[1]==newBallot.ballotNumber && sbc[2]<newBallot.coordinatorID) )))) {
					assert(logger.isLogged(packets[j]));
				}
			}
			System.out.println("checked");
			logger.closeGracefully();
			System.out.println("SUCCESS: No exceptions or assertion violations were triggered. " +
					"Average log time over " + numPackets + " packets = " + ((double)time)/numPackets+" ms");
			System.exit(1);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
