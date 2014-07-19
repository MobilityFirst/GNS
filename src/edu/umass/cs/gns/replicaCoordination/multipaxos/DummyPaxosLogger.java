package edu.umass.cs.gns.replicaCoordination.multipaxos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import org.json.JSONException;

import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.AcceptPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.PValuePacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.PaxosPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.RequestPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.StatePacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.PaxosPacket.PaxosPacketType;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.Ballot;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.HotRestoreInfo;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.Messenger;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.RecoveryInfo;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.SlotBallotState;

/**
 * @author V. Arun
 */

/* This quick-and-dirty logger is a memory-based logger, so it is fake. 
 * Not only is it fake, but it scales poorly as it uses up much more memory 
 * than the paxos instances themselves, so it will limit the number of instances
 * per machine. This class is no longer maintained but is lying around in 
 * case we want to do some performance comparison of memory vs. disk operations.
 * 
 * Use DerbyPaxosLogger that extends this class for a more scalable, efficient, 
 * and persistent logger.
 * 
 * Testing: Only shows that this logger barely scales to a few hundred 
 * thousand paxos instances.
 */
public class DummyPaxosLogger extends AbstractPaxosLogger {
	public static final boolean DEBUG=PaxosManager.DEBUG;

	// The two maps below are needed only for the fake, memory-based logger
	private ConcurrentHashMap<String,PaxosCheckpoint> checkpoints=null; // most recent checkpoint
	private ConcurrentHashMap<String,ArrayList<PaxosPacket>> messages=null; // messages to be replayed since checkpoint

	// Convenience structure used only by the fake, memory-based logger
	private class PaxosCheckpoint {
		protected final short version;
		protected final int[] group;
		protected final Ballot ballot;
		protected final int slot;
		protected final String state;
		PaxosCheckpoint(short ver, int[] g, Ballot b, int s, String cp) {
			this.version = ver;
			this.group = g;
			this.ballot=b;
			this.slot=s;
			this.state=cp;
		}
	}

	private static Logger log = Logger.getLogger(DummyPaxosLogger.class.getName()); // GNS.getLogger();

	DummyPaxosLogger(int id, String logDir, Messenger messenger) {
		super(id, logDir, messenger);
		checkpoints = new ConcurrentHashMap<String,PaxosCheckpoint>();
		messages = new ConcurrentHashMap<String,ArrayList<PaxosPacket>>();
	}

	/************* Start of non-extensible methods **********************/
	public static final void logAndExecute(DummyPaxosLogger logger, PValuePacket decision, PaxosInstanceStateMachine pism) {
		assert(false) : "Method not implemented";
	}
	/************* End of non-extensible methods **********************/


	/************* Start of extensible methods ************************
	 * Not all of these are implemented for the fake logger as it 
	 * got tiresome to do it. This fake logger is needed only for
	 * performance testing against a persistent logger.
	 */
	public ArrayList<RecoveryInfo> getAllPaxosInstances() {
		ArrayList<RecoveryInfo> allPaxosInstances = new ArrayList<RecoveryInfo>();
		for(String paxosID : this.checkpoints.keySet()) {
			allPaxosInstances.add(new RecoveryInfo(paxosID, this.checkpoints.get(paxosID).version, this.checkpoints.get(paxosID).group));
		}
		return allPaxosInstances; 
	}
	public Ballot getCheckpointBallot(String paxosID) {
		PaxosCheckpoint checkpoint = this.checkpoints.get(paxosID);
		if(checkpoint!=null) return checkpoint.ballot;
		return null;
	}
	public int getCheckpointSlot(String paxosID) {
		PaxosCheckpoint checkpoint = this.checkpoints.get(paxosID);
		if(checkpoint!=null) return checkpoint.slot;
		return 0;
	}
	public SlotBallotState getSlotBallotState(String paxosID) {
		PaxosCheckpoint checkpoint = this.checkpoints.get(paxosID);
		SlotBallotState sb = new SlotBallotState(checkpoint.slot, checkpoint.ballot.ballotNumber, checkpoint.ballot.coordinatorID);
		return sb;
	}
	public SlotBallotState getSlotBallotState(String paxosID, short version, boolean versionMatch) {
		return getSlotBallotState(paxosID);
	}
	public String getCheckpointState(String paxosID) {
		PaxosCheckpoint checkpoint = this.checkpoints.get(paxosID);
		if(checkpoint!=null) return checkpoint.state;
		return null;
	}
	/* gcSlot determines which accept messages can be dropped even if they are older than the checkpoint.
	 */
	public void putCheckpointState(String paxosID, short version, int[] group, int slot, Ballot ballot, String state, int gcSlot) {
		PaxosCheckpoint checkpoint = new PaxosCheckpoint(version, group, ballot, slot, state);
		this.checkpoints.put(paxosID, checkpoint);
	}
	public StatePacket getStatePacket(String paxosID) {
		int slot = getCheckpointSlot(paxosID);
		Ballot ballot= getCheckpointBallot(paxosID);
		String state = getCheckpointState(paxosID);
		StatePacket statePacket = new StatePacket(ballot, slot, state);
		return statePacket;
	}
	/* Returns messages logged since checkpoint. We need either
	 * this method or the above rollForward method, but not both.
	 */
	public ArrayList<PaxosPacket> getLoggedMessages(String paxosID) {
		return this.messages.get(paxosID);
	}
	public Map<Integer,PValuePacket> getLoggedAccepts(String paxosID, int firstSlot) {
		ArrayList<PaxosPacket> list = this.messages.get(paxosID);
		for(Iterator<PaxosPacket> packetIterator = list.iterator(); packetIterator.hasNext();) {
			if(packetIterator.next().getType()!=PaxosPacket.PaxosPacketType.ACCEPT) {packetIterator.remove();}
		}
		HashMap<Integer,PValuePacket> accepted = new HashMap<Integer,PValuePacket>();
			for(PaxosPacket p : list) {
				PValuePacket pvalue = ((AcceptPacket)p);
				int slot = pvalue.slot;
				if(!accepted.containsKey(slot) || accepted.get(slot).ballot.compareTo(pvalue.ballot) < 0) // pruning
					accepted.put(pvalue.slot, pvalue);
			}
		for(Iterator<Integer> slotIterator = accepted.keySet().iterator(); slotIterator.hasNext();) {
			int slot = slotIterator.next();
			if(slot < firstSlot) slotIterator.remove();
		}
		return accepted;
	}
	public ArrayList<PValuePacket> getLoggedDecisions(String paxosID, int minSlot, int maxSlot) throws JSONException{
		ArrayList<PaxosPacket> list = this.messages.get(paxosID);
		for(Iterator<PaxosPacket> packetIterator = list.iterator(); packetIterator.hasNext();) {
			if(packetIterator.next().getType()!=PaxosPacket.PaxosPacketType.DECISION) {packetIterator.remove();}
		}
		ArrayList<PValuePacket> decisions = new ArrayList<PValuePacket>();
		for(PaxosPacket p : list) decisions.add(new PValuePacket(p.toJSONObject()));
		for(Iterator<PValuePacket> pvalueIterator = decisions.iterator(); pvalueIterator.hasNext();) {
			int slot = pvalueIterator.next().slot;
			if(slot > maxSlot || slot < minSlot) pvalueIterator.remove();
		}
		return decisions;
	}
	/* Removes all state for the paxosID */
	public boolean remove(String paxosID) {
		this.checkpoints.remove(paxosID);
		this.messages.remove(paxosID);
		return true;
	}
	/* Logs a paxos packet. Used by logAndMessage and can
	 * also be invoked independently.
	 */
	public boolean log(PaxosPacket packet) {
		boolean logged=false;
		String paxosID = packet.getPaxosID();
		if(paxosID!=null) {
			ArrayList<PaxosPacket> msgs = this.messages.get(paxosID);
			if(msgs==null) msgs = new ArrayList<PaxosPacket>();
			msgs.add(packet);
			this.messages.put(paxosID, msgs);
			logged = true;
		} else {
			log.severe("Received a log message with no paxosID");
		}
		return logged;
	}
	public boolean log(String paxosID, short version, int slot, int ballotnum, int coordinator, PaxosPacketType type, String message) {
		assert(false); // should never be invoked
		return false;
	}
	protected synchronized boolean initiateReadCheckpoints(boolean b) {
		assert(false) : "Method not implemented";
		return false;
	}
	protected synchronized RecoveryInfo readNextCheckpoint(boolean b) {
		assert(false) : "Method not implemented";
		return null;
	}
	protected boolean initiateReadMessages() {
		assert(false) : "Method not implemented";
		return false;		
	}
	protected PaxosPacket readNextMessage() {
		assert(false) : "Method not implemented";
		return null;
	}
	protected synchronized void closeReadAll() {
		assert(false) : "Method not implemented";
	}
	public synchronized boolean removeAll() {
		assert(false) : "Method not implemented";
		return false;
	}
	protected synchronized RecoveryInfo getRecoveryInfo(String paxosID) {
		assert(false) : "Method not implemented";
		return null;
	}
	public boolean pause(String paxosID, String serializedState) {
		assert(false) : "Method not implemented";
		return true;
	}
	public HotRestoreInfo unpause(String paxosID) {
		assert(false) : "Method not implemented";
		return null;
	}
	public void close() {
		assert(false) : "Method not implemented";
	}
	public void waitToFinish() {
		assert(false) : "Method not implemented";		
	}


	/**************** End of extensible methods ***********************/

	public static void main(String[] args) {
		int million = 1000000;
		int nNodes = (int)(million*0.7);
		int numPackets = 10;
		DummyPaxosLogger[] loggers = new DummyPaxosLogger[nNodes];
		for(int i=0; i<nNodes;i++) {
			int[] group = {i, i+1, i+23, i+44};
			Ballot ballot = new Ballot(i,i);
			int slot = i;
			String state = "state"+i;
			loggers[i] = new DummyPaxosLogger(i,null,null);
			PaxosPacket[] packets = new PaxosPacket[numPackets];
			String paxosID = "paxos"+i;
			for(int j=0; j<packets.length; j++) {
				packets[j] = new RequestPacket(25,  "26", false);
				packets[j].putPaxosID(paxosID, (short)0);
				loggers[i].log(packets[j]);
			}
			loggers[i].putCheckpointState(paxosID, (short)0, group, slot, ballot, state, 0);
		}
	}

	@Override
	public boolean logBatch(PaxosPacket[] packets) {
		assert(false) : "Method not implemented";
		return false;
	}
}