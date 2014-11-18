package edu.umass.cs.gns.gigapaxos;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;



import edu.umass.cs.gns.gigapaxos.multipaxospacket.AcceptPacket;
import edu.umass.cs.gns.gigapaxos.multipaxospacket.PValuePacket;
import edu.umass.cs.gns.gigapaxos.multipaxospacket.PreparePacket;
import edu.umass.cs.gns.gigapaxos.multipaxospacket.PrepareReplyPacket;
import edu.umass.cs.gns.gigapaxos.multipaxospacket.ProposalPacket;
import edu.umass.cs.gns.gigapaxos.multipaxospacket.RequestPacket;
import edu.umass.cs.gns.gigapaxos.paxosutil.Ballot;
import edu.umass.cs.gns.gigapaxos.paxosutil.HotRestoreInfo;
import edu.umass.cs.gns.util.MultiArrayMap;
import edu.umass.cs.gns.util.NullIfEmptyMap;

/**
@author V. Arun
 */

/* This class is a paxos acceptor and manages all acceptor state.
 * It has no public methods, only protected methods, as it is 
 * expected to be used *only* by PaxosInstanceStateMachine.
 * 
 * Every PaxosInstanceStateMachine has a PaxosAcceptorState 
 * object, whether or not the paxos group is processing any
 * requests. This object contributes to about 90B of space.
 */
public class PaxosAcceptor {
	/* It suffices to maintain accept logs only on disk, so that we don't
	 * have to maintain them in memory. We have to log accepts on disk anyway. 
	 * We might as well serve them from the disk as well upon a coordinator 
	 * change, which should be infrequent and result only in bulk reads from 
	 * the database. The memory savings will be rather significant given how
	 * space-inefficient hashmaps are. With on-disk accept logs, the main
	 * space eater is committedRequests, which must be in memory for 
	 * efficiency and its size is determined by the average level of 
	 * out-of-order-ness.
	 * 
	 * FIXME: We currently put an accept into the in-memory map when it is
	 * received and remove it when the corresponding decision is received.
	 * We might as well not memory-log accepts *at all*. 
	 */
	public static final boolean ACCEPTED_PROPOSALS_ON_DISK=DerbyPaxosLogger.isLoggingEnabled(); 

	private int _slot=0;
	private int ballotNum=-1; // who'd have thought it takes 24 less bytes to use two ints instead of Ballot!
	private int ballotCoord=-1;
	private boolean stopped=false;
	private int minCommittedFrontierSlot=-1; // slot up to which accepted are garbage-collected

	/* The two maps below are of type NullIfEmptyMap as testing shows that
	 * storing null maps as opposed to empty maps yields an overall 
	 * reduction of at least 2x in inactive paxos instance state. Their
	 * size depends on how out-of-order decisions arrive.
	 */
	private NullIfEmptyMap<Integer, PValuePacket> acceptedProposals=new NullIfEmptyMap<Integer,PValuePacket>();
	private NullIfEmptyMap<Integer, PValuePacket> committedRequests=new NullIfEmptyMap<Integer,PValuePacket>();

	// static, so does not count towards space.
	private static Logger log = Logger.getLogger(PaxosAcceptor.class.getName()); // GNS.getLogger();	


	PaxosAcceptor(int b, int c, int s, HotRestoreInfo hri) {
		this.ballotNum = b;
		this.ballotCoord = c;
		this._slot = s;
		if(hri!=null) this.hotRestore(hri);
	}
	protected synchronized boolean hotRestore(HotRestoreInfo hri) {
		this.ballotNum = hri.accBallot.ballotNumber;
		this.ballotCoord = hri.accBallot.coordinatorID;
		this._slot = hri.accSlot;
		this.minCommittedFrontierSlot = hri.accGCSlot;
		return true;
	}

	private synchronized void stop() {this.stopped = true;}
	/* isStopped() is checked only in PaxosInstanceStateMachine.handlePaxosMessages().
	 * stop() does not deactivate this PaxosAcceptorState immediately, it only prevents new 
	 * messages from being processed.
	 */
	protected synchronized boolean isStopped() {return this.stopped;}
	/* Can be called forcibly by PaxosInstanceStateMachine. Otherwise,
	 * the private method stop() is only called after a stop is executed.
	 */
	protected synchronized void forceStop() {stop();}

	protected synchronized int getGCSlot() {return this.minCommittedFrontierSlot;}
	protected synchronized int getSlot() {return _slot;}
	protected synchronized Ballot getBallot() {return new Ballot(ballotNum, ballotCoord);}
	protected synchronized String getBallotStr() {return Ballot.getBallotString(ballotNum, ballotCoord);}
	protected synchronized String getBallotSlot() {return getBallot()+", "+getSlot();}
	protected synchronized int getBallotNum() {return this.ballotNum;}
	protected synchronized int getBallotCoord() {return this.ballotCoord;}

	/* Note: We don't have public putSlot and putBallot methods as external 
	 * entities have no business modifying paxos state directly. 
	 */

	/* Phase1a
	 * Event: Acceptor receives a new ballot from a would-be coordinator.
	 * Action: Acceptor accepts the ballot if it is strictly greater than the 
	 * current ballot and if so, updates its current ballot.
	 * 
	 * Return: (1) The set of proposals this acceptor previously accepted but for
	 * which it has not yet received commits. These proposals would need to
	 * be carefully moved by the would-be coordinator to its proposed ballot.
	 * (2) The (possibly updated) current ballot.
	 */
	protected synchronized PrepareReplyPacket handlePrepare(PreparePacket prepare, String debug) {
		if(this.isStopped()) return null;

		PrepareReplyPacket preply = null;
		if(prepare.ballot.compareTo(new Ballot(ballotNum,ballotCoord)) > 0) {
			log.info(debug + " acceptor " + prepare.receiverID + " updating to higher ballot " + prepare.ballot);
			this.ballotNum = prepare.ballot.ballotNumber; this.ballotCoord = prepare.ballot.coordinatorID;
		}
		/* Why return accepted values even though they were proposed in lower 
		 * ballots? So that the preparer knows the set of accepted values to
		 * carry over across a view change.
		 */
		preply = new PrepareReplyPacket(prepare.coordinatorID, prepare.receiverID, this.getBallot(), 
				pruneAcceptedProposals(this.acceptedProposals.getMap(), prepare.firstUndecidedSlot)
				);
		return preply;
	}
	
	// prunes accepted pvalues below those requested by coordinator
	private synchronized Map<Integer,PValuePacket> pruneAcceptedProposals(Map<Integer,PValuePacket> acceptedMap, int minSlot) {
		Iterator<Integer> slotIterator = acceptedMap.keySet().iterator();
		while(slotIterator.hasNext()) {
			if(slotIterator.next() < minSlot) slotIterator.remove();
		}
		return acceptedMap;
	}

	/* Phase2a
	 * Event: Acceptor receives a proposal from a coordinator. 
	 * Action: Accept if the proposal belongs to a ballot at least as high
	 * as the current ballot. If higher, update current ballot.
	 * 
	 * Return: updated current ballot.
	 */
	protected synchronized Ballot acceptAndUpdateBallot(AcceptPacket accept) {
		if(this.isStopped()) return null;

		if (accept.ballot.compareTo(new Ballot(ballotNum,ballotCoord)) >= 0) {  // accept the pvalue and the ballot
			this.ballotNum = accept.ballot.ballotNumber; this.ballotCoord=accept.ballot.coordinatorID; // no-op if the two are equal anyway
			if(accept.slot > this.minCommittedFrontierSlot) this.acceptedProposals.put(accept.slot, accept);
		}
		garbageCollectAccepted(accept.majorityCommittedSlot);
		return new Ballot(ballotNum,ballotCoord);
	}

	/* Phase 3: execute if next-in-line commit, else enqueue */
	protected synchronized PValuePacket putAndRemoveNextExecutable(PValuePacket decision) {
		if(this.isStopped()) return null;
		if(decision==null && (decision=this.committedRequests.get(this.getSlot()))==null) return null;

		this.garbageCollectAccepted(decision.getMajorityCommittedSlot());
		if(ACCEPTED_PROPOSALS_ON_DISK) this.acceptedProposals.remove(decision.slot); // corresponding accept must be disk-logged at a majority 

		if(decision.slot>=this.getSlot()) this.committedRequests.put(decision.slot, decision); 
		PValuePacket nextExecutable=null;
		if (this.committedRequests.containsKey(this.getSlot())) { // might be removing what got inserted just above
			nextExecutable = this.committedRequests.remove(this.getSlot());
			this.executed(nextExecutable.slot, nextExecutable.isStopRequest());
		}
		// Note: by design, assertSlotInvariant() may not hold right here.
		return nextExecutable;
	}
	protected synchronized void assertSlotInvariant() {assert(!this.committedRequests.containsKey(this.getSlot()));}

	/* We are done! Only phase1a, phase2a, phase3 messages are received by acceptors. 
	 * The corresponding 1b and 2b messages are received by a coordinator 
	 * and are processed in PaxosCoordinatorState. The corresponding sends are 
	 * invoked by the paxos instance using the return values of the methods above. 
	 * 
	 * The methods below are utility methods invoked by PaxosInstanceStateMachine. 
	 * They touch this paxos instance's state, so they are synchronized.
	 */

	protected synchronized ArrayList<Integer> getMissingCommittedSlots(int sizeLimit) {
		if(this.isStopped()) return null;

		if(this.committedRequests.isEmpty()) return null;
		ArrayList<Integer> missing=new ArrayList<Integer>();
		int maxCommittedSlot = getMaxCommittedSlot();
		for(int i=this.getSlot(); i < Math.min(maxCommittedSlot, this.getSlot()+sizeLimit); i++) missing.add(i);
		return missing; // in sorted order
	}
	protected synchronized int getMaxCommittedSlot() {
		if(this.isStopped() || this.committedRequests.isEmpty()) return this.getSlot()-1;

		int maxSlot=this.getSlot()-1;
		for(int i : this.committedRequests.keySet()) {
			maxSlot = Math.max(i, maxSlot);
		}
		return maxSlot;
	}
	protected synchronized boolean caughtUp() {
		return this.acceptedProposals.isEmpty() && this.committedRequests.isEmpty();
	}

	/********************** Start of private methods ***************************/
	private synchronized void executed(int s, boolean stop) {
		if(s==this.getSlot()) {
			this._slot++; 
			if(stop) {this.stop();}
		} else assert false : ("YIKES! Asked to execute " + s + " when expecting " + this.getSlot());
	}
	private synchronized void garbageCollectAccepted(int gcSlot) {
		if(gcSlot > this.minCommittedFrontierSlot) {
			this.minCommittedFrontierSlot = gcSlot;
			Iterator<Integer> slotIterator = this.acceptedProposals.keySet().iterator();
			while(slotIterator.hasNext()) {
				if((Integer)slotIterator.next() <= gcSlot) slotIterator.remove();
			}
		}
	}
	/*********************** End of private methods *****************/


	/***************** Start of testing methods *******************************/
	public String toString() {
		return "{Acceptor: [slot=" + this.getSlot() + ", ballot="+this.ballotNum+":"+this.ballotCoord +
				", isStopped="+this.isStopped()+", |accepted|="+this.acceptedProposals.size() + ", |committed|=" + 
				this.committedRequests.size() + ", committedFrontier="+ this.minCommittedFrontierSlot + 
				this.getAccepted() + this.getCommitted() + "]}";
	}
	private String getAccepted() {
		String s=" accepted=[";
		for(PValuePacket pvalue : this.acceptedProposals.values()) {
			s += pvalue.slot + " ";
		}
		return s+"] ";
	}
	private String getCommitted() {
		String s=" committed=[";
		for(PValuePacket pvalue : this.committedRequests.values()) {
			s += pvalue.slot + " ";
		}
		return s+"] ";
	}

	protected void testingInitInstance(int load) {
		this.acceptedProposals = new NullIfEmptyMap<Integer,PValuePacket>();
		this.committedRequests = new NullIfEmptyMap<Integer,PValuePacket>();
		for(int i=0; i<load; i++) {
			this.acceptedProposals.put(25+i, new PValuePacket(new Ballot(ballotNum,ballotCoord), 
					new ProposalPacket(45+i,
							new RequestPacket(34+i, "hello39"+i,false))
					)
					);
			RequestPacket req = new RequestPacket(71+i, "hello41"+i,false);
			this.committedRequests.put(43+i, new PValuePacket(new Ballot(0,0), new ProposalPacket(43+i,req)));
		}
	}
	protected synchronized void jumpSlot(int slotNumber) {
		assert(slotNumber>getSlot()); // otherwise we should never have gotten here
		// doing it this way just to ensure executed(.) is the only way to increment slot
		for(int i=getSlot(); i<slotNumber; i++) {
			this.executed(i, false);
			this.committedRequests.remove(i);
			this.acceptedProposals.remove(i); // have to be logged on disk for correctness
		}
		assert(slotNumber==getSlot()); 
	}


	private static int testingCreateAcceptor(int size, int id, Set<Integer> group, String testMode) {
		PaxosInstanceStateMachine[] pismarray = null;
		MultiArrayMap<String, PaxosInstanceStateMachine> pismMap = new MultiArrayMap<String, PaxosInstanceStateMachine>(size);
		PaxosAcceptor[] pasarray = null;
		int j=1;
		System.out.print("Number of created instances: ");
		int million=1000000;
		String ID = "something";
		int coord = (Integer)(group.toArray()[0]);
		for(int i=0; i<size; i++) {
			try {
				if(testMode.equals("FULL_INSTANCE")) {
					if(pismarray==null) pismarray = new PaxosInstanceStateMachine[size];
					pismarray[i] = new PaxosInstanceStateMachine(ID+i, (short)i, (i%3==0 ? coord : id), group, null, null, null); 
					pismMap.put(pismarray[i].getKey(), pismarray[i]);
					pismarray[i].testingInit(0);
				} 
				else if(testMode.equals("ACCEPTOR")) {
					if(pasarray==null) pasarray = new PaxosAcceptor[size];
					pasarray[i] = new PaxosAcceptor(1, 2,0,null);
					pasarray[i].testingInitInstance(0);
				}
				if(i%j==0 || (i+1)%million==0) {System.out.print(((i+1)>=million?(i+1)/million+"M":i)+" ");j*=2;}
				else if(i>2*million && i%100000==0) {System.out.println(i);}
			} catch(Exception e) {
				e.printStackTrace();
				System.out.println("Successfully created " + ((i/100000)/10.0) + 
						" million *inactive* " + (testMode.equals("FULL_INSTANCE")?"full":"acceptor") +  
						" paxos instances before running out of 1GB memory.");
				return i;
			}
		}
		return size;
	}
	

	/**
	 * @param args
	 */
	public static void testMemory() {
		Handler[] handlers = Logger.getLogger( "" ).getHandlers();
		for ( int index = 0; index < handlers.length; index++ ) {
			handlers[index].setLevel( Level.WARNING );
		}

		int million=1000000;
		int size=(int)(6*million); // 11.5M for acceptors, and 4M for full instance (with GC kicking in at around 3M)
		TreeSet<Integer> group = new TreeSet<Integer>();
		group.add(23);

		try {
			System.out.print("Verifying that JVM size is set to 1GB...");
			RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
			List<String> arguments = runtimeMxBean.getInputArguments();
			for (String arg : arguments) {
				int vmsize=0;
				if(arg.matches("-Xms[0-9]*M")) {
					try {
						vmsize = new Integer(arg.replaceAll("-Xms", "").replaceAll("M", ""));
					} finally {
						if(vmsize!=1024) System.out.println("\nPlease ensure that the VM size is set to 1024M using the -Xms1024M option.");
					}
				}
			}
			System.out.println("verified. \nTesting in progress...");

			Thread.sleep(2000);
			int numCreated = testingCreateAcceptor(size, 24, group, "FULL_INSTANCE");
			System.out.println("\nSuccessfully created " + ((numCreated/100000)/10.0) + 
					" million *inactive* acceptor paxos instances with 1GB memory.");
		} catch(Exception e) {e.printStackTrace();}
	}
	
	private void testAcceptor() {
		int ballotnum = 22;
		int ballotCoord = 1;
		int slot = 7;
		PaxosAcceptor acceptor = new PaxosAcceptor(ballotnum, ballotCoord, slot, null);
	}

	public static void main(String[] args) {
		testMemory();
	}
}
