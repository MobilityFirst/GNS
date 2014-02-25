package edu.umass.cs.gns.replicaCoordination.multipaxos;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;



import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.AcceptPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.PValuePacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.PaxosPacketType;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.PreparePacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.ProposalPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.RequestPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.SynchronizeReplyPacket;
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
 * requests. Largely because of this object, the minimum
 * *total* size of an inactive PaxosInstanceStateMachine
 * is <170B of which <100B comes because of this
 * PaxosAcceptorState object.
 */
public class PaxosAcceptorState {
	private int slot=0;
	private Ballot ballot=null;

	/* The two maps below are of type NullIfEmptyMap as testing shows that
	 * storing null maps as opposed to empty maps yields an overall 
	 * reduction of at least 2x in inactive paxos instance state.
	 */
	private NullIfEmptyMap<Integer, PValuePacket> acceptedProposals=new NullIfEmptyMap<Integer,PValuePacket>();
	private NullIfEmptyMap<Integer, RequestPacket> committedRequests=new NullIfEmptyMap<Integer,RequestPacket>();

	// static, so does not count towards space.
	private static Logger log = Logger.getLogger(PaxosAcceptorState.class.getName()); // GNS.getLogger();	
	
	PaxosAcceptorState(Ballot b) {
		ballot = b;
	}
	PaxosAcceptorState(PaxosAcceptorState pis) {
		slot = pis.getSlot();
		ballot = pis.getBallot();
	}
	
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
	protected synchronized PreparePacket handlePrepare(PreparePacket prepare) {
		PreparePacket prepareReply=null;
		if(prepare.ballot.compareTo(this.ballot) > 0) {
			log.info("Node " + prepare.receiverID + " updating to higher ballot " + prepare.ballot);
			this.ballot = prepare.ballot;
		}
		/* Why return accepted values even though they were proposed in lower 
		 * ballots? So that the preparer knows the set of accepted values to
		 * carry over across a view change.
		 */
		prepareReply = prepare.getPrepareReplyPacket(this.ballot, prepare.receiverID, 
				this.acceptedProposals.getMap(), this.slot);
		return prepareReply;
	}

	/* Phase2a
	 * Event: Acceptor receives a proposal from a coordinator. 
	 * Action: Accept if the proposal belongs to a ballot at least as high
	 * as the current ballot. If higher, update current ballot.
	 * 
	 * Return: updated current ballot.
	 */
	protected synchronized Ballot acceptAndUpdateBallot(AcceptPacket accept) {
		if (accept.pValue.ballot.compareTo(this.ballot) >= 0) {  // accept the pvalue and the ballot
			this.ballot = new Ballot(accept.pValue.ballot.ballotNumber, accept.pValue.ballot.coordinatorID);
			this.acceptedProposals.put(accept.pValue.proposal.slot, accept.pValue);
		} 
		return this.ballot;
	}
	
	/* We are done! Only phase1a and phase2a messages are received by an acceptor. 
	 * The corresponding 1b and 2b messages are received by a coordinator 
	 * and are processed in PaxosCoordinatorState. The corresponding sends are 
	 * invoked by the paxos instance using the return values of the methods above. 
	 * 
	 * The methods below are just convenience methods. All of them touch this 
	 * paxos instance's state, so they are synchronized to be thread-safe.
	 */
	
	/*********************** Start of self-synchronized methods *****************/
	protected synchronized boolean executeIfNextCommittedRequest(ProposalPacket proposal) {
		if(proposal.slot != this.slot) return false;
		this.executed(proposal.slot);
		return true;
	}
	protected synchronized void executed(int s) {if(s==slot) slot++;}
	protected synchronized int getSlot() {return slot;}
	protected synchronized Ballot getBallot() {return ballot;}
	/* Note: We don't have public putSlot and putBallot methods
	 * as external entities have no business modifying paxos
	 * state directl. The exception is "executed(.)" that 
	 * results in incrementing slot.
	 */

	protected synchronized ArrayList<ProposalPacket> getMissingCommits(SynchronizeReplyPacket syncReply) {
		if(syncReply==null || syncReply.missingSlotNumbers.isEmpty()) return null;

		ArrayList<ProposalPacket> missing = new ArrayList<ProposalPacket>();
		for(int i : syncReply.missingSlotNumbers) {
			missing.add(new ProposalPacket(i, this.committedRequests.get(i), 
					PaxosPacketType.PROPOSAL));
		}
		return missing;
	}

	protected synchronized void putCommittedRequest(int s, RequestPacket rp) {
		this.committedRequests.put(s, rp);
		// If a request has been committed, dequeue it from accepted
		this.acceptedProposals.remove(s);
	}
	/* Removes a request if it is the next in-order committed request */
	protected synchronized RequestPacket executeIfNextCommittedRequest() {
		RequestPacket request = this.committedRequests.remove(slot);
		if(request!=null) this.executed(this.slot);
		return request;
	}
	protected synchronized ArrayList<Integer> getMissingSlotsCommittedRequests() {
		if(this.committedRequests.isEmpty()) return null;
		ArrayList<Integer> missing=new ArrayList<Integer>();
		int maxCommittedSlot = getMaxSlotCommittedRequests();
		for(int i=this.slot; i<maxCommittedSlot; i++) missing.add(this.slot);
		return missing;
	}
	protected synchronized int getMaxSlotCommittedRequests() {
		if(this.committedRequests.isEmpty()) return this.slot-1;
		int maxSlot=this.slot;
		for(int i : this.committedRequests.keySet()) maxSlot = Math.max(i, maxSlot);
		assert(maxSlot > this.slot+1); // Otherwise we should have executed it already.
		return maxSlot;
	}

	/*********************** End of self-synchronized methods *****************/

	/***************** Start of testing methods *******************************/
	protected void testingInitInstance(int load) {
		this.acceptedProposals = new NullIfEmptyMap<Integer,PValuePacket>();
		this.committedRequests = new NullIfEmptyMap<Integer,RequestPacket>();
		for(int i=0; i<load; i++) {
			this.acceptedProposals.put(25+i, new PValuePacket(this.ballot, 
					new ProposalPacket(45+i,
							new RequestPacket(34+i, "hello39"+i, PaxosPacketType.REQUEST,false),
							PaxosPacketType.PROPOSAL)
					)
					);
			this.committedRequests.put(43+i, new RequestPacket(71+i, "hello41"+i, PaxosPacketType.REQUEST,false));
		}
	}
	
	private static int testingCreateAcceptor(int size, int id, Set<Integer> group, String testMode) {
		PaxosInstanceStateMachine[] pismarray = null;
		PaxosAcceptorState[] pasarray = null;
		int j=1;
		System.out.print("Number of created instances: ");
		int million=1000000;
		for(int i=0; i<size; i++) {
			try {
				if(testMode.equals("FULL_INSTANCE")) {
					if(pismarray==null) pismarray = new PaxosInstanceStateMachine[size];
					pismarray[i] = new PaxosInstanceStateMachine("something", id, group, null, null);
					pismarray[i].testingInit(0);
				} 
				else if(testMode.equals("ACCEPTOR")) {
					if(pasarray==null) pasarray = new PaxosAcceptorState[size];
					pasarray[i] = new PaxosAcceptorState(new Ballot(1, 2));
					pasarray[i].testingInitInstance(0);
				}
				if(i%j==0 || (i+1)%million==0) {System.out.print(((i+1)>=million?(i+1)/million+"M":i)+" ");j*=2;}
			} catch(Exception e) {
				e.printStackTrace();
				System.out.println("Successfully created " + ((i/100000)/10.0) + 
						" million *inactive* " + (group.contains(id)?"acceptor":"coordinator") +  
						" paxos instances before running out of 1GB memory.");
				return i;
			}
		}
		return size;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Handler[] handlers = Logger.getLogger( "" ).getHandlers();
		for ( int index = 0; index < handlers.length; index++ ) {
			handlers[index].setLevel( Level.WARNING );
		}

		int million=1000000;
		int size=(int)(6*million);
		TreeSet<Integer> group = new TreeSet<Integer>();
		group.add(23);

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
		
		int numCreated = testingCreateAcceptor(size, 24, group, "FULL_INSTANCE");
		System.out.println("\nSuccessfully created " + ((numCreated/100000)/10.0) + 
				" million *inactive* acceptor paxos instances with 1GB memory.");
	}
}
