package edu.umass.cs.gns.replicaCoordination.multipaxos;

import edu.umass.cs.gns.packet.paxospacket.FailureDetectionPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.RequestPacket;

/**
@author V. Arun
 */
public class DefaultPaxosInterfaceApp implements PaxosInterface {
	String myState = new String("Initial state");

	@Override
	public void handlePaxosDecision(String paxosID,
			RequestPacket requestPacket, boolean recovery) {
		System.out.println("PaxosID " + paxosID + " executing request " + requestPacket.requestID + " by changing state to " + requestPacket.value);
		myState = requestPacket.value;
	}

	@Override
	public void handleFailureMessage(FailureDetectionPacket fdPacket) {
		System.out.println("Ignoring notification about failure of node " + fdPacket.responderNodeID);
	}

	@Override
	public String getState(String paxosID) {
		return myState;
	}

	@Override
	public void updateState(String paxosID, String state) {
		myState = state;
	}

	@Override
	public String getPaxosKeyForPaxosID(String paxosID) {
		assert(false) : "Method not implemented";
		return null;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
