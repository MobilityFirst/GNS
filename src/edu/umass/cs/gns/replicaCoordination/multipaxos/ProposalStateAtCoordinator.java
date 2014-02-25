package edu.umass.cs.gns.replicaCoordination.multipaxos;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.PValuePacket;

/**
@author V. Arun
 */
public class ProposalStateAtCoordinator {

	protected final PValuePacket pValuePacket;
	protected WaitForUtility waitfor=null;

	public ProposalStateAtCoordinator(int[] members, PValuePacket pValuePacket) {
		this.pValuePacket = pValuePacket;
		this.waitfor = new WaitForUtility(members);
	}
}
