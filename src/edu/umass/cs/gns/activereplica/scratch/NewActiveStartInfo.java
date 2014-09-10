package edu.umass.cs.gns.activereplica.scratch;

import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;
import edu.umass.cs.gns.nsdesign.packet.NewActiveSetStartupPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.WaitforUtility;
import edu.umass.cs.gns.util.Util;

/**
 * @author V. Arun
 * Based on code created by abhigyan on 3/28/14.
 * 
 * FIXME: Arun: Unclear what the point of this class is. Needs to be
 * documented. It seems to be waiting for a majority of responses 
 * from (old or new?) active replicas. Should use WaitforUtility in
 * multipaxos.
 */
public class NewActiveStartInfo {

	public final NewActiveSetStartupPacket originalPacket;
	private final WaitforUtility waitfor;
	boolean sent = false;

	public NewActiveStartInfo(NewActiveSetStartupPacket originalPacket) {
		this.originalPacket = originalPacket;
		this.waitfor = new WaitforUtility(Util.setToNodeIdArray(originalPacket.getNewActiveNameServers()));
	}

	public synchronized boolean receivedResponseFromActive(NodeId<String> ID) {
		return this.waitfor.updateHeardFrom(ID);
	}

	public synchronized boolean haveMajorityActivesResponded() {
		if (sent == false && waitfor.heardFromMajority()) {
			sent = true;
			return true;
		}
		return false;
	}
}
