package edu.umass.cs.gns.replicaCoordination;

import org.json.JSONObject;

import edu.umass.cs.gns.nio.GNSNIOTransport;
import edu.umass.cs.gns.nio.NodeConfig;
import edu.umass.cs.gns.packet.BasicPacket;
import edu.umass.cs.gns.replicaCoordination.ActiveReplicaCoordinator;
import edu.umass.cs.gns.paxos.PaxosManager;

/**
@author V. Arun
 */

/* Work in progress. Inactive code.
 */
public class ActiveReplicaPaxos implements ActiveReplicaCoordinator {

	PaxosManager paxosManager=null;
	NodeConfig nodeConfig=null;
	GNSNIOTransport nioTransport=null;
	
	public ActiveReplicaPaxos(GNSNIOTransport niot, NodeConfig nc) {
		this.nioTransport = niot;
		this.nodeConfig = nc; 
	}
	
	@Override
	public int handleRequest(JSONObject request) {
		// TODO Auto-generated method stub
		return 0;
	}
	public int handleRequest(BasicPacket bp) {
		return 0;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
