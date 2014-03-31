package edu.umass.cs.gns.replicaCoordination;

import edu.umass.cs.gns.nio.GNSNIOTransport;
import edu.umass.cs.gns.nio.NodeConfig;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurable;
import edu.umass.cs.gns.packet.BasicPacket;
import edu.umass.cs.gns.paxos.PaxosManager;
import org.json.JSONObject;

/**
@author V. Arun
 */
// fixme delete this file
/* Work in progress. Inactive code.
 */
public class ActiveReplicaPaxos {

	PaxosManager paxosManager=null;
	NodeConfig nodeConfig=null;
	GNSNIOTransport nioTransport=null;
  GnsReconfigurable activeReplica=null;
	
	public ActiveReplicaPaxos(GNSNIOTransport niot, NodeConfig nc, GnsReconfigurable activeReplica) {
		this.nioTransport = niot;
		this.nodeConfig = nc;
    this.activeReplica = activeReplica;
	}
	

	public int coordinateRequest(JSONObject request) {
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
