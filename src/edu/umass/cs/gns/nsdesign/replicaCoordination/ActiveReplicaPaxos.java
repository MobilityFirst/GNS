package edu.umass.cs.gns.nsdesign.replicaCoordination;

import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurable;
import edu.umass.cs.gns.newApp.packet.BasicPacket;
import edu.umass.cs.gns.paxos.PaxosManager;
import edu.umass.cs.nio.InterfaceNodeConfig;
import edu.umass.cs.nio.JSONNIOTransport;

import org.json.JSONObject;

/**
@author V. Arun
 */
// fixme delete this file
/* Work in progress. Inactive code.
 */
@Deprecated
public class ActiveReplicaPaxos {

	PaxosManager paxosManager=null;
	InterfaceNodeConfig nodeConfig=null;
	JSONNIOTransport nioTransport=null;
  GnsReconfigurable activeReplica=null;
	
	public ActiveReplicaPaxos(JSONNIOTransport niot, InterfaceNodeConfig nc, GnsReconfigurable activeReplica) {
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
