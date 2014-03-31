package edu.umass.cs.gns.replicaCoordination;

import edu.umass.cs.gns.nio.GNSNIOTransport;
import edu.umass.cs.gns.nio.NodeConfig;
import edu.umass.cs.gns.nsdesign.replicaController.ReplicaController;
import edu.umass.cs.gns.packet.BasicPacket;
import edu.umass.cs.gns.paxos.PaxosManager;
import org.json.JSONObject;

/**
@author V. Arun
 */ 
// fixme delete this file
/* The purpose of this class is to abstract away all group change operations 
 * done using paxos. This is meant to subsume much of the functionality of 
 * the currently static class PaxosManager. We probably need an instantiable
 * version of PaxosManager anyway that is customized for replica controllers.
 */
public class ReplicaControllerPaxos {


  PaxosManager paxosManager=null;
  NodeConfig nodeConfig=null;
  GNSNIOTransport nioTransport=null;
  ReplicaController replicaController=null;

  public ReplicaControllerPaxos(GNSNIOTransport niot, NodeConfig nc, ReplicaController replicaController) {
    this.nioTransport = niot;
    this.nodeConfig = nc;
    this.replicaController = replicaController;
  }


	public int coordinateRequest(JSONObject request) {
		// TODO Auto-generated method stub
		return 0;
	}
	public int handleRequest(BasicPacket bp) {
		return 0;
	}
	public int initGroupChange(String name) {
		return 0;
	}

}
