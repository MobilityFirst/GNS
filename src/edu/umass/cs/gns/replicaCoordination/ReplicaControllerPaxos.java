package edu.umass.cs.gns.replicaCoordination;

import org.json.JSONObject;

import edu.umass.cs.gns.packet.BasicPacket;

/**
@author V. Arun
 */ 

/* The purpose of this class is to abstract away all group change operations 
 * done using paxos. This is meant to subsume much of the functionality of 
 * the currently static class PaxosManager. We probably need an instantiable
 * version of PaxosManager anyway that is customized for replica controllers.
 */
public class ReplicaControllerPaxos implements ReplicaControllerCoordinator {

	@Override
	public int handleRequest(JSONObject request) {
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
