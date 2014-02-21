package edu.umass.cs.gns.replicaCoordination;

import org.json.JSONObject;

import edu.umass.cs.gns.packet.BasicPacket;

/**
@author V. Arun
 */
/* Work in progress. Inactive code.
 */
public interface ReplicaControllerCoordinator {
	public int handleRequest(JSONObject request);
	public int handleRequest(BasicPacket bp);
	public int initGroupChange(String name);
}
