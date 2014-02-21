package edu.umass.cs.gns.replicaCoordination;

import org.json.JSONObject;

import edu.umass.cs.gns.packet.BasicPacket;

/**
@author V. Arun
 */
/* Work in progress. Inactive code.
 */
public interface ActiveReplicaCoordinator {
	/* handleRequest will look into the request type, e.g., 
	 * read, write, add, etc. and decide what kind of replica
	 * coordination is needed to implement it.
	 */
	public int handleRequest(JSONObject request);
	public int handleRequest(BasicPacket bp); // is anything non-JSON?
}
