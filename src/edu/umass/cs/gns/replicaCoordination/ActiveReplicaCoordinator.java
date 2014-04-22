package edu.umass.cs.gns.replicaCoordination;

import org.json.JSONObject;

/**
@author V. Arun
 */
/* Work in progress. Inactive code.
 */
public abstract class ActiveReplicaCoordinator {

	/* coordinateRequest will look into the request type, e.g.,
	 * read, write, add, etc. and decide what kind of replica
	 * coordination is needed to implement it.
	 */
	public abstract int coordinateRequest(JSONObject request);

	public abstract void reset();
}
