package edu.umass.cs.gns.replicaCoordination;

import edu.umass.cs.gns.util.Shutdownable;
import org.json.JSONObject;

/**
@author V. Arun
 */
/* Work in progress. Inactive code.
 */
@Deprecated
public abstract class ActiveReplicaCoordinator implements Shutdownable{

	/* coordinateRequest will look into the request type, e.g.,
	 * read, write, add, etc. and decide what kind of replica
	 * coordination is needed to implement it.
	 */
	public abstract int coordinateRequest(JSONObject request);

	public abstract void reset();
}
