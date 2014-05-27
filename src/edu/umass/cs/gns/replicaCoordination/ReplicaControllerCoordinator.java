package edu.umass.cs.gns.replicaCoordination;

import org.json.JSONObject;

/**
@author V. Arun
 */
/* Work in progress. Inactive code.
 */
public interface ReplicaControllerCoordinator {
	public int coordinateRequest(JSONObject request);
	public int initGroupChange(String name); // FIXME: Arun: Why is this needed?
	public void reset(); // FIXME: Arun: Why is this part of coordination??
}
