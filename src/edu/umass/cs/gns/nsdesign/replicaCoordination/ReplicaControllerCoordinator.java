package edu.umass.cs.gns.nsdesign.replicaCoordination;

import edu.umass.cs.gns.util.Shutdownable;
import org.json.JSONObject;

/**
@author V. Arun
 */
/* Work in progress. Inactive code.
 */
@Deprecated
public interface ReplicaControllerCoordinator extends Shutdownable{
	public int coordinateRequest(JSONObject request); // FIXME: Why does this return an int?
	public int initGroupChange(String name); // FIXME: Arun: Why is this needed?
	public void reset(); // FIXME: Arun: Why is this part of coordination??
}
