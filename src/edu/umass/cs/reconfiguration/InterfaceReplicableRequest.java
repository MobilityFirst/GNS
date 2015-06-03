package edu.umass.cs.reconfiguration;

import edu.umass.cs.gigapaxos.InterfaceRequest;


/**
@author V. Arun
 */
public interface InterfaceReplicableRequest extends InterfaceRequest {
	public boolean needsCoordination();
	public void setNeedsCoordination(boolean b);
}
