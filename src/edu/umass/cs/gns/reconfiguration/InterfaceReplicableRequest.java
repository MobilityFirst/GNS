package edu.umass.cs.gns.reconfiguration;

import edu.umass.cs.gns.gigapaxos.InterfaceRequest;


/**
@author V. Arun
 */
public interface InterfaceReplicableRequest extends InterfaceRequest {
	public boolean needsCoordination();
	public void setNeedsCoordination(boolean b);
}
