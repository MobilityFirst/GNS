package edu.umass.cs.reconfiguration;

import edu.umass.cs.gigapaxos.InterfaceRequest;


/**
@author V. Arun
 */
public interface InterfaceReplicableRequest extends InterfaceRequest {
	/**
	 * @return True means this request needs to be coordinated.
	 */
	public boolean needsCoordination();
	/**
	 * @param b
	 */
	public void setNeedsCoordination(boolean b);
}
