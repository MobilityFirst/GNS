package edu.umass.cs.gns.reconfiguration;

import edu.umass.cs.gns.gigapaxos.InterfaceRequest;


/**
@author V. Arun
 */

/* The method executed(.) in this interface is called only by AbstractReplicaCoordinator
 * that is an internal class. This interface is not (yet) expected to be implemented
 * by a third-party class like an instance of Application.
 */
public interface InterfaceReconfiguratorCallback {
	public void executed(InterfaceRequest request, boolean handled);
}
