package edu.umass.cs.gns.reconfiguration;

/**
@author V. Arun
 */

/* The method executed(.) in this interface is called only by AbstractReplicaCoordinator
 * that is an internal class. This interface is not (yet) expected to be implemented
 * by a third-party class like an instance of Application.
 */
public interface InterfaceReconfiguratorCallback {
	public void executed(InterfaceStopRequest request, boolean handled);
}
