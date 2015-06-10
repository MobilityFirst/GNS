package edu.umass.cs.reconfiguration;

import edu.umass.cs.gigapaxos.InterfaceRequest;

/**
 * @author V. Arun
 *         <p>
 *         The method executed(.) in this interface is called only by
 *         AbstractReplicaCoordinator that is an internal class. This interface
 *         is not (yet) expected to be implemented by a third-party class like
 *         an instance of Application.
 */
public interface InterfaceReconfiguratorCallback {
	/**
	 * @param request
	 * @param handled
	 */
	public void executed(InterfaceRequest request, boolean handled);
}
