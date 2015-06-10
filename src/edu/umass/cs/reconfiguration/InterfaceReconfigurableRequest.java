package edu.umass.cs.reconfiguration;

import edu.umass.cs.gigapaxos.InterfaceRequest;

/**
 * @author V. Arun
 *         <p>
 *         A reconfigurable request can also be a stop request and comes with an
 *         epoch number. The application must be aware of both of these.
 */
public interface InterfaceReconfigurableRequest extends InterfaceRequest {
	/**
	 * @return The epoch number.
	 */
	public int getEpochNumber();

	/**
	 * @return True if this request is a stop request.
	 */
	public boolean isStop();
}
