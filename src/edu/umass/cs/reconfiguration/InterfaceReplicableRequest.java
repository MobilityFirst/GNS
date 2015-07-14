package edu.umass.cs.reconfiguration;

import edu.umass.cs.gigapaxos.InterfaceRequest;

/**
 * @author V. Arun
 */
public interface InterfaceReplicableRequest extends InterfaceRequest {
	/**
	 * @return True if this request needs to be coordinated.
	 */
	public boolean needsCoordination();

	/**
	 * After this method returns, {@link #needsCoordination()} should
	 * subsequently return {@code b}. Note that although this method could
	 * supply a true or false argument, {@link AbstractReplicaCoordinator} will
	 * only ever invoke it with a false argument. That is, we only expect to go
	 * from true to false and never the other way round.
	 * <p>
	 * 
	 * We need application requests to explicitly support this method so that
	 * coordinated packets are not coordinated again infinitely. For example, if
	 * a replica coordinator's coordination strategy is to simply flood the
	 * request to all replicas, there needs to be a way for a recipient of a
	 * copy of this already once coordinated request to know that it should not
	 * coordinate it again. This method provides
	 * {@link AbstractReplicaCoordinator} a placeholder in the application
	 * request to prevent such infinite coordination loops.
	 * <p>
	 *
	 * FIXME: Change this method's definition to just
	 * {@code setDoesNotNeedCoordination()} without an explicit argument.
	 * 
	 * @param b
	 */
	public void setNeedsCoordination(boolean b);
}
