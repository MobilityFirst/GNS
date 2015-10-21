package edu.umass.cs.reconfiguration.examples;

import edu.umass.cs.gigapaxos.InterfaceReplicable;
import edu.umass.cs.gigapaxos.InterfaceRequest;
import edu.umass.cs.reconfiguration.PaxosReplicaCoordinator;
import edu.umass.cs.reconfiguration.interfaces.InterfaceReconfigurable;
import edu.umass.cs.reconfiguration.interfaces.InterfaceReconfigurableRequest;

/**
 * @author arun
 * 
 *         This class can be extended to implement a simple reconfigurable-paxos
 *         application. It allows the application to just implement
 *         {@link InterfaceReplicable} and yet be reconfigurable provided
 *         {@link PaxosReplicaCoordinator} is used as the replica coordinator.
 * 
 * @param <NodeIDType>
 *
 */
public abstract class AbstractReconfigurablePaxosApp<NodeIDType> implements
		InterfaceReplicable, InterfaceReconfigurable {

	/**
	 * This class returns a default no-op stop request. It is probably not a
	 * good idea to rely on this as the stop will be oblivious to the app and
	 * will appear just like any no-op, so the app may not be able to do garbage
	 * collection or other app-specific activities when an epoch is stopped.
	 */
	@Override
	public InterfaceReconfigurableRequest getStopRequest(String name, int epoch) {
		return AppRequest.getObliviousPaxosStopRequest(name, epoch);
	}

	@Override
	public String getFinalState(String name, int epoch) {
		throw new RuntimeException("This method should not have been called");
	}

	@Override
	public void putInitialState(String name, int epoch, String state) {
		throw new RuntimeException("This method should not have been called");
	}

	@Override
	public boolean deleteFinalState(String name, int epoch) {
		throw new RuntimeException("This method should not have been called");
	}

	@Override
	public Integer getEpoch(String name) {
		throw new RuntimeException("This method should not have been called");
	}

	@Override
	public boolean handleRequest(InterfaceRequest request,
			boolean doNotReplyToClient) {
		throw new RuntimeException("This method should not have been called");
	}
}
