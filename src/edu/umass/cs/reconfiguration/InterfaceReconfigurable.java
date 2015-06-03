package edu.umass.cs.reconfiguration;

import edu.umass.cs.gigapaxos.InterfaceApplication;


/**
 * @author V. Arun
 */

public interface InterfaceReconfigurable extends InterfaceApplication {

	/*
	 * Returns a stop request that when executed by the app ensures that no
	 * subsequent request after the stop is executed in that epoch. We need the
	 * app to return a stop request as opposed to just exposing a stop() method
	 * because, in general, the stop may need to be coordinated across app
	 * replicas.
	 * 
	 * If nothing special has to be done by the app upon an epoch stop, it is
	 * okay for this method to just return null or a no-op request. If an app is
	 * using PaxosReplicaCoordinator, nothing special needs to be done by the
	 * app upon an epoch stop, so it can return either null or a no-op request.
	 */
	public InterfaceReconfigurableRequest getStopRequest(String name, int epoch);

	/*
	 * The following methods are automatically provided if
	 * PaxosReplicaCoordinator is used. Otherwise, they have to be implemented
	 * by the app.
	 */
	
	/* Return the final checkpointed state for name, epoch. If a checkpoint
	 * does not yet exist, it has to be created. This method should return null
	 * if the app has moved on, i.e., its current epoch number is higher than
	 * epoch and it has no final state for the requested epoch (possibly 
	 * because it has been garbage-collected). 
	 * 
	 * This method will typically get called right after a stop request has been
	 * supplied for execution to the app, so it is a good idea for the application
	 * to create the checkpoint immediately after executing an epoch stop request.
	 */
	public String getFinalState(String name, int epoch);

	/* Inserts the specified state as the initial state for name, epoch.
	 * This method marks the creation of an epoch. All lower epoch numbers
	 * must be marked as inactive at this point. After this method has
	 * been called, the app can only accept requests for "name" in the 
	 * epoch "epoch".
	 */
	public void putInitialState(String name, int epoch, String state);

	/* Garbage-collects the final state for name, epoch.
	 */
	public boolean deleteFinalState(String name, int epoch);

	/* Returns the current (unique) epoch for name. At most a single
	 * epoch can actively exist at any time at a replica. The app 
	 * replica may maintain the final state for lower (inactive)
	 * epochs, but may not accept requests in any epoch other than
	 * the currently active epoch if one exists. This method should
	 * return null if no active replica for the name exists at this
	 * node.
	 */
	public Integer getEpoch(String name);
}
