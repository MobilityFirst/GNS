/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.reconfiguration.interfaces;

import edu.umass.cs.gigapaxos.interfaces.Application;

/**
 * @author V. Arun
 */

public interface Reconfigurable extends Application {

	/**
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
	 * 
	 * @param name
	 * @param epoch
	 * @return InterfaceReconfigurableRequest corresponding to the stop request
	 *         for the replica group {@code name:epoch}.
	 */
	public ReconfigurableRequest getStopRequest(String name, int epoch);

	/*
	 * The following methods are automatically provided if
	 * PaxosReplicaCoordinator is used. Otherwise, they have to be implemented
	 * by the app.
	 */

	/**
	 * Return the final checkpointed state for name, epoch. If a checkpoint does
	 * not yet exist, it has to be created. This method should return null if
	 * the app has moved on, i.e., its current epoch number is higher than epoch
	 * and it has no final state for the requested epoch (possibly because it
	 * has been garbage-collected).
	 * 
	 * This method will typically get called right after a stop request has been
	 * supplied for execution to the app, so it is a good idea for the
	 * application to create the checkpoint immediately after executing an epoch
	 * stop request.
	 * 
	 * @param name
	 * @param epoch
	 * @return The final state, or a concise handle thereof, as a String.
	 */
	public String getFinalState(String name, int epoch);

	/**
	 * Inserts the specified state as the initial state for name, epoch. This
	 * method marks the creation of an epoch. All lower epoch numbers must be
	 * marked as inactive at this point. After this method has been called, the
	 * app can only accept requests for "name" in the epoch "epoch".
	 * 
	 * @param name
	 * @param epoch
	 * @param state
	 */
	public void putInitialState(String name, int epoch, String state);

	/**
	 * Garbage-collects the final state for name, epoch.
	 * 
	 * @param name
	 * @param epoch
	 * @return True if final state for {@code name:epoch} was deleted or did not
	 *         exist to begin with.
	 */
	public boolean deleteFinalState(String name, int epoch);

	/**
	 * Returns the current (unique) epoch for name. At most a single epoch can
	 * actively exist at any time at a replica. The app replica may maintain the
	 * final state for lower (inactive) epochs, but may not accept requests in
	 * any epoch other than the currently active epoch if one exists. This
	 * method should return null if no active replica for the name exists at
	 * this node.
	 * 
	 * @param name
	 * @return The current epoch number for {@code name}.
	 */
	public Integer getEpoch(String name);
}
