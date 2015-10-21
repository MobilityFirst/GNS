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
package edu.umass.cs.gigapaxos;

/**
 * @author V. Arun
 */
public interface InterfaceReplicable extends InterfaceApplication {
	/**
	 * This method must handle the request atomically and return true or throw
	 * an exception.
	 * 
	 * @param request
	 * @param doNotReplyToClient
	 * @return Returns true if the application handled the request successfully.
	 *         If the request is bad and is to be discarded, the application
	 *         must still return true (after "successfully" discarding it). If
	 *         the application returns false, the replica coordination protocol
	 *         (e.g., paxos) will try to repeatedly re-execute it until
	 *         successful, so the application may get stuck.
	 */
	public boolean handleRequest(InterfaceRequest request,
			boolean doNotReplyToClient);

	/**
	 * Checkpoints the current application state and returns it.
	 * 
	 * @param name
	 * @return Returns the checkpoint state. If the application encounters an
	 *         error while creating the checkpoint, it must retry until
	 *         successful or throw a RuntimeException. Returning a null value
	 *         will be interpreted to mean that the application state is indeed
	 *         null.
	 */
	public String getState(String name);

	/**
	 * Resets the current application state for {@code name} to {@code state}.
	 * Note that {@code state} may simply be an app-specific handle, e.g., a
	 * file name, representing the state as opposed to the actual state.
	 * 
	 * @param name
	 * @param state
	 * @return True if the app atomically updated the state successfully. Else,
	 *         it must throw an exception. If it returns false, the replica
	 *         coordination protocol may try to repeat the operation until
	 *         successful causing the application to get stuck.
	 */
	public boolean updateState(String name, String state);
}
