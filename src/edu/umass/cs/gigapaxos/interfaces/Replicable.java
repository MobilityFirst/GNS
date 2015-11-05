/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.gigapaxos.interfaces;

/**
 * @author V. Arun
 */
public interface Replicable extends Application {
	/**
	 * This method must handle the request atomically and return true or throw
	 * an exception or return false. It is the application's responsibility to
	 * ensure atomicity, i.e., it should return true iff the request was
	 * successfully executed; if it returns false or throws an exception, the
	 * application should ensure that the state of the application rolls back
	 * any partial execution of this request.
	 * 
	 * @param request
	 * @param doNotReplyToClient
	 *            If true, the application is expected to not send a response
	 *            back to the originating client (say, because this request is
	 *            part of a post-crash roll-forward or only the primary replica
	 *            or the "entry replica" that received the request from the
	 *            client is expected to respond back. If false, the application
	 *            is expected to either send a response (if any) back to the
	 *            client via the {@link ClientMessenger} interface or delegate
	 *            response messaging to paxos via the {@link ClientRequest} and
	 *            interface.
	 * 
	 * @return Returns true if the application handled the request successfully.
	 *         If the request is bad and is to be discarded, the application
	 *         must still return true (after "successfully" discarding it). If
	 *         the application returns false, the replica coordination protocol
	 *         (e.g., paxos) might try to repeatedly re-execute it until
	 *         successful or kill this replica group (
	 *         {@code request.getServiceName()}) altogether after a limited
	 *         number of retries, so the replica group may get stuck unless it
	 *         returns true after a limited number of retries. Thus, with paxos
	 *         as the replica coordination protocol, returning false is not
	 *         really an option as paxos has no way to "roll back" a request
	 *         whose global order has already been agreed upon.
	 *         <p>
	 *         With replica coordination protocols other than paxos, the boolean
	 *         return value could be used in protocol-specific ways, e.g., a
	 *         primary-backup protocol might normally execute a request at just
	 *         one replica but relay the request to one or more backups if the
	 *         return value of {@link #execute(Request)} is false.
	 */
	public boolean execute(Request request, boolean doNotReplyToClient);

	/**
	 * Checkpoints the current application state and returns it.
	 * 
	 * @param name
	 * @return Returns the checkpoint state. If the application encounters an
	 *         error while creating the checkpoint, it must retry until
	 *         successful or throw a RuntimeException. Returning a null value
	 *         will be interpreted to mean that the application state is indeed
	 *         null.
	 *         <p>
	 *         Note that {@code state} may simply be an app-specific handle,
	 *         e.g., a file name, representing the state as opposed to the
	 *         actual state. The application is responsible for interpreting the
	 *         state returned by {@link #checkpoint(String)} and that supplied
	 *         in {@link #restore(String, String)} in a consistent manner.
	 */
	public String checkpoint(String name);

	/**
	 * Resets the current application state for {@code name} to {@code state}.
	 * <p>
	 * Note that {@code state} may simply be an app-specific handle, e.g., a
	 * file name, representing the state as opposed to the actual state. The
	 * application is responsible for interpreting the state returned by
	 * {@link #checkpoint(String)} and that supplied in
	 * {@link #restore(String, String)} in a consistent manner.
	 * 
	 * @param name
	 * @param state
	 * @return True if the app atomically updated the state successfully. Else,
	 *         it must throw an exception. If it returns false, the replica
	 *         coordination protocol may try to repeat the operation until
	 *         successful causing the application to get stuck.
	 */
	public boolean restore(String name, String state);
}
