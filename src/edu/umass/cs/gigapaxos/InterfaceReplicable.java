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
	 *         must still return true (after "successfully" discarding it. If
	 *         the application returns false, the replica coordination protocol
	 *         (e.g., paxos) may try to repeatedly re-execute it until
	 *         successful, so the application may get stuck.
	 */
	public boolean handleRequest(InterfaceRequest request,
			boolean doNotReplyToClient);

	/**
	 * Checkpoints the current application state and returns it.
	 * 
	 * @param name
	 * @return Returns the checkpointed state. If the application encounters an
	 *         error while creating the checkpoint, it must retry until
	 *         successful or throw a RuntimeException. Returning a null value
	 *         will be interpreted to mean that the application state is indeed
	 *         null. Returning a legitimate null value for a checkpoint is a bad
	 *         idea as it can cause reconfiguration of paxos groups to stall; it
	 *         is better to return an empty string instead.
	 */
	public String getState(String name);

	/**
	 * Resets the current application state for {@code name} to {@code state}. Note that
	 * {@code state} may simply be an app-specific handle, e.g., a file name, representing
	 * the state as opposed to the actual state.
	 * 
	 * @param name
	 * @param state
	 * @return True if the app atomically updated the state successfully. Else, it must return false. 
	 */
	public boolean updateState(String name, String state);
}
