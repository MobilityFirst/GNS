package edu.umass.cs.gigapaxos.deprecated;

/**
 * @author V. Arun
 */

public interface Application {
	/**
	 * An atomic operation that either completes and returns true or throws an
	 * exception. If the method does not return true, paxos will call this
	 * method again until it returns true.
	 * 
	 * @param name
	 * @param value
	 * @param doNotReplyToClient
	 * @return Should always return true or throw an exception. The paxos
	 *         replicated state machine can not make progress otherwise.
	 */
	public boolean handleDecision(String name, String value,
			boolean doNotReplyToClient);
}
