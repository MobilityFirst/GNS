package edu.umass.cs.gigapaxos.paxosutil;

/**
 * @author arun
 *
 *         A silly container for a string to distinguish between null state and
 *         no state when we retrieve paxos checkpoint state.
 */
@SuppressWarnings("javadoc")
public class StringContainer {
	/**
	 * The contained string.
	 */
	public final String state;

	public StringContainer(String s) {
		this.state = s;
	}
}
