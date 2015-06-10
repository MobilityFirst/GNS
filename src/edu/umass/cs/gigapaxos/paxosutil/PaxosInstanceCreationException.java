package edu.umass.cs.gigapaxos.paxosutil;

/*
 * @author arun
 * 
 * We need the PaxosInstanceStateMachine constructor to be able to throw an
 * exception if an instance can not be created for any app-specific reason.
 */
@SuppressWarnings("javadoc")
public class PaxosInstanceCreationException extends RuntimeException {
	static final long serialVersionUID = 0;

	/**
	 * @param msg
	 */
	public PaxosInstanceCreationException(String msg) {
		super(msg);
	}
}
