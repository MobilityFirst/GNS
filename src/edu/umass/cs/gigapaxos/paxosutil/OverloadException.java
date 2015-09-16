package edu.umass.cs.gigapaxos.paxosutil;

/**
 * @author arun
 *
 */
public class OverloadException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	/**
	 * @param msg
	 */
	public OverloadException(String msg) {
		super(msg);
	}
}
