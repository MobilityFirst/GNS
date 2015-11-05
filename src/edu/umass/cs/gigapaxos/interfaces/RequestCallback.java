package edu.umass.cs.gigapaxos.interfaces;


/**
 * @author arun
 *
 */
public interface RequestCallback {
	/**
	 * @param response
	 */
	public void handleResponse(Request response);
}
