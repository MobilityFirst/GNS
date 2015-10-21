package edu.umass.cs.gigapaxos;

/**
 * @author arun
 *
 */
public interface RequestCallback {
	/**
	 * @param response
	 */
	public void handleResponse(InterfaceRequest response);
}
