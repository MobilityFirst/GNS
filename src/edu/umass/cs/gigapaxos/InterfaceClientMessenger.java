package edu.umass.cs.gigapaxos;

import org.json.JSONObject;

import edu.umass.cs.nio.InterfaceSSLMessenger;

/**
 * @author arun
 *
 */
public interface InterfaceClientMessenger {
	/**
	 * An interface to supply a messenger to the app so that it can directly
	 * send responses for executed requests back to the app.
	 * 
	 * @param messenger
	 */
	public void setClientMessenger(
			InterfaceSSLMessenger<?, JSONObject> messenger);
}
