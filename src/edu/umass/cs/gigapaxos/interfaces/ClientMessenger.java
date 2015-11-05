package edu.umass.cs.gigapaxos.interfaces;

import org.json.JSONObject;

import edu.umass.cs.nio.interfaces.SSLMessenger;

/**
 * @author arun
 *
 */
public interface ClientMessenger {
	/**
	 * An interface to supply a messenger to the app so that it can directly
	 * send responses for executed requests back to the app.
	 * 
	 * @param messenger
	 */
	public void setClientMessenger(
			SSLMessenger<?, JSONObject> messenger);
}
