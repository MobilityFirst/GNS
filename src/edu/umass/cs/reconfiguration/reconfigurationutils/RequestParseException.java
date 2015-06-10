package edu.umass.cs.reconfiguration.reconfigurationutils;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.InterfaceApplication;

/**
 * @author V. Arun
 * 
 */
public class RequestParseException extends Exception {
	static final long serialVersionUID = 0;

	/**
	 * Meant to be thrown when {@link InterfaceApplication#getRequest(String)}
	 * can not parse the supplied string into an InterfaceRequest object.
	 * 
	 * @param e
	 */
	public RequestParseException(Exception e) {
		super(e);
	}

	static void main(String[] args) {
		JSONObject json = new JSONObject();
		try {
			json.getString("key");
		} catch (JSONException je) {
			RequestParseException rpe = new RequestParseException(je);
			rpe.printStackTrace();
		}
	}
}
