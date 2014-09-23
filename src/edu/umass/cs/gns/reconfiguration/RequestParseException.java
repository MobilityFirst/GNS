package edu.umass.cs.gns.reconfiguration;

import org.json.JSONException;
import org.json.JSONObject;

/**
@author V. Arun
 */
public class RequestParseException extends Exception {
	static final long serialVersionUID = 0;
	public RequestParseException(Exception e) {
		super(e);
	}
	public static void main(String[] args) {
		JSONObject json = new JSONObject();
		try {
			json.getString("key");
		} catch(JSONException je) {
			RequestParseException rpe = new RequestParseException(je);
			rpe.printStackTrace();
		}
	}
}
