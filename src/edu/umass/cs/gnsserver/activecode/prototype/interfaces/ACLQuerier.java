package edu.umass.cs.gnsserver.activecode.prototype.interfaces;

import org.json.JSONObject;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveException;


public interface ACLQuerier {

	public JSONObject lookupUsernameForGuid(String guid) throws ActiveException;
}
