package edu.umass.cs.gnsserver.activecode.prototype.interfaces;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveException;
import org.json.JSONObject;


public interface ACLQuerier {

	public JSONObject lookupUsernameForGuid(String guid) throws ActiveException;
}
