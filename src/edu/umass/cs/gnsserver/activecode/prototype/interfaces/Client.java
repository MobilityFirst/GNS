package edu.umass.cs.gnsserver.activecode.prototype.interfaces;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveException;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import org.json.JSONObject;


public interface Client {
	

	public JSONObject runCode(InternalRequestHeader header, String guid, String accessor, String code, JSONObject valuesMap, int ttl, long budget) throws ActiveException;
	
	

	public void shutdown();
}
