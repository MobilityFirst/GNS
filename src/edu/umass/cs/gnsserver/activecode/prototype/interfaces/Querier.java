package edu.umass.cs.gnsserver.activecode.prototype.interfaces;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveException;
import jdk.nashorn.api.scripting.ScriptObjectMirror;


@SuppressWarnings("restriction")
public interface Querier {
	

	public ScriptObjectMirror readGuid(String queriedGuid, String field) throws ActiveException;
	
	

	public void writeGuid(String queriedGuid, String field, ScriptObjectMirror value) throws ActiveException;
}
