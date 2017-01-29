package edu.umass.cs.gnsserver.activecode.prototype.interfaces;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveException;
import jdk.nashorn.api.scripting.ScriptObjectMirror;


public interface DNSQuerier {
	

	public ScriptObjectMirror getLocations(ScriptObjectMirror ipList) throws ActiveException;
		
}
