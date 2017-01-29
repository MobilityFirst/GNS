package edu.umass.cs.gnsserver.activecode.prototype.interfaces;

import jdk.nashorn.api.scripting.ScriptObjectMirror;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveException;


public interface DNSQuerier {
	

	public ScriptObjectMirror getLocations(ScriptObjectMirror ipList) throws ActiveException;
		
}
