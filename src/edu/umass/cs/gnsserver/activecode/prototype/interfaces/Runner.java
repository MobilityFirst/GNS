package edu.umass.cs.gnsserver.activecode.prototype.interfaces;

import javax.script.ScriptException;


public interface Runner {
	

	public String runCode(String guid, String accessor, String code, String value, int ttl, long id) throws ScriptException, NoSuchMethodException;
}
