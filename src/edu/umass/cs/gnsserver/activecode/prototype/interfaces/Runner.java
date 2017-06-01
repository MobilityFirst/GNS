package edu.umass.cs.gnsserver.activecode.prototype.interfaces;

import javax.script.ScriptException;

/**
 * @author gaozy
 *
 */
public interface Runner {
	
	/**
	 * 
	 * @param guid
	 * @param accessor
	 * @param code
	 * @param value
	 * @param ttl
	 * @param id
	 * @return the executed result by script engine
	 * @throws ScriptException 
	 * @throws NoSuchMethodException 
	 */
	public String runCode(String guid, String accessor, String code, String value, int ttl, long id) throws ScriptException, NoSuchMethodException;
}
