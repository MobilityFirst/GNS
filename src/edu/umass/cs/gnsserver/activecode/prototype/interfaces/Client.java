package edu.umass.cs.gnsserver.activecode.prototype.interfaces;

import org.json.JSONObject;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveException;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;

/** 
 * 
 * @author gaozy
 *
 */
public interface Client {
	
	/**
	 * The handler calls this method to have the client to
	 * process a request. All I/O and messaging are hidden to handler.
	 * This method needs to handle the communication between GNS and
	 * worker.
	 * 
	 * <p>An ActiveException should be thrown by this method if the
	 * request failed as indicated by the response message, or 
	 * communication channel is broken.
	 * 
	 * @param header 
	 * @param guid
	 * @param accessor
	 * @param code
	 * @param valuesMap
	 * @param ttl
	 * @param budget 
	 * @return the executed result as an ValuesMap object
	 * @throws ActiveException 
	 */
	public JSONObject runCode(InternalRequestHeader header, String guid, String accessor, String code, JSONObject valuesMap, int ttl, long budget) throws ActiveException;
	
	
	/**
	 * Shutdown this client when system stopped.
	 */
	public void shutdown();
}
