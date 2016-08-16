package edu.umass.cs.gnsserver.activecode.prototype.interfaces;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveException;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.gnsserver.utils.ValuesMap;

/** 
 * 
 * @author gaozy
 *
 */
public interface Client {
	
	/**
	 * The handler only call this client interface to let the client
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
	 * @param field
	 * @param code
	 * @param valuesMap
	 * @param ttl
	 * @param budget 
	 * @return the executed result as an ValuesMap object
	 * @throws ActiveException 
	 */
	public ValuesMap runCode(InternalRequestHeader header, String guid, String field, String code, ValuesMap valuesMap, int ttl, long budget) throws ActiveException;
	
	
	/**
	 * Shutdown this client when system stopped.
	 */
	public void shutdown();
}
