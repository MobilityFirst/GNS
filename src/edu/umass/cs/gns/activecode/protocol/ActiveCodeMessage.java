package edu.umass.cs.gns.activecode.protocol;

import java.io.Serializable;

import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.util.ValuesMap;

/**
 * Used to pass messages between the main GNS process
 * and active code worker processes
 * @author mbadov
 *
 */
public class ActiveCodeMessage implements Serializable {
	/**
	 * Indicates that the worker should shut down
	 */
	public boolean shutdown;
	
	/**
	 * Indicates that the active code request finished
	 */
	public boolean finished;
	
	/**
	 * Inidicates that the active code request crashed
	 */
	public boolean crashed;
	
	/**
	 * The active code params to execute
	 */
	public ActiveCodeParams acp;
	
	/**
	 * Stores the result of the active code computation
	 */
	public String valuesMapString;
	
	/**
	 * Denotes a request by the worker to perform a query
	 */
	public ActiveCodeQueryRequest acqreq;
	
	/**
	 * Returns a query response to the worker
	 */
	public ActiveCodeQueryResponse acqresp;
}
