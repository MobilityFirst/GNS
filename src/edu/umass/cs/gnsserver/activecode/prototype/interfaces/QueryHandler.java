package edu.umass.cs.gnsserver.activecode.prototype.interfaces;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage;
import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * This class is used for executing the queries sent from
 * ActiveWorker and sending response back.
 * @author gaozy
 *
 */
public abstract class QueryHandler {
	
	/**
	 * This method handles the incoming requests from ActiveQuerier,
	 * the query could be a read or write request.
	 * 
	 * @param am 
	 * @return an ActiveMessage being sent back to worker as a response to the query
	 */
	public ActiveMessage handleQuery(ActiveMessage am){
		ActiveMessage response;
		if(am.type == ActiveMessage.Type.READ_QUERY)
			response = handleReadQuery(am.getGuid(), am.getTargetGuid(), am.getField(), am.getTtl());
		else
			response = handleWriteQuery(am.getGuid(), am.getTargetGuid(), am.getField(), am.getValue(), am.getTtl());
		
		return response;
	}
	
	
	/**
	 * This method handles read query from the worker. 
	 * 
	 * @param querierGuid
	 * @param queriedGuid
	 * @param field 
	 * @param ttl 
	 * @return the response ActiveMessage
	 */
	public abstract ActiveMessage handleReadQuery(String querierGuid, String queriedGuid, String field, int ttl);
	
	/**
	 * This method handles write query from the worker. 
	 * 
	 * @param querierGuid
	 * @param queriedGuid
	 * @param field 
	 * @param value
	 * @param ttl 
	 * @return the response ActiveMessage
	 */
	public abstract ActiveMessage handleWriteQuery(String querierGuid, String queriedGuid, String field, ValuesMap value, int ttl);
	
}
