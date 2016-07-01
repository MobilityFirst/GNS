package edu.umass.cs.gnsserver.activecode.prototype.interfaces;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage;
import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * This class is used for executing the queries sent from
 * ActiveWorker and sending response back.
 * @author gaozy
 *
 */
public abstract class ActiveQueryHandler {
	
	GNSClient client;
	
	/**
	 * This method handles the incoming requests from ActiveQuerier,
	 * the query could be a read or write request.
	 * 
	 * @param am 
	 * @return an ActiveMessage being sent back to worker as a response to the query
	 */
	public ActiveMessage handleQuery(ActiveMessage am){
		ActiveMessage response;
		if(am.isRead())
			response = handleReadQuery(am.getGuid(), am.getTargetGuid(), am.getValue(), am.getTtl());
		else
			response = handleWriteQuery(am.getGuid(), am.getTargetGuid(), am.getValue(), am.getTtl());
		
		return response;
	}
	
	
	/**
	 * This method handles read query from the worker. 
	 * 
	 * @param querierGuid
	 * @param queriedGuid
	 * @param value
	 * @param ttl 
	 * @return the response ActiveMessage
	 */
	public abstract ActiveMessage handleReadQuery(String querierGuid, String queriedGuid, ValuesMap value, int ttl);
	
	/**
	 * This method handles write query from the worker. 
	 * 
	 * @param querierGuid
	 * @param queriedGuid
	 * @param value
	 * @param ttl 
	 * @return the response ActiveMessage
	 */
	public abstract ActiveMessage handleWriteQuery(String querierGuid, String queriedGuid, ValuesMap value, int ttl);
	
}
