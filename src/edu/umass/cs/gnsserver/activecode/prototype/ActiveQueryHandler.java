package edu.umass.cs.gnsserver.activecode.prototype;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.json.JSONException;

import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;
import edu.umass.cs.gnsserver.interfaces.ActiveDBInterface;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * This class is used for executing the queries sent from
 * ActiveWorker and sending response back.
 * @author gaozy
 *
 */
public class ActiveQueryHandler {
	ActiveDBInterface app;
	
	/**
	 * Initialize a query handler
	 * @param app
	 */
	public ActiveQueryHandler(ActiveDBInterface app){
		this.app = app;
	}
	
	
	/**
	 * This method handles the incoming requests from ActiveQuerier,
	 * the query could be a read or write request.
	 * 
	 * @param am the query to handle
	 * @param header 
	 * @return an ActiveMessage being sent back to worker as a response to the query
	 */
	public ActiveMessage handleQuery(ActiveMessage am, InternalRequestHeader header){
		ActiveMessage response;
		if(am.type == ActiveMessage.Type.READ_QUERY)
			response = handleReadQuery(am, header);
		else
			response = handleWriteQuery(am, header);
		return response;
	}
	
	
	/**
	 * This method handles read query from the worker. 
	 * 
	 * @param am 
	 * @param header 
	 * @return the response ActiveMessage
	 */
	public ActiveMessage handleReadQuery(ActiveMessage am, InternalRequestHeader header) {
		
		ActiveMessage resp = null;
		try {
			ValuesMap value = new ValuesMap(app.read(header, am.getTargetGuid(), am.getField()));
			resp = new ActiveMessage(am.getId(), value, null);
		} catch (InternalRequestException | ClientException e) {
			e.printStackTrace();
			resp = new ActiveMessage(am.getId(), null, "Read failed");
		} 
		
		return resp;
	}

	
	/**
	 * This method handles write query from the worker. 
	 * 
	 * @param am 
	 * @param header 
	 * @return the response ActiveMessage
	 */
	public ActiveMessage handleWriteQuery(ActiveMessage am, InternalRequestHeader header) {
		ActiveMessage resp;
		try {
			app.write(header, am.getTargetGuid(), am.getField(), am.getValue());
			resp = new ActiveMessage(am.getId(), new ValuesMap(), null);
		} catch (ClientException | InternalRequestException e) {
			e.printStackTrace();
			resp = new ActiveMessage(am.getId(), null, "Write failed");
		} 
				
		return resp;
	}
	
	/**
	 * @param args
	 * @throws JSONException
	 * @throws ActiveException 
	 */
	public static void main(String[] args) throws JSONException, ActiveException{
		
		String guid = "zhaoyu gao";
		String field = "nextGuid";
		String depth_code = "";
		try {
			depth_code = new String(Files.readAllBytes(Paths.get("./scripts/activeCode/chain.js")));
		} catch (IOException e) {
			e.printStackTrace();
		} 
		ValuesMap value = new ValuesMap();
		value.put("nextGuid", "alvin");
		
		ActiveHandler handler = new ActiveHandler(null, 1);
		
		int n = 1000000;
		
		long t1 = System.currentTimeMillis();
		
		for(int i=0; i<n; i++){
			handler.runCode(null, guid, field, depth_code, value, 0);
		}
		
		
		long elapsed = System.currentTimeMillis() - t1;
		System.out.println("It takes "+elapsed+"ms, and the average latency for each operation is "+(elapsed*1000.0/n)+"us");
	}
}
