package edu.umass.cs.gnsserver.activecode.prototype;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.json.JSONException;

import edu.umass.cs.gnsserver.interfaces.ActiveDBInterface;
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
	 * @param am O
	 * @return an ActiveMessage being sent back to worker as a response to the query
	 */
	public ActiveMessage handleQuery(ActiveMessage am){
		ActiveMessage response;
		if(am.type == ActiveMessage.Type.READ_QUERY)
			response = handleReadQuery(am);
		else
			response = handleWriteQuery(am);

		return response;
	}
	
	
	/**
	 * This method handles read query from the worker. 
	 * 
	 * @param am 
	 * @return the response ActiveMessage
	 */
	public ActiveMessage handleReadQuery(ActiveMessage am) {
		ValuesMap value = app.read(am.getGuid(), am.getTargetGuid(), am.getField());
		ActiveMessage resp = new ActiveMessage(am.getId(), value, null);
		return resp;
	}

	
	/**
	 * This method handles write query from the worker. 
	 * 
	 * @param am 
	 * @return the response ActiveMessage
	 */
	public ActiveMessage handleWriteQuery(ActiveMessage am) {
		boolean wSuccess = app.write(am.getGuid(), am.getTargetGuid(), am.getField(), am.getValue());
		ActiveMessage resp;
		if(wSuccess){
			resp = new ActiveMessage(am.getId(), new ValuesMap(), null);
		}else{
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
			handler.runCode(guid, field, depth_code, value, 0);
		}
		
		
		long elapsed = System.currentTimeMillis() - t1;
		System.out.println("It takes "+elapsed+"ms, and the average latency for each operation is "+(elapsed*1000.0/n)+"us");
	}
}
