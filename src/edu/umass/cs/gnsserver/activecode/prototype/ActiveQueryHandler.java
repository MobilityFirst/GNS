package edu.umass.cs.gnsserver.activecode.prototype;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.json.JSONException;

import edu.umass.cs.gnsserver.activecode.prototype.interfaces.QueryHandler;
import edu.umass.cs.gnsserver.interfaces.ActiveDBInterface;
import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * @author gaozy
 *
 */
public class ActiveQueryHandler extends QueryHandler{
	ActiveDBInterface app;
	
	protected ActiveQueryHandler(ActiveDBInterface app){
		this.app = app;
	}
	
	@Override
	public ActiveMessage handleReadQuery(String querierGuid, String queriedGuid, String field, int ttl) {
		ValuesMap value = app.read(querierGuid, queriedGuid, field);
		ActiveMessage am = new ActiveMessage(value, null);
		return am;
	}

	@Override
	public ActiveMessage handleWriteQuery(String querierGuid, String queriedGuid, String field,  ValuesMap valuesMap, int ttl) {
		boolean wSuccess = app.write(querierGuid, queriedGuid, field, valuesMap);
		ActiveMessage am;
		if(wSuccess){
			am = new ActiveMessage(new ValuesMap(), null);
		}else{
			am = new ActiveMessage(null, "Write failed");
		}
		return am;
	}
	
	/**
	 * @param args
	 * @throws JSONException
	 */
	public static void main(String[] args) throws JSONException{
		
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
