package edu.umass.cs.gnsserver.activecode;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnsserver.activecode.ActiveCodeThruputTest.RequestThread;
import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * @author gaozy
 *
 */
public class ActiveCodeMalThruputTest {	
	/**
	 * @throws JSONException
	 */
	@Test
	public void thruputWithMaliciousTest() throws JSONException{
		int numThread = 5;
		int numReq_benign = 1000;
		int numReq_mal = 10;
		
		ActiveCodeHandler handler = new ActiveCodeHandler(null);
		ExecutorService executor = Executors.newFixedThreadPool(numThread);
		
		JSONObject obj = new JSONObject();
		obj.put("testGuid", "success");	
		ValuesMap valuesMap = new ValuesMap(obj);
		
		try{
			System.out.println("Start testing thruput with one malicious user and "+ (numThread-1)+" benign users ...");
			String noop_code = new String(Files.readAllBytes(Paths.get("./scripts/activeCode/noop.js"))); 
			String noop_code64 = Base64.encodeToString(noop_code.getBytes("utf-8"), true);
			
			executor = Executors.newFixedThreadPool(numThread);
			String mal_code = new String(Files.readAllBytes(Paths.get("./scripts/activeCode/mal.js")));
			String mal_code64 = Base64.encodeToString(mal_code.getBytes("utf-8"), true);
			for (int i=0; i<numThread-1; i++){
				final String guid = "guid"+i;
				final String field = "testGuid";
				executor.execute(new RequestThread(numReq_benign, noop_code64, guid, field, valuesMap));
			}
			executor.execute(new RequestThread(numReq_mal, mal_code64, "guid2", "nextGuid", valuesMap));
			
			long last = 0;
			long total = 0;
			long TOTAL_MAL = numReq_mal +  numReq_benign*(numThread-1);
			while(total < TOTAL_MAL){
				total = handler.getExecutor().getCompletedTaskCount();
				System.out.println("The thruput is "+(total-last)+" reqs/sec, totally finished: "+total+" reqs /"+handler.getExecutor().getActiveCount());
				last = total;
				Thread.sleep(1000);
			}

			assert(handler.getExecutor().getCompletedTaskCount() == TOTAL_MAL);
			assert(handler.getExecutor().getActiveCount() == 0);
			
			System.out.println("############### Thruput test with malicious users passed! ###############");
		} catch (Exception | Error e){
			e.printStackTrace();
		} finally {
			handler.getClientPool().shutdown();
		}
	}
}
