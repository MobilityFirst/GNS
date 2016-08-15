package edu.umass.cs.gnsserver.activecode;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.ActiveCode;
import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * @author gaozy
 *
 */
public class ActiveCodeMalThruputTest {
	static int completed = 0;
	
	static synchronized void incr(){
		completed++;
	}
	
	/**
	 * @throws JSONException
	 * @throws InterruptedException 
	 */
	@Test
	public void thruputWithMaliciousTest() throws JSONException, InterruptedException{
		int numThread = 5;
		int numReq_benign = 1000;
		int numReq_mal = 10;
		
		ActiveCodeHandler handler = new ActiveCodeHandler(null);
		ExecutorService executor = Executors.newFixedThreadPool(numThread);
		
		Thread.sleep(2000);
		
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
			executor.execute(new RequestThread(numReq_mal, mal_code64, "GUID", "nextGuid", valuesMap));
			
			long last = 0;
			long total = 0;
			long TOTAL_MAL = numReq_mal + numReq_benign*(numThread - 1);
			while(total < TOTAL_MAL){
				total = completed;
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
	
	static class RequestThread implements Runnable {
		int numReq = 0;
		String code64;
		String guid;
		String field;
		String action = ActiveCode.READ_ACTION;
		ValuesMap valuesMap;
		
		
		RequestThread(int numReq, String code64, String guid, String field, ValuesMap valuesMap){
			this.numReq = numReq;
			this.code64 = code64;
			this.guid = guid;
			this.field = field;
			this.valuesMap = valuesMap;
		}
		@Override
		public void run() {
			for (int i=0; i<numReq; i++){
				ActiveCodeHandler.runCode(null, code64, guid, field, action, valuesMap, 100);
				ActiveCodeMalThruputTest.incr();
			}			
		}
		
	}
}
