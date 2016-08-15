package edu.umass.cs.gnsserver.activecode;

import java.io.IOException;
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
public class ActiveCodeThruputTest {
	
	/**
	 * @throws IOException
	 * @throws JSONException
	 * @throws InterruptedException
	 */
	@Test
	public void thruputWithoutMalTest() throws IOException, JSONException, InterruptedException{

		int numThread = 5;
		int numReq_benign = 1000;
		
		ActiveCodeHandler handler = new ActiveCodeHandler(null);
		ExecutorService executor = Executors.newFixedThreadPool(numThread);
		
		JSONObject obj = new JSONObject();
		obj.put("testGuid", "success");	
		ValuesMap valuesMap = new ValuesMap(obj);
		
		try{
			
			System.out.println("Start testing thruput without malicious users ...");
			
			String noop_code = new String(Files.readAllBytes(Paths.get("./scripts/activeCode/noop.js"))); 
			String noop_code64 = Base64.encodeToString(noop_code.getBytes("utf-8"), true);
			
			for (int i=0; i<numThread; i++){
				final String guid = "guid"+i;
				final String field = "testGuid";
				executor.execute(new RequestThread(numReq_benign, noop_code64, guid, field, valuesMap));
			}
			
			long last = 0;
			long total = 0;
			long TOTAL = numReq_benign*numThread;
			while(total < TOTAL){
				total = handler.getExecutor().getCompletedTaskCount();
				System.out.println("The thruput is "+(total-last)+" reqs/sec");
				last = total;
				Thread.sleep(1000);
			}
			assert(handler.getExecutor().getCompletedTaskCount() == TOTAL);
			assert(handler.getExecutor().getActiveCount() == 0);
			executor.shutdown();
			
			System.out.println("############### Thruput test without malicious users passed! ###############\n\n");
			
			
		} catch(Exception | Error e){
			e.printStackTrace();
		}finally{
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
			}
			
		}
		
	}
}
