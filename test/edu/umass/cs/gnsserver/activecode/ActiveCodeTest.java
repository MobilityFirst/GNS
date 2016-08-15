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

public class ActiveCodeTest {

	/**
	 * Make sure there are at least 2 workers to test the depth
	 * @throws JSONException 
	 * @throws InterruptedException 
	 */
	@Test
	public void test() throws JSONException, InterruptedException {
		// Initialize the handler and get the executor for instrument
		ActiveCodeHandler handler = new ActiveCodeHandler(null);
		ActiveCodeExecutor executor = handler.getExecutor();
		
		Thread.sleep(2000);
		
		
		// The variable to record the total number of tasks that should be completed
		int completed = 0;
		
		// initialize the parameters used in the test 
		JSONObject obj = new JSONObject();
		obj.put("testGuid", "success");

		ValuesMap valuesMap = new ValuesMap(obj);
		final String guid1 = "guid1";
		final String field1 = "testGuid";
		final String read_action = "read";
		
		try{		
		
			/************** Test normal code *************/
			System.out.println("################# Start testing normal active code ... ###################");
			String noop_code = new String(Files.readAllBytes(Paths.get("./scripts/activeCode/noop.js"))); 
			String noop_code64 = Base64.encodeToString(noop_code.getBytes("utf-8"), true);
			
			ValuesMap result = ActiveCodeHandler.runCode(null, noop_code64, guid1, field1, read_action, valuesMap, 100);
			completed++;
			//Thread.sleep(2000);
			System.out.println("Active count number is "+executor.getActiveCount()+
					", the number of completed tasks is "+executor.getCompletedTaskCount());
			
			// test result, # of completed tasks, and # of active threads
			System.out.println(result);
			
			assert(executor.getActiveCount() == 0);
			assert(executor.getCompletedTaskCount() == completed);
			System.out.println("############# TEST FOR NOOP PASSED! ##############\n\n");
			Thread.sleep(1000);
			
			
			/************** Test malicious code *************/
			System.out.println("################# Start testing malicious active code ... ###################");
			String mal_code = new String(Files.readAllBytes(Paths.get("./scripts/activeCode/mal.js")));
			String mal_code64 = Base64.encodeToString(mal_code.getBytes("utf-8"), true);
					
			result = ActiveCodeHandler.runCode(null, mal_code64, "guid1", "testGuid", "read", valuesMap, 100);
			completed++;
			result = ActiveCodeHandler.runCode(null, noop_code64, guid1, field1, read_action, valuesMap, 100);
			completed++;
			//Thread.sleep(2000);
			
			assert(executor.getActiveCount() == 0);
			assert(executor.getCompletedTaskCount() == completed);
			System.out.println("############# TEST FOR MALICOUS PASSED! ##############\n\n");
			//Thread.sleep(1000);
			
			/************** Test chain code *************/
			System.out.println("################# Start testing chain active code ... ###################");
			String chain_code = new String(Files.readAllBytes(Paths.get("./scripts/activeCode/chain-test.js")));
			String chain_code64 = Base64.encodeToString(chain_code.getBytes("utf-8"), true);
					
			result = ActiveCodeHandler.runCode(null, chain_code64, "guid1", "testGuid", "read", valuesMap, 100);
			completed++;
			completed++;
			result = ActiveCodeHandler.runCode(null, noop_code64, guid1, field1, read_action, valuesMap, 100);
			completed++;
			
			int count = 0;
			while(count <10){
				System.out.println("" + executor.getCompletedTaskCount() + " "
						+ executor.getActiveCount() + "; actualActiveCount = "
						+ ActiveCodeTask.getActiveCount());
				if(executor.getActiveCount()==0) break;
				Thread.sleep(1000);
				count++;
			}
			assert(executor.getActiveCount() == 0) : executor.getActiveCount();
			assert(executor.getCompletedTaskCount() == completed);
			System.out.println("############# TEST FOR CHAIN PASSED! ##############\n\n");
			
		}catch(Exception | Error e){
			e.printStackTrace();
		}finally{
			handler.getClientPool().shutdown();
		}
	}
	
}
