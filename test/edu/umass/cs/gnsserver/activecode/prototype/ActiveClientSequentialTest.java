package edu.umass.cs.gnsserver.activecode.prototype;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.json.JSONException;
import org.junit.Test;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveClientThroughputTest.SimpleTask;
import edu.umass.cs.gnsserver.utils.ValuesMap;

public class ActiveClientSequentialTest {

	
	@Test
	public void test_sequentialRequestThroughput() throws JSONException, ActiveException, InterruptedException, ExecutionException {
		
		final String suffix = "";
		
		int numThread = 1;
		if(System.getProperty("numThread")!=null){
			numThread = Integer.parseInt(System.getProperty("numThread"));
		}
		
		long budget = 500;
		if(System.getProperty("budget")!= null){
			budget = Long.parseLong(System.getProperty("budget"));
		}
		
		long executionTime = 1000;
		if(System.getProperty("executionTime")!=null){
			executionTime = Long.parseLong(System.getProperty("executionTime"));
		}
		
		String codeFile = "./scripts/activeCode/takeTime.js";
		if(System.getProperty("codeFile")!=null){
			codeFile = System.getProperty("codeFile");
		}
		
		String cfile = "/tmp/client"+suffix;
		String sfile = "/tmp/server"+suffix;		
		/**
		 * Test client performance with named pipe channel
		 */
		ActiveClientWithNamedPipe client = new ActiveClientWithNamedPipe(null, cfile, sfile, 0, numThread);
		Thread th = new Thread(client);
		th.start();
		
		String guid = "guid";
		String field = "name";
		String code = "";
		try {
			code = new String(Files.readAllBytes(Paths.get(codeFile)));
		} catch (IOException e) {
			e.printStackTrace();
		} 
		ValuesMap value = new ValuesMap();
		value.put(field, executionTime);	
		ValuesMap result = client.runCode(null, guid, field, code, value, 0, budget);

		assertEquals(result.toString(), value.toString());
		
		int initial = client.getRecv();
		
		int n = 1000000;
		
		long t1 = System.currentTimeMillis();
		
		for (int i=0; i<n; i++){
			client.runCode(null, guid, field, code, value, 0, budget);
			
		}
		
		long elapsed = System.currentTimeMillis() - t1;
		System.out.println("It takes "+elapsed+"ms");
		System.out.println("The average time for each task is "+elapsed/n+"ms.");
		
		assert(client.getRecv()-initial == n):"the number of responses is not the same as the number of requests";
		
		System.out.println("Sequential throughput test succeeds.");
		
		th.interrupt();
		
		client.shutdown();
		
	}
}
