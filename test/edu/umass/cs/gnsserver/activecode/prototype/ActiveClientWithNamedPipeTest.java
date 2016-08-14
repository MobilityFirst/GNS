package edu.umass.cs.gnsserver.activecode.prototype;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.json.JSONException;
import org.junit.Test;

import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * @author gaozy
 *
 */
public class ActiveClientWithNamedPipeTest {
	
	static class SimpleTask implements Callable<ValuesMap>{
		ActiveClientWithNamedPipe client;
		String guid;
		String field;
		String code;
		ValuesMap value;
		int ttl;
		
		SimpleTask(ActiveClientWithNamedPipe client, String guid, String field, String code, ValuesMap value, int ttl){
			this.client = client;
			this.guid = guid;
			this.field = field;
			this.code = code;
			this.value = value;
			this.ttl = ttl;
		}
		
		@Override
		public ValuesMap call() throws Exception {
			return client.runCode(guid, field, code, value, ttl);
		}
		
	}
	
	/**
	 * @throws JSONException
	 * @throws ActiveException
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	@Test
	public void test01_sequentialRequestThroughput() throws JSONException, ActiveException, InterruptedException, ExecutionException {
		
		String suffix = "";

		int numThread = 1; //Integer.parseInt(args[0]);
				
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
		String noop_code = "";
		try {
			noop_code = new String(Files.readAllBytes(Paths.get("./scripts/activeCode/noop.js")));
		} catch (IOException e) {
			e.printStackTrace();
		} 
		ValuesMap value = new ValuesMap();
		value.put("string", "hello world!");	
		ValuesMap result = client.runCode(guid, field, noop_code, value, 0);
		assertEquals(result.toString(), value.toString());
		
		ExecutorService executor = Executors.newFixedThreadPool(10);
		List<Future<ValuesMap>> tasks = new ArrayList<Future<ValuesMap>>();
		
		int n = 1000000;
		
		long t1 = System.currentTimeMillis();
		
		for (int i=0; i<n; i++){
			tasks.add(executor.submit(new SimpleTask(client, guid, field, noop_code, value, 0)));
		}
		
		for (Future<ValuesMap> future:tasks){
			future.get();
		}
		
		long elapsed = System.currentTimeMillis() - t1;
		System.out.println("It takes "+elapsed+"ms, and the average latency for each operation is "+(elapsed*1000.0/n)+"us");
		System.out.println("The average throughput is "+(n*1000.0/elapsed)*numThread);
		
		th.interrupt();
		
		client.shutdown();
	}

}
