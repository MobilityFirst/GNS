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
	 * This test sends requests all together and check the responses one by one.
	 * The output shows the throughput.
	 * 
	 * @throws JSONException
	 * @throws ActiveException
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	@Test
	public void test_01_sequentialRequestThroughput() throws JSONException, ActiveException, InterruptedException, ExecutionException {
		
		final String suffix = "";

		final int numThread = 1;
				
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
		
		int initial = client.getRecv();
		
		int n = 100000;
		
		long t1 = System.currentTimeMillis();
		
		for (int i=0; i<n; i++){
			//client.runCode(guid, field, noop_code, value, 0);
			tasks.add(executor.submit(new SimpleTask(client, guid, field, noop_code, value, 0)));
		}
		
		for (Future<ValuesMap> future:tasks){
			future.get();
		}
		
		long elapsed = System.currentTimeMillis() - t1;
		System.out.println("It takes "+elapsed+"ms");
		System.out.println("The average throughput is "+(n*1000.0/elapsed)*numThread);
		
		assert(client.getRecv()-initial == n):"the number of responses is not the same as the number of requests";
		
		System.out.println("Throughput test succeeds.");
		th.interrupt();
		
		client.shutdown();
		
	}
	
	/**
	 * This manually crashes the worker to test the worker
	 * recovery property.
	 * @throws InterruptedException 
	 * @throws JSONException 
	 * @throws ActiveException 
	 */
	@Test
	public void test_11_crashWorker() throws InterruptedException, JSONException, ActiveException{
		final String suffix = "";
		final int numThread = 1;
		
		// initialize the client		
		String cfile = "/tmp/client"+suffix;
		String sfile = "/tmp/server"+suffix;		
		ActiveClientWithNamedPipe client = new ActiveClientWithNamedPipe(null, cfile, sfile, 0, numThread);
		Thread th1 = new Thread(client);
		th1.start();
		
		Thread.sleep(1000);
		
		// Kill the worker periodically
		Thread th2 = new Thread(client){
			@Override
			public void run(){
				int count = 0;
				while(count < 2){
					if(client != null && client.getWorker() != null){
						client.getWorker().destroyForcibly();
						count++;
					}
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						// swallow this exception
					}
				}				
			}
		};
		th2.start();		
		
		
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
		int initial = client.getRecv();
		
		int n = 10000;
		
		long t1 = System.currentTimeMillis();
		
		for (int i=0; i<n; i++){
			client.runCode(guid, field, noop_code, value, 0);
		}
		
		long elapsed = System.currentTimeMillis() - t1;
		System.out.println("It takes "+elapsed+"ms, and the average latency for each operation is "+(elapsed*1000.0/n)+"us");
		System.out.println("The average throughput is "+(n*1000.0/elapsed)*numThread);
		
		assert(client.getRecv()-initial == n):"the number of responses is not the same as the number of requests";
		
		System.out.println("Received "+(client.getRecv()-initial)+" requests. Robustness test succeeds.");
		th1.interrupt();
		th2.interrupt();
		
		client.shutdown();
		return;
	}

}
