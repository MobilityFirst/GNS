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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.json.JSONException;
import org.junit.Test;

import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * @author gaozy
 *
 */
public class ActiveClientThroughputTest {
	
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
			return client.runCode(null, guid, field, code, value, ttl, 1000);
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
	public void test_sequentialRequestThroughput() throws JSONException, ActiveException, InterruptedException, ExecutionException {
		
		final String suffix = "";

		int numThread = 8;
		if(System.getProperty("numThread")!=null){
			numThread = Integer.parseInt(System.getProperty("numThread"));
		}
		
		String cfile = "/tmp/client"+suffix;
		String sfile = "/tmp/server"+suffix;		
		/**
		 * Test client performance with named pipe channel
		 */
		ActiveClientWithNamedPipe client = new ActiveClientWithNamedPipe(null, cfile, sfile, 0, numThread, 1024);
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
		value.put(field, "hello world!");	
		ValuesMap result = client.runCode(null, guid, field, noop_code, value, 0, 10000);

		assertEquals(result.toString(), value.toString());
		
		ExecutorService executor = new ThreadPoolExecutor(numThread, numThread, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
		List<Future<ValuesMap>> tasks = new ArrayList<Future<ValuesMap>>();
		
		int initial = client.getRecv();
		
		int n = 1000000;
		
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
		System.out.println("The average throughput is "+(n*1000.0/elapsed));
		
		assert(client.getRecv()-initial == n):"the number of responses is not the same as the number of requests";
		
		System.out.println("Throughput test succeeds.");
		
		th.interrupt();
		
		client.shutdown();
		
	}

}
