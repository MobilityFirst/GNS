package edu.umass.cs.gnsserver.activecode.prototype;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.json.JSONException;
import org.junit.Test;

import edu.umass.cs.gnsserver.utils.ValuesMap;

public class ActiveClientRobustnessTest {
	/**
	 * This manually crashes the worker to test the worker
	 * recovery property.
	 * @throws InterruptedException 
	 * @throws JSONException 
	 * @throws ActiveException 
	 */
	@Test
	public void test_robustness() throws InterruptedException, JSONException, ActiveException{
		final String suffix = "";
		final int numThread = 4;
		
		// initialize the client		
		String cfile = "/tmp/client"+suffix;
		String sfile = "/tmp/server"+suffix;		
		ActiveNonBlockingClient client = new ActiveNonBlockingClient(null, cfile, sfile, 0, numThread, 128);
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
			client.runCode(null, guid, field, noop_code, value, 0, 2000);
		}
		
		long elapsed = System.currentTimeMillis() - t1;
		System.out.println("It takes "+elapsed+"ms, and the average latency for each operation is "+(elapsed*1000.0/n)+"us");
		System.out.println("The average throughput is "+(n*1000.0/elapsed)*numThread);
		
		assert(client.getRecv()-initial == n):"the number of responses is not the same as the number of requests";
		
		System.out.println("Received "+(client.getRecv()-initial)+" requests. Robustness test succeeds.");
		
		Thread.sleep(1000);
		
		
		
		th1.interrupt();
		client.shutdown();
	}

}
