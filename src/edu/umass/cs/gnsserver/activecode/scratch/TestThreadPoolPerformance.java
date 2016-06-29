package edu.umass.cs.gnsserver.activecode.scratch;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.json.JSONException;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage;
import edu.umass.cs.gnsserver.utils.ValuesMap;

public class TestThreadPoolPerformance {
	
	private static class SimpleTask implements Callable<ValuesMap> {
		
		ActiveMessage am;
		SimpleTask(ActiveMessage am){
			this.am = am;
		}
		
		@Override
		public ValuesMap call() throws Exception {
			return am.getValue();
		}
		
	}
	
	/**
	 * @param args
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws JSONException
	 */
	public static void main(String[] args) throws InterruptedException, ExecutionException, JSONException{
		
		int numThread = 10; //Integer.parseInt(args[0]);
		
		String guid = "zhaoyu";
		String field = "gao";
		String noop_code = "";
		try {
			noop_code = new String(Files.readAllBytes(Paths.get("./scripts/activeCode/noop.js")));
		} catch (IOException e) {
			e.printStackTrace();
		} 
		ValuesMap value = new ValuesMap();
		value.put("string", "hello world");
		ActiveMessage am = new ActiveMessage(guid, field, noop_code, value, 0);
		
		ThreadPoolExecutor executor = new ThreadPoolExecutor(numThread, numThread, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
		executor.prestartAllCoreThreads();
		
		int n = 1000000;
		long t = System.currentTimeMillis();
		for(int i=0; i<n; i++){
			Future<ValuesMap> future = executor.submit(new SimpleTask(am));
			future.get();
		}
		long elapsed = System.currentTimeMillis() - t;
		System.out.println("It takes "+elapsed+"ms for submitting task to a thread pool and executing it, and the average latency for each operation is "+(elapsed*1000.0/n)+"us");	

		executor.shutdown();
	}
}
