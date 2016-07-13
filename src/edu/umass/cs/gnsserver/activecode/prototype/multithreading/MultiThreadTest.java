package edu.umass.cs.gnsserver.activecode.prototype.multithreading;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveException;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Client;
import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * @author gaozy
 *
 */
public class MultiThreadTest {
	
	private static final int total = 1000000;	
	private static ThreadPoolExecutor executor;
	
	static class SimpleTask implements Callable<ValuesMap>{
		
		private Client client;
		private String guid;
		private String field;
		private String code;
		private ValuesMap value;
		
		SimpleTask(Client client, String guid, String field, String code, ValuesMap value){
			this.client = client;
			this.guid = guid;
			this.field = field;
			this.code = code;
			this.value = value;
		}
		

		@Override
		public ValuesMap call() throws Exception {
			ValuesMap result = null;
			try {
				result = client.runCode(guid, field, code, value, 0);
			} catch (ActiveException e) {
				e.printStackTrace();
			}
			return result;
		}
		
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args){
		int num = 10;
		
		executor = new ThreadPoolExecutor(num, num, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
		executor.prestartAllCoreThreads();
		
		int numThread = 2; //Integer.parseInt(args[0]);
		String cfile = "/tmp/client";
		String sfile = "/tmp/server";
		int id = 0;
		
		MultiThreadActiveClient client = new MultiThreadActiveClient(null, numThread, cfile, sfile, id);
		
		new Thread(client).start();
		
		
		try{
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
			
			ArrayList<Future<ValuesMap>> futures = new ArrayList<Future<ValuesMap>>();
			
			long t = System.currentTimeMillis();
			for(int i=0; i<total; i++){
				//client.sendRequest(guid, field, noop_code, value, 0);
				//client.runCode(guid, field, noop_code, value, 0);
				futures.add(executor.submit(new SimpleTask(client, guid, field, noop_code, value)));
			}
			
			for (Future<ValuesMap> future:futures){
				future.get();
			}
			long elapsed = System.currentTimeMillis() - t;
			System.out.println("It takes "+elapsed+"ms, and the average latency for each operation is "+(elapsed*1000.0/total)+"us");
			System.out.println("The average throughput is "+(total*1000.0/elapsed));
			
		}catch(Exception e){
			e.printStackTrace();
		} finally {		
			client.shutdown();
		}
		
		System.exit(0);
	}
}
