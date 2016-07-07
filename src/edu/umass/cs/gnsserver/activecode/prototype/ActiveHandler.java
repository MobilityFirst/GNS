package edu.umass.cs.gnsserver.activecode.prototype;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.json.JSONException;

import edu.umass.cs.gnsserver.interfaces.ActiveDBInterface;
import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * @author gaozy
 *
 */
public class ActiveHandler {
	
	private ActiveClient[] clientPool;
	
	private final String cfilePrefix = "/tmp/client_";
	private final String sfilePrefix = "/tmp/server_";
	private final String suffix = "_pipe";
	
	private final int numProcess;
	final AtomicInteger counter = new AtomicInteger();
	
	/****************** Test only ******************/
	private final ThreadPoolExecutor executor;
	
	/**
	 * @param app 
	 * @param numProcess
	 */
	public ActiveHandler(ActiveDBInterface app, int numProcess){
		
		this.numProcess = numProcess;
		executor = new ThreadPoolExecutor(numProcess, numProcess, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
		executor.prestartAllCoreThreads();
		
		clientPool = new ActiveClient[numProcess];
		for (int i=0; i<numProcess; i++){
			clientPool[i] = new ActiveClient(app, cfilePrefix+i+suffix, sfilePrefix+i+suffix, i, 1);
		}
		
	}
	
	
	/**
	 * Shutdown all the client and its corresponding workers
	 */
	public void shutdown(){
		for(int i=0; i<numProcess; i++){
			if(clientPool[i] != null){
				clientPool[i].shutdown();
			}
		}
	}
	
	/**
	 * @param guid
	 * @param field
	 * @param code
	 * @param value
	 * @param ttl
	 * @return executed result
	 */
	public ValuesMap runCode(String guid, String field, String code, ValuesMap value, int ttl){
		return clientPool[counter.getAndIncrement()%numProcess].runCode(guid, field, code, value, ttl);
	}
	
	/***************** Test methods ****************/
	private Future<ValuesMap> submitTask(String guid, String field, String code, ValuesMap value, int ttl){
		ActiveClient client = clientPool[ttl%numProcess];
		return executor.submit(new ActiveTask(client, guid, field, code, value, ttl));
	}
	
	
	
	/**
	 * @param args
	 * @throws JSONException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws JSONException, InterruptedException, ExecutionException{
		int num = Integer.parseInt(args[0]);
		if(num <= 0){
			System.out.println("Number of clients must be larger than 0.");
			System.exit(0);
		}
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
		
		ActiveHandler handler = new ActiveHandler(null, num);
		ArrayList<Future<ValuesMap>> tasks = new ArrayList<Future<ValuesMap>>();
		
		int n = 1000000;
		
		long t1 = System.currentTimeMillis();
		
		for(int i=0; i<n; i++){
			tasks.add(handler.submitTask(guid, field+i, noop_code, value, i));
		}
		for(Future<ValuesMap> task:tasks){
			task.get();
		}
		
		long elapsed = System.currentTimeMillis() - t1;
		System.out.println("It takes "+elapsed+"ms, and the average latency for each operation is "+(elapsed*1000.0/n)+"us");
		System.out.println("The throughput is "+n*1000.0/elapsed);
		handler.shutdown();
		
		System.exit(0);
	}
	
}
