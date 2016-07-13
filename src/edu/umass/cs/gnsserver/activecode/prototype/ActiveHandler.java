package edu.umass.cs.gnsserver.activecode.prototype;

import java.io.File;
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

import edu.umass.cs.gnsserver.activecode.ActiveCodeHandler;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Client;
import edu.umass.cs.gnsserver.activecode.prototype.multithreading.MultiThreadActiveClient;
import edu.umass.cs.gnsserver.interfaces.ActiveDBInterface;
import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * @author gaozy
 *
 */
public class ActiveHandler {
	
	private final Client[] clientPool;
	
	private final static String cfilePrefix = "/tmp/client_";
	private final static String sfilePrefix = "/tmp/server_";
	private final static String suffix = "_pipe";
	private final static int clientPort = 50000;
	private final static int workerPort = 60000;
	
	/**
	 * Test then initialize this variable
	 */
	public boolean pipeEnable = true;
	
	private final int numProcess;
	final AtomicInteger counter = new AtomicInteger();
	
	/****************** Test only ******************/
	private final ThreadPoolExecutor executor;
	
	/**
	 * Initialize handler with multi-process multi-threaded workers.
	 * @param app 
	 * @param numProcess
	 * @param numThread 
	 */
	public ActiveHandler(ActiveDBInterface app, int numProcess, int numThread){
		final String fileTestForPipe = "/tmp/test";
		try {
			Runtime.getRuntime().exec("mkfifo "+fileTestForPipe);
			new File(fileTestForPipe).delete();
		} catch (IOException e) {
			pipeEnable = false;
			e.printStackTrace();			
		}
		
		System.out.println(ActiveCodeHandler.class.getName()+" start running "+numProcess+" workers with "+numThread+" threads ...");
		
		this.numProcess = numProcess;
		if(numThread == 1){
			executor = new ThreadPoolExecutor(numProcess, numProcess, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
		}else{
			executor = new ThreadPoolExecutor(10*numProcess, 10*numProcess, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
		}
		executor.prestartAllCoreThreads();
		
		if(numThread == 1){
			// initialize single threaded clients and workers
			clientPool = new ActiveClient[numProcess];
			for (int i=0; i<numProcess; i++){
				if(pipeEnable){
					clientPool[i] = new ActiveClient(app, cfilePrefix+i+suffix, sfilePrefix+i+suffix, i, 1);
				} else {
					clientPool[i] = new ActiveClient(app, clientPort+i, workerPort+i, i, 1);
				}
			}
		} else {
			// initialize multi-threaded clients and workers
			clientPool = new MultiThreadActiveClient[numProcess];
			for (int i=0; i<numProcess; i++){
				if(pipeEnable){
					clientPool[i] = new MultiThreadActiveClient(app, numThread, cfilePrefix+i+suffix, sfilePrefix+i+suffix, i);
					new Thread((MultiThreadActiveClient) clientPool[i]).start();
				} else {
					clientPool[i] = new MultiThreadActiveClient(app, numThread, clientPort+i, workerPort+i, i);
					new Thread((MultiThreadActiveClient) clientPool[i]).start();
				}
			}
		}
	}
	
	/**
	 * Initialize a handler with multi-process single-threaded workers.
	 * @param app
	 * @param numProcess
	 */
	public ActiveHandler(ActiveDBInterface app, int numProcess){
		this(app, numProcess, 1);
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
	 * @throws ActiveException 
	 */
	public ValuesMap runCode(String guid, String field, String code, ValuesMap value, int ttl) throws ActiveException{
		return clientPool[counter.getAndIncrement()%numProcess].runCode(guid, field, code, value, ttl);
	}
	
	/***************** Test methods ****************/
	private Future<ValuesMap> submitTask(String guid, String field, String code, ValuesMap value, int ttl){
		Client client = clientPool[ttl%numProcess];
		return executor.submit(new ActiveTask(client, guid, field, code, value, ttl));
	}
	
	
	
	/**
	 * @param args
	 * @throws JSONException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws JSONException, InterruptedException, ExecutionException{
		int numProcess = Integer.parseInt(args[0]);
		int numThread = Integer.parseInt(args[1]);
		if(numProcess <= 0){
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
		
		// initialize a multithreaded client
		ActiveHandler handler = new ActiveHandler(null, numProcess, numThread);
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
