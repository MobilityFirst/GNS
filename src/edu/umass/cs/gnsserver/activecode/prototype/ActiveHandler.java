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
import java.util.logging.Level;

import javax.script.ScriptException;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnsserver.activecode.ActiveCodeConfig;
import edu.umass.cs.gnsserver.activecode.ActiveCodeHandler;
import edu.umass.cs.gnsserver.activecode.prototype.blocking.ActiveBlockingClient;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Client;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Runner;
import edu.umass.cs.gnsserver.activecode.prototype.unblocking.ActiveNonBlockingClient;
import edu.umass.cs.gnsserver.interfaces.ActiveDBInterface;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.gnsserver.utils.Util;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.utils.DelayProfiler;

/**
 * This class handles the requests when ActiveCodeHandler decides
 * that it is necessary to run active code.
 * It is responsible for checking whether the code is trusted.
 * 
 * @author gaozy
 *
 */
public class ActiveHandler {
	
	private static Client[] clientPool;
	
	private final static String cfilePrefix = "/tmp/client_";
	private final static String sfilePrefix = "/tmp/server_";
	private final String suffix;
	private final static int clientStartPort = 50000;
	private final static int workerStartPort = 60000;
	
	private final Runner runner;
	
	/**
	 * Test then initialize this variable
	 */
	public boolean pipeEnable = true;
	
	private final int numProcess;
	final AtomicInteger counter = new AtomicInteger();
	
	
	/**
	 * Initialize handler with clients and workers.
	 * @param nodeID 
	 * @param app 
	 * @param numProcess
	 * @param numThread 
	 * @param blocking blocking client or not
	 */
	public ActiveHandler(String nodeID, ActiveDBInterface app, int numProcess, int numThread, boolean blocking){
		this.suffix = nodeID;
		this.numProcess = numProcess;
		
		final String fileTestForPipe = "/tmp/test";
		try {
			Runtime.getRuntime().exec("mkfifo "+fileTestForPipe);			
		} catch (IOException e) {
			pipeEnable = false;
			e.printStackTrace();			
		} finally{
			new File(fileTestForPipe).delete();
		}
		
		//FIXME: initialize Querier here, instead of initialize a query in each client
		runner = new ActiveTrustedRunner(null);
		
		// initialize single clients and workers
		clientPool = new Client[numProcess];
		for (int i=0; i<numProcess; i++){
			if(blocking){
				
				if(pipeEnable){
					clientPool[i] = new ActiveBlockingClient(nodeID, app, cfilePrefix+i+suffix, sfilePrefix+i+suffix, i, numThread);
				}else{
					clientPool[i] = new ActiveBlockingClient(nodeID, app, clientStartPort+i, workerStartPort+i, i, numThread);
				}
			}else{
				
				if(pipeEnable){
					clientPool[i] = new ActiveNonBlockingClient(nodeID, app, cfilePrefix+i+suffix, sfilePrefix+i+suffix, i, numThread);
				} else {
					clientPool[i] = new ActiveNonBlockingClient(nodeID, app, clientStartPort+i, workerStartPort+i, i, numThread);
				}
				new Thread((ActiveNonBlockingClient) clientPool[i]).start();
			}
		}
		ActiveCodeHandler.getLogger().log(Level.INFO, "ActiveHandler has been started with "+numProcess+"("+numThread+" threads) "
				+(blocking?"blocking":"nonblocking")+" worker processes.");
	}
	
	/**
	 * Initialize a handler with multi-process single-threaded workers.
	 * @param nodeId 
	 * @param app
	 * @param numProcess
	 */
	public ActiveHandler(String nodeId, ActiveDBInterface app, int numProcess){
		this(nodeId, app, numProcess, 1, false);
	}
	
	/**
	 * Shutdown all the client and its corresponding workers
	 */
	private void shutdown(){
		for(int i=0; i<numProcess; i++){
			if(clientPool[i] != null){
				clientPool[i].shutdown();
			}
		}
	}
	
	/**
	 * This runCode method is used to check whether we need to send this request to
	 * a worker. If the code is trusted, we could run the request locally without
	 * sending it to the worker.
	 * 
	 * @param header 
	 * @param guid
	 * @param accessor
	 * @param code
	 * @param value
	 * @param ttl
	 * @return executed result
	 * @throws ActiveException 
	 */
	public JSONObject runCode(InternalRequestHeader header, String guid, 
			String accessor, String code, JSONObject value, int ttl) throws ActiveException{
		if(ActiveCodeConfig.activeCodeTrustedMode){
			String result = null;
			try {
				result = runner.runCode(guid, accessor, code, value.toString(), ttl, 0);
			} catch (NoSuchMethodException | ScriptException e) {
				return value;
			}
			if(result != null){
				try {
					return new JSONObject(result);
				} catch (JSONException e) {
					return value;
				}
			}
			return value;
		}
		return clientPool[counter.getAndIncrement()%numProcess].runCode(header, guid, accessor, code, value, ttl, 2000);
	}
	
	/***************** Test methods ****************/	
	/**
	 * @param args
	 * @throws JSONException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws JSONException, InterruptedException, ExecutionException{
		int numProcess = Integer.parseInt(args[0]);
		int numThread = Integer.parseInt(args[1]);
		boolean blocking = Boolean.parseBoolean(args[2]);
		if(numProcess <= 0){
			System.out.println("Number of clients must be larger than 0.");
			System.exit(0);
		}
				
		final ThreadPoolExecutor executor;		
		
		executor = new ThreadPoolExecutor(numProcess*numThread, numProcess*numThread, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());		
		executor.prestartAllCoreThreads();
		
		String guid = "4B48F507395639FD806459281C3C09BCBB16FDFF";
		String field = "someField";
		String noop_code = "";
		try {
			noop_code = new String(Files.readAllBytes(Paths.get("./scripts/activeCode/noop.js")));
		} catch (IOException e) {
			e.printStackTrace();
		} 
		ValuesMap value = new ValuesMap();
		value.put(field, "someValue");
		
		// initialize a handler
		ActiveHandler handler = new ActiveHandler("", null, numProcess, numThread, blocking);
		ArrayList<Future<JSONObject>> tasks = new ArrayList<Future<JSONObject>>();
		
		int n = 1000000;
		
		long t1 = System.currentTimeMillis();
		
		for(int i=0; i<n; i++){
			tasks.add(executor.submit(new ActiveTask(clientPool[i%numProcess], guid, field, noop_code, value, 0)));
		}
		for(Future<JSONObject> task:tasks){
			task.get();
		}
		
		long elapsed = System.currentTimeMillis() - t1;
		System.out.println("It takes "+elapsed+"ms, and the average latency for each operation is "+(elapsed*1000.0/n)+"us");
		System.out.println("The throughput is "+Util.df(n*1000.0/elapsed)+"reqs/sec");
		handler.shutdown();
		
		System.out.println(DelayProfiler.getStats());
		System.exit(0);
	}
	
}
