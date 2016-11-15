package edu.umass.cs.gnsserver.activecode.prototype.unblockingworker;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;

import org.json.JSONException;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage;
import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * @author gaozy
 *
 */
public class ActiveRunner {
	
	private ScriptEngine engine;
	private Invocable invocable;
	
	private final HashMap<String, ScriptContext> contexts = new HashMap<String, ScriptContext>();
	private final HashMap<String, Integer> codeHashes = new HashMap<String, Integer>();
	
	private ActiveQuerier querier;
	
	/**
	 * @param querier
	 */
	public ActiveRunner(ActiveQuerier querier){
		this.querier = querier;
		
		engine = new ScriptEngineManager().getEngineByName("nashorn");
		
		invocable = (Invocable) engine;
	}
	
	private void updateCache(String codeId, String code) throws ScriptException {
	    if (!contexts.containsKey(codeId)) {
	      // Create a context if one does not yet exist and eval the code
	      ScriptContext sc = new SimpleScriptContext();
	      contexts.put(codeId, sc);
	      codeHashes.put(codeId, code.hashCode());
	      engine.eval(code, sc);
	    } else if ( codeHashes.get(codeId) != code.hashCode()) {
	      // The context exists, but we need to eval the new code
	      ScriptContext sc = contexts.get(codeId);
	      codeHashes.put(codeId, code.hashCode());
	      engine.eval(code, sc);
	    }
	}
	
	/**
	 * @param guid
	 * @param field
	 * @param code
	 * @param value
	 * @param ttl
	 * @param id 
	 * @return ValuesMap result 
	 * @throws ScriptException
	 * @throws NoSuchMethodException
	 */
	public synchronized ValuesMap runCode(String guid, String field, String code, ValuesMap value, int ttl, long id) throws ScriptException, NoSuchMethodException {		
		updateCache(guid, code);
		engine.setContext(contexts.get(guid));
		if(querier != null) querier.resetQuerier(guid, ttl, id);
		ValuesMap valuesMap;
		
		valuesMap = (ValuesMap) invocable.invokeFunction("run", value, field, querier);
		
		return valuesMap;
	}
	
  /**
   *
   * @param am
   */
  protected void release(ActiveMessage am){
		querier.release(am, true);
	}
	
	private static class SimpleTask implements Callable<ValuesMap>{
		
		ActiveRunner runner;
		ActiveMessage am;
		
		SimpleTask(ActiveRunner runner, ActiveMessage am){
			this.runner = runner;
			this.am = am;
		}
		
		@Override
		public ValuesMap call() throws Exception {
			return runner.runCode(am.getGuid(), am.getField(), am.getCode(), am.getValue(), am.getTtl(), am.getId());
		}
		
	}
	
	/**
	 * Test throughput with multithread worker with multiple script engine
	 * @param args
	 * @throws JSONException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws JSONException, InterruptedException, ExecutionException{
		
		int numThread = 10; 		
		final ActiveRunner[] runners = new ActiveRunner[numThread];
		for (int i=0; i<numThread; i++){
			runners[i] = new ActiveRunner(null);
		}
		
		final ThreadPoolExecutor executor = new ThreadPoolExecutor(numThread, numThread, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
		executor.prestartAllCoreThreads();
		
		ArrayList<Future<ValuesMap>> tasks = new ArrayList<Future<ValuesMap>>();
		
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
		
		ActiveMessage msg = new ActiveMessage(guid, field, noop_code, value, 0, 500);
		int n = 1000000;
		
		long t1 = System.currentTimeMillis();
		
		for(int i=0; i<n; i++){
			tasks.add(executor.submit(new SimpleTask(runners[0], msg)));
		}
		for(Future<ValuesMap> task:tasks){
			task.get();
		}
		
		long elapsed = System.currentTimeMillis() - t1;
		System.out.println("It takes "+elapsed+"ms, and the average latency for each operation is "+(elapsed*1000.0/n)+"us");
		System.out.println("The throughput is "+n*1000.0/elapsed);
		
		
		
		/**
		 * Test runner's protected method
		 */
		ActiveRunner runner = new ActiveRunner(new ActiveQuerier(null));
		String chain_code = null;
		try {
			//chain_code = new String(Files.readAllBytes(Paths.get("./scripts/activeCode/permissionTest.js")));
			chain_code = new String(Files.readAllBytes(Paths.get("./scripts/activeCode/mal.js")));
			//chain_code = new String(Files.readAllBytes(Paths.get("./scripts/activeCode/testLoad.js")));
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			runner.runCode(guid, field, chain_code, value, 0, 0);			
			// fail here
			assert(false):"The code should not be here";
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
		System.exit(0);
	}
}
