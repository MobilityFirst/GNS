package edu.umass.cs.gnsserver.activecode.prototype.unblocking;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;

import org.json.JSONException;

import com.maxmind.geoip2.DatabaseReader;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Channel;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Runner;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import jdk.nashorn.api.scripting.NashornScriptEngineFactory;
import jdk.nashorn.api.scripting.ScriptObjectMirror;


public class ActiveNonBlockingRunner implements Runner {
	
	final private ScriptEngine engine;
	final private Invocable invocable;
	
	private final HashMap<String, ScriptContext> contexts = new HashMap<String, ScriptContext>();
	private final HashMap<String, Integer> codeHashes = new HashMap<String, Integer>();
	private final Channel channel;
	private final ConcurrentHashMap<Long, ActiveNonBlockingQuerier> map = new ConcurrentHashMap<Long, ActiveNonBlockingQuerier>();
	private final DatabaseReader dbReader;
	
	// This object is used to serialize/deserialize values passing between Java and Javascript
	private static ScriptObjectMirror JSON;
	

	public ActiveNonBlockingRunner(Channel channel, DatabaseReader dbReader){
		this.channel = channel;
		this.dbReader = dbReader;
		
		// Initialize an script engine without extensions and java
		NashornScriptEngineFactory factory = new NashornScriptEngineFactory();
		engine = factory.getScriptEngine("-strict", "--no-java", "--no-syntax-extensions");		
		try {
			JSON = (ScriptObjectMirror) engine.eval("JSON");
		} catch (ScriptException e) {
			e.printStackTrace();
			throw new RuntimeException("Can not eval JSON");			
		}
		
		invocable = (Invocable) engine;
	}
	

	private synchronized void updateCache(String codeId, String code) throws ScriptException {
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
	

        @Override
	public String runCode(String guid, String accessor, String code, String value, int ttl, long id) throws ScriptException, NoSuchMethodException {		
		ActiveNonBlockingQuerier querier = new ActiveNonBlockingQuerier(channel, dbReader, JSON, ttl, guid, id);
		map.put(id, querier);
		
		updateCache(guid, code);
		engine.setContext(contexts.get(guid));
		
		String result = querier.js2String((ScriptObjectMirror) invocable.invokeFunction("run", querier.string2JS(value),
				accessor, querier));
		
		map.remove(id);
		return result;
	}
	

	public void release(ActiveMessage am){
		
		ActiveNonBlockingQuerier querier = map.get(am.getId());
		if(querier != null){
			// querier might be null as it might be already removed because of timedout task
			querier.release(am, true);
		}
		
	}
	

	

	private static class SimpleTask implements Callable<String>{
		
		ActiveNonBlockingRunner runner;
		ActiveMessage am;
		
		SimpleTask(ActiveNonBlockingRunner runner, ActiveMessage am){
			this.runner = runner;
			this.am = am;
		}
		
		@Override
		public String call() throws Exception {
			return runner.runCode(am.getGuid(), am.getAccessor(), am.getCode(), am.getValue(), am.getTtl(), am.getId());
		}
		
	}
	

	public static void main(String[] args) throws JSONException, InterruptedException, ExecutionException{
		
		int numThread = 10; 		
		final ActiveNonBlockingRunner[] runners = new ActiveNonBlockingRunner[numThread];
		for (int i=0; i<numThread; i++){
			runners[i] = new ActiveNonBlockingRunner(null, null);
		}
		
		final ThreadPoolExecutor executor = new ThreadPoolExecutor(numThread, numThread, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
		executor.prestartAllCoreThreads();
		
		ArrayList<Future<String>> tasks = new ArrayList<Future<String>>();
		
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
		
		ActiveMessage msg = new ActiveMessage(guid, field, noop_code, value.toString(), 0, 500);
		int n = 1000000;
		
		long t1 = System.currentTimeMillis();
		
		for(int i=0; i<n; i++){
			tasks.add(executor.submit(new SimpleTask(runners[0], msg)));
		}
		for(Future<String> task:tasks){
			task.get();
		}
		
		long elapsed = System.currentTimeMillis() - t1;
		System.out.println("It takes "+elapsed+"ms, and the average latency for each operation is "+(elapsed*1000.0/n)+"us");
		System.out.println("The throughput is "+n*1000.0/elapsed);
		
		
		

		ActiveNonBlockingRunner runner = new ActiveNonBlockingRunner(null, null);
		String chain_code = null;
		try {
			//chain_code = new String(Files.readAllBytes(Paths.get("./scripts/activeCode/permissionTest.js")));
			chain_code = new String(Files.readAllBytes(Paths.get("./scripts/activeCode/permissionTest.js")));
			//chain_code = new String(Files.readAllBytes(Paths.get("./scripts/activeCode/testLoad.js")));
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			runner.runCode(guid, field, chain_code, value.toString(), 0, 0);			
			// fail here
			assert(false):"The code should not be here";
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
		System.exit(0);
	}
}
