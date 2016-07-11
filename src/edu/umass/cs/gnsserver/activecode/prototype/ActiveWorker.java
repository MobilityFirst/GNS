package edu.umass.cs.gnsserver.activecode.prototype;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;

import org.json.JSONException;

import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Channel;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Querier;
import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * @author gaozy
 *
 */
public class ActiveWorker {
		
	private ScriptEngine engine;
	private Invocable invocable;
	
	private final HashMap<String, ScriptContext> contexts = new HashMap<String, ScriptContext>();
	private final HashMap<String, Integer> codeHashes = new HashMap<String, Integer>();
	
	
	
	private final Channel channel;
	private final String ifile;
	private final String ofile;
	private final int id;

	
	private Querier querier;
	
	/**
	 * bufferSize for all byte buffer
	 */
	//public final static int bufferSize = 1024*4;
	
	/******************* TEST ********************/
	private final ThreadPoolExecutor executor;
	private final ActiveRunner[] runners;
	private static final AtomicInteger counter = new AtomicInteger();
	
	/**
	 * @param ifile
	 * @param ofile
	 * @param id 
	 * @param numThread 
	 * @param isTest
	 */
	public ActiveWorker(String ifile, String ofile, int id, int numThread, boolean isTest) {		
		this.ifile = ifile;
		this.ofile = ofile;
		this.id = id;
		
		engine = new ScriptEngineManager().getEngineByName("nashorn");
		invocable = (Invocable) engine;
		if(numThread>1){
			executor = new ThreadPoolExecutor(numThread, numThread, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());	
			executor.prestartAllCoreThreads();
			runners = new ActiveRunner[numThread];
			for (int i=0; i<numThread; i++){
				runners[i] = new ActiveRunner(null);
			}
		}else{
			executor = null;
			runners = null;
		}
		
		if(!isTest){
			channel = new ActiveNamedPipe(ifile, ofile);
			querier = new ActiveQuerier(channel);
			
			try {
				runWorker(numThread);
			} catch (Exception e){
				e.printStackTrace();
			} finally {
				channel.shutdown();
			}
		} else {
			channel = null;
		}
	}
	
	/**
	 * @param ifile
	 * @param ofile
	 */
	public ActiveWorker(String ifile, String ofile){
		this(ifile, ofile, 0, 1, false);
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
	 * @return ValuesMap result 
	 * @throws ScriptException
	 * @throws NoSuchMethodException
	 */
	public ValuesMap runCode(String guid, String field, String code, ValuesMap value, int ttl) throws ScriptException, NoSuchMethodException {		
		updateCache(guid, code);
		engine.setContext(contexts.get(guid));
		((ActiveQuerier) querier).resetQuerier(guid, ttl);
		return (ValuesMap) invocable.invokeFunction("run", value, field, querier);
	}

	
	private void runWorker(int numThread) throws JSONException, IOException {
		System.out.println("Start running "+this+" by listening on "+ifile+", and write to "+ofile);
		
		ActiveMessage msg = null;
		while((msg = (ActiveMessage) channel.receiveMessage()) != null){
			if(numThread == 1){
				ActiveMessage response;
				try {
					response = new ActiveMessage(runCode(msg.getGuid(), msg.getField(), msg.getCode(), msg.getValue(), msg.getTtl()), null);
				} catch (NoSuchMethodException | ScriptException e) {
					response = new ActiveMessage(null, e.getMessage());
					e.printStackTrace();
				}				
				channel.sendMessage(response);
			} else{
				ValuesMap value = null;
				ArrayList<Future<ValuesMap>> tasks = new ArrayList<Future<ValuesMap>>();
				tasks.add(executor.submit(new SimpleTask(runners[counter.getAndIncrement()%numThread], msg)));
				for (Future<ValuesMap> task:tasks){
					try {
						value = task.get();
					} catch (InterruptedException | ExecutionException e) {
						e.printStackTrace();
					}
				}
				
				ActiveMessage response = new ActiveMessage(value, null);
				channel.sendMessage(response);
			}
		}
	}
	
	
	private static class SimpleTask implements Callable<ValuesMap>{
		private ActiveRunner runner;
		private ActiveMessage am;
		
		private SimpleTask(ActiveRunner runner, ActiveMessage am){
			this.runner = runner;
			this.am = am;
		}
		
		@Override
		public ValuesMap call() throws Exception {
			return runner.runCode(am.getGuid(), am.getField(), am.getCode(), am.getValue(), am.getTtl());
		}
		
	}
	
	public String toString(){
		return this.getClass().getSimpleName()+id;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args){
		String cfile = args[0];
		String sfile = args[1];
		int id = Integer.parseInt(args[2]);
		int copy = Integer.parseInt(args[3]);
		
		new ActiveWorker(cfile, sfile, id, copy, false);
		
	}
}
