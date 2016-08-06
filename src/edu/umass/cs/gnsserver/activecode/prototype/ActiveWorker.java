package edu.umass.cs.gnsserver.activecode.prototype;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.script.ScriptException;

import org.json.JSONException;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage.Type;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Channel;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Querier;
import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * @author gaozy
 *
 */
public class ActiveWorker {
	
	
	private final ActiveRunner runner;
	private final Channel channel;
	private final int id;
	
	private Querier querier;
	
	
	/******************* TEST ********************/
	private final ThreadPoolExecutor executor;
	private final ActiveRunner[] runners;
	private static final AtomicInteger counter = new AtomicInteger();
	
	/**
	 * Initialize a worker with a UDP channel
	 * @param port
	 * @param id
	 * @param numThread
	 */
	protected ActiveWorker(int port, int serverPort, int id, int numThread){
		this.id = id;
		
		
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
		
		channel = new ActiveDatagramChannel(port, serverPort);
		querier = new ActiveQuerier(channel);
		runner = new ActiveRunner(querier);
		
		try {
			runWorker(numThread);
		} catch (Exception e){
			e.printStackTrace();
		} finally {
			channel.shutdown();
		}
	}
	
	/**
	 * Initialize a worker with a named pipe
	 * @param ifile
	 * @param ofile
	 * @param id 
	 * @param numThread 
	 * @param isTest
	 */
	protected ActiveWorker(String ifile, String ofile, int id, int numThread, boolean isTest) {
		this.id = id;
		
		//engine = new ScriptEngineManager().getEngineByName("nashorn");
		//invocable = (Invocable) engine;
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
			runner = new ActiveRunner(querier);
			
			try {
				runWorker(numThread);
			} catch (Exception e){
				e.printStackTrace();
			} finally {
				channel.shutdown();
			}
		} else {
			channel = null;
			runner = null;
		}
	}
	
	/**
	 * @param ifile
	 * @param ofile
	 */
	public ActiveWorker(String ifile, String ofile){
		this(ifile, ofile, 0, 1, false);
	}
	
	/**
	 * @param guid
	 * @param field
	 * @param code
	 * @param value
	 * @param ttl
	 * @return ValuesMap result 
	 * @throws Exception 
	 */
	public ValuesMap runCode(String guid, String field, String code, ValuesMap value, int ttl) throws Exception {	
		return runner.runCode(guid, field, code, value, ttl);
	}

	
	private void runWorker(int numThread) throws JSONException, IOException {		
		
		ActiveMessage msg = null;
		while((msg = (ActiveMessage) channel.receiveMessage()) != null){
			if(numThread == 1){
				if(msg.type == Type.REQUEST){
					//System.out.println(this+" receives a request "+msg);
					ActiveMessage response;
					try {
						response = new ActiveMessage(msg.getId(), runCode(msg.getGuid(), msg.getField(), msg.getCode(), msg.getValue(), msg.getTtl()), null);
					} catch (Exception e) {
						response = new ActiveMessage(msg.getId(), null, e.getMessage());
						//e.printStackTrace();
					}				
					channel.sendMessage(response);
				} else if (msg.type == Type.RESPONSE ){
					System.out.println("This is a response message, the execution should not be here!");
				}
			} else{
				// This is a test
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
				
				ActiveMessage response = new ActiveMessage(msg.getId(), value, null);
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
		boolean pipeEnable = Boolean.parseBoolean(args[4]);
		if(pipeEnable){
			String cfile = args[0];
			String sfile = args[1];
			int id = Integer.parseInt(args[2]);
			int copy = Integer.parseInt(args[3]);
			
			new ActiveWorker(cfile, sfile, id, copy, false);
		}
		else {
			int port = Integer.parseInt(args[0]);
			int serverPort = Integer.parseInt(args[1]);
			int id = Integer.parseInt(args[2]);
			int copy = Integer.parseInt(args[3]);
			
			new ActiveWorker(port, serverPort, id, copy);
		}
	}
}
