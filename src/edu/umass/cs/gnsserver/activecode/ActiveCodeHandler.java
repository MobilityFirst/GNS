/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Misha Badov, Westy
 *
 */
package edu.umass.cs.gnsserver.activecode;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.binary.Base64;

import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeParams;
import edu.umass.cs.gnsserver.exceptions.FieldNotFoundException;
import edu.umass.cs.gnsserver.gnsApp.AppReconfigurableNodeOptions;
import edu.umass.cs.gnsserver.gnsApp.GnsApplicationInterface;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.ActiveCode;
import edu.umass.cs.gnsserver.gnsApp.recordmap.NameRecord;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.utils.DelayProfiler;

/**
 * This class is the entry of activecode, it provides
 * the interface for GNS to run active code. It's creates
 * a threadpool to connect the real isolated active worker 
 * to run active code. It also handles the misbehaviours.
 * 
 * @author Zhaoyu Gao
 */
public class ActiveCodeHandler {	
	private GnsApplicationInterface<?> gnsApp;
	private static ClientPool clientPool;
	private static ThreadFactory threadFactory;
	private static ActiveCodeExecutor executorPool;
	private static ActiveCodeGuardian guard;
	private static ActiveCodeScheduler scheduler;
	
	
	/**
	 * Initializes an ActiveCodeHandler
	 * @param app
	 */
	public ActiveCodeHandler(GnsApplicationInterface<String> app) {
		int numProcesses = AppReconfigurableNodeOptions.activeCodeWorkerCount;
		this.setGnsApp(app);
	    
		clientPool = new ClientPool(app); 
		(new Thread(clientPool)).start();
		
		
		guard = new ActiveCodeGuardian(clientPool);
		if (AppReconfigurableNodeOptions.activeCodeEnableTimeout){
			(new Thread(guard)).start();
		}
		
	    // Get the ThreadFactory implementation to use
	    threadFactory = new ActiveCodeThreadFactory(clientPool);
	    // Create the ThreadPoolExecutor
	    executorPool = new ActiveCodeExecutor(numProcesses, numProcesses, 0, TimeUnit.SECONDS, 
	    		new LinkedBlockingQueue<Runnable>(),
	    		threadFactory, 
	    		new ThreadPoolExecutor.DiscardPolicy(),
	    		guard,
	    		clientPool);
	    
	    // Start the processes
	    int numThread = executorPool.prestartAllCoreThreads();
	    System.out.println("##################### "+numThread+" threads have been started! ##################");
	    //System.out.println("There are "+executorPool.getActiveCount() + " threads running, and " + executorPool.getPoolSize()+" threads available.");
	    
	    scheduler = new ActiveCodeScheduler(executorPool);
	    (new Thread(scheduler)).start();
	    
	    clientPool.startSpareWorkers();
	}
	
	
	/**
	 * Checks to see if this guid has active code for the specified action.
	 * @param nameRecord
	 * @param action can be 'read' or 'write'
	 * @return whether or not there is active code
	 */
	public static boolean hasCode(NameRecord nameRecord, String action) {
		try {
            return nameRecord.getValuesMap().has(ActiveCode.codeField(action));
		} catch (FieldNotFoundException e) {
			return false;
		}
	}
	
	
	/**
	 * Runs the active code. Returns a {@link ValuesMap}.
     * 
	 * @param code64 base64 encoded active code, as stored in the db
	 * @param guid the guid
	 * @param field the field
	 * @param action either 'read' or 'write'
         * @param valuesMap
	 * @param activeCodeTTL the remaining active code TTL
	 * @return a Valuesmap
	 */
	public static ValuesMap runCode(String code64, String guid, String field, String action, ValuesMap valuesMap, int activeCodeTTL) {
		long startTime = System.nanoTime();		
		//Construct Value parameters
		String code = new String(Base64.decodeBase64(code64));
		String values = valuesMap.toString();
		
		//System.out.println("Got the request from guid "+guid+" for the field "+field+" with original value "+valuesMap);
		
		ActiveCodeParams acp = new ActiveCodeParams(guid, field, action, code, values, activeCodeTTL);
		ActiveCodeFutureTask futureTask = new ActiveCodeFutureTask(new ActiveCodeTask(acp));
		
		DelayProfiler.updateDelayNano("activeHandlerPrepare", startTime);		
		
		scheduler.submit(futureTask, guid);
		
		ValuesMap result = null;
		
		try {
			result = futureTask.get();
		} catch (ExecutionException ee) {
			//System.out.println("Execution "+guid+" Task "+futureTask);
			//ee.printStackTrace();
			scheduler.finish(guid);
			return valuesMap;
		} catch(CancellationException ce) {
			//System.out.println("Cancel "+guid+" Task "+futureTask);
			//ce.printStackTrace();
			scheduler.finish(guid);
			return valuesMap;
		} catch(InterruptedException ie) {
			System.out.println("Interrupt "+guid+" Task "+futureTask+" thread "+Thread.currentThread());			
			guard.cancelTask(futureTask);
			scheduler.finish(guid);
			return valuesMap;
		} catch (Exception e){
			e.printStackTrace();
		}
		
		scheduler.finish(guid);
		
		DelayProfiler.updateDelayNano("activeHandler", startTime);
		
	    return result;
		
	}


	/**
	 * @return gnsApp
	 */
	public GnsApplicationInterface<?> getGnsApp() {
		return gnsApp;
	}


	/**
	 * @param gnsApp
	 */
	public void setGnsApp(GnsApplicationInterface<?> gnsApp) {
		this.gnsApp = gnsApp;
	}
	
	static class Task implements Runnable {
		public void run() {
			System.out.println("Task running");
			throw new RuntimeException("TaskException " + System.currentTimeMillis());
		}
	}
	
	/**
	 * @param args
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(4);
		Future<?> future  = executor.submit(new Task());
		Thread.sleep(1000);
		try {
			future.get();
		} catch(ExecutionException re) {
			re.printStackTrace();
			future.get();
		}
	}
}
