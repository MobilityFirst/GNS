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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.binary.Base64;

import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeParams;
import edu.umass.cs.gnsserver.exceptions.FieldNotFoundException;
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
	GnsApplicationInterface<?> gnsApp;
	ClientPool clientPool;
	ThreadFactory threadFactory;
	ActiveCodeExecutor executorPool;
	ActiveCodeGuardian guard;
	ActiveCodeScheduler scheduler;
	
	ConcurrentHashMap<String, Long> blacklist;
	long blacklistSeconds;
	
	protected static final long MILLISECONDS_PER_SEC = 1000;
	
	/**
	 * Initializes an ActiveCodeHandler
	 * @param app
	 * @param numProcesses
	 * @param blacklistSeconds
	 */
	public ActiveCodeHandler(GnsApplicationInterface<?> app, int numProcesses, long blacklistSeconds) {
		gnsApp = app;			    		
	    
		clientPool = new ClientPool(app, this); 
		
		guard = new ActiveCodeGuardian(clientPool);
	    (new Thread(guard)).start();
		
	    // Get the ThreadFactory implementation to use
	    threadFactory = new ActiveCodeThreadFactory(clientPool);
	    // Create the ThreadPoolExecutor
	    executorPool = new ActiveCodeExecutor(numProcesses, numProcesses, 0, TimeUnit.SECONDS, 
	    		new LinkedBlockingQueue<Runnable>(), 
	    		//new SynchronousQueue<Runnable>(),
	    		threadFactory, new ThreadPoolExecutor.DiscardPolicy(),
	    		guard);
	    // Start the processes
	    executorPool.prestartAllCoreThreads();
	    System.out.println("##################### All threads have been started! ##################");
	    //System.out.println("There are "+executorPool.getActiveCount() + " threads running, and " + executorPool.getPoolSize()+" threads available.");
	    
	    scheduler = new ActiveCodeScheduler(executorPool);
	    (new Thread(scheduler)).start();
	    // Blacklist init
		blacklist = new ConcurrentHashMap<String, Long>();
		this.blacklistSeconds = blacklistSeconds;
	}
	
	
	/**
	 * Checks to see if this guid has active code for the specified action.
	 * @param nameRecord
	 * @param action can be 'read' or 'write'
	 * @return whether or not there is active code
	 */
	public boolean hasCode(NameRecord nameRecord, String action) {
		try {
            return nameRecord.getValuesMap().has(ActiveCode.codeField(action));
			//ResultValue code = nameRecord.getKeyAsArray(ActiveCode.codeField(action));
			//return code != null && !code.isEmpty();
		} catch (FieldNotFoundException e) {
			return false;
		}
	}
	
	/**
	 * Checks to see if all of the workers are busy.
	 * @return true if all workers are busy
	 */
	private boolean isPoolBusy() {
		return executorPool.getActiveCount() == executorPool.getMaximumPoolSize();
	}
	
	/**
	 * Checks to see if the guid is currently blacklisted.
	 * @param guid
	 * @return true if the guid is blacklisted
	 */
	private boolean isBlacklisted(String guid) {
		if(!blacklist.containsKey(guid))
			return false;
		
		long blacklistedAt = blacklist.get(guid);
		//System.out.println(guid+"'s elapsed time is "+(System.currentTimeMillis() - blacklistedAt));
		return (System.currentTimeMillis() - blacklistedAt) < (blacklistSeconds * MILLISECONDS_PER_SEC);
	}
	
	/**
	 * Adds the guid to the blacklist
	 * @param guid
	 */
	private void addToBlacklist(String guid) {
		System.out.println("Guid " + guid + " is blacklisted from running code!");
		scheduler.remove(guid);
		blacklist.put(guid, System.currentTimeMillis());
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
	public ValuesMap runCode(String code64, String guid, String field, String action, ValuesMap valuesMap, int activeCodeTTL) {		
		//System.out.println("Original value is "+valuesMap);
		long startTime = System.nanoTime();
		// If the guid is blacklisted, just return immediately
		/*
		if(isBlacklisted(guid)) {			
			return valuesMap;
		}		
		*/
		
		//Construct Value parameters
		String code = new String(Base64.decodeBase64(code64));
		String values = valuesMap.toString();
		ActiveCodeParams acp = new ActiveCodeParams(guid, field, action, code, values, activeCodeTTL);
		FutureTask<ValuesMap> futureTask = new FutureTask<ValuesMap>(new ActiveCodeTask(acp, clientPool));
		
		DelayProfiler.updateDelayNano("activeHandlerPrepare", startTime);		
		
		scheduler.submit(futureTask, guid);
		
		ValuesMap result = null;
		
		try {
			result = futureTask.get();
		} catch (ExecutionException e) {
			//System.out.println("Added " + guid + " to blacklist!");
			//addToBlacklist(guid);
			System.out.println("Execution");
			scheduler.finish(guid);
			return valuesMap;
		} catch (CancellationException e){
			//addToBlacklist(guid);			
			System.out.println("Cancellation thread "+Thread.currentThread().getId());
			scheduler.finish(guid);
			return valuesMap;
		} catch (Exception e){
			System.out.println("Other");
			scheduler.finish(guid);
			return valuesMap;
		}
		
		scheduler.finish(guid);
		
		DelayProfiler.updateDelayNano("activeHandler", startTime);
		
		/*
		if(executorPool.getPoolSize() > 0 &&
				executorPool.getQueue().remainingCapacity() > 0) {
			executorPool.execute(futureTask);
			try {
				result = futureTask.get();
			} catch (ExecutionException e) {
				System.out.println("Added " + guid + " to blacklist!");
				addToBlacklist(guid);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} else {
			System.out.println("Rejecting task!");
		}
		
		if(result == null){
			return valuesMap;
		}
		*/
		
	    return result;
	}
}
