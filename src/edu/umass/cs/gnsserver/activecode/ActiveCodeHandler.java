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


import java.util.HashMap;
import java.util.Map;
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
	//ThreadPoolExecutor executorPool;
	ActiveCodeExecutor executorPool;
	ActiveCodeGuardian guard;
	ActiveCodeScheduler scheduler;
	ConcurrentHashMap<Runnable, Thread> threadMap;
	Map<String, Long> blacklist;
	long blacklistSeconds;
	
	protected static final int NANOSECONDS_PER_SEC = 1000000000;
	
	/**
	 * Initializes an ActiveCodeHandler
	 * @param app
	 * @param numProcesses
	 * @param blacklistSeconds
	 */
	public ActiveCodeHandler(GnsApplicationInterface<?> app, int numProcesses, long blacklistSeconds) {
		gnsApp = app;		
	    threadMap = new ConcurrentHashMap<Runnable, Thread>();
	    
		clientPool = new ClientPool(app); 
	    // Get the ThreadFactory implementation to use
	    threadFactory = new ActiveCodeThreadFactory(clientPool);
	    // Create the ThreadPoolExecutor
	    executorPool = new ActiveCodeExecutor(numProcesses, numProcesses, 0, TimeUnit.SECONDS, 
	    		new LinkedBlockingQueue<Runnable>(1000), 
	    		//new SynchronousQueue<Runnable>(),
	    		threadFactory, new ThreadPoolExecutor.DiscardPolicy(),
	    		this);
	    // Start the processes
	    executorPool.prestartAllCoreThreads();
	    System.out.println("##################### All threads have been started! ##################");
	    
	    guard = new ActiveCodeGuardian();
	    (new Thread(guard)).start();
	    
	    scheduler = new ActiveCodeScheduler(executorPool, guard);
	    (new Thread(scheduler)).start();
	    // Blacklist init
		blacklist = new HashMap<>();
		this.blacklistSeconds = blacklistSeconds;
	}
	
	protected void register(Runnable r, Thread t){
		threadMap.put(r, t);
	}
	
	protected void remove(Runnable r){
		threadMap.remove(r);
	}
	
	protected Thread get(Runnable r){
		return threadMap.get(r);
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
	public boolean isPoolBusy() {
		return executorPool.getActiveCount() == executorPool.getMaximumPoolSize();
	}
	
	/**
	 * Checks to see if the guid is currently blacklisted.
	 * @param guid
	 * @return true if the guid is blacklisted
	 */
	public boolean isBlacklisted(String guid) {
		if(!blacklist.containsKey(guid))
			return false;
		
		long blacklistedAt = blacklist.get(guid);
		
		return (System.nanoTime() - blacklistedAt) < (blacklistSeconds * NANOSECONDS_PER_SEC);
	}
	
	/**
	 * Adds the guid to the blacklist
	 * @param guid
	 */
	public void addToBlacklist(String guid) {
		blacklist.put(guid, System.nanoTime());
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
		// If the guid is blacklisted, just return immediately
		if(isBlacklisted(guid)) {
			System.out.println("Guid " + guid + " is blacklisted from running code!");
			return valuesMap;
		}
		
		
		//Construct Value parameters
		String code = new String(Base64.decodeBase64(code64));
		String values = valuesMap.toString();		
		ActiveCodeParams acp = new ActiveCodeParams(guid, field, action, code, values, activeCodeTTL);
		FutureTask<ValuesMap> futureTask = new FutureTask<ValuesMap>(new ActiveCodeTask(acp, clientPool));
				
		scheduler.submit(futureTask, guid);
		
		ValuesMap result = null;
		
		try {
			result = futureTask.get();
		} catch (ExecutionException e) {
			System.out.println("Added " + guid + " to blacklist!");
			//addToBlacklist(guid);
			e.printStackTrace();
		} catch (CancellationException e){
			// Exception because of being cancelled
			ActiveCodeClient client = clientPool.getClient(threadMap.get(futureTask).getId());
			// Restart the server of the corresponding thread
			client.restartServer();
		} catch (Exception e){
			e.printStackTrace();
		}
		
		
		this.threadMap.remove(futureTask);
		if(!futureTask.isCancelled()){
			guard.remove(futureTask);
		}
		scheduler.release();
		
		
		// This is an best effort implementation
		// Only run if there are free workers and queue space
		// This prevents excessive CPU usage
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
