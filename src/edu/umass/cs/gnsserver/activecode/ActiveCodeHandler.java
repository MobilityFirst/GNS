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



import java.net.InetSocketAddress;
import java.util.concurrent.CancellationException;
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
	InetSocketAddress addr;
	long blacklistSeconds;
	
	protected static final long MILLISECONDS_PER_SEC = 1000;
	
	/**
	 * Initializes an ActiveCodeHandler
	 * @param app
	 * @param numProcesses
	 * @param blacklistSeconds
	 */
	public ActiveCodeHandler(GnsApplicationInterface<String> app, int numProcesses, long blacklistSeconds, InetSocketAddress addr) {
		this.gnsApp = app;			    		
	    this.addr = addr;
	    
		clientPool = new ClientPool(app, this); 
		(new Thread(clientPool)).start();
		
		
		guard = new ActiveCodeGuardian(clientPool);
	    (new Thread(guard)).start();
		
	    // Get the ThreadFactory implementation to use
	    threadFactory = new ActiveCodeThreadFactory(clientPool);
	    // Create the ThreadPoolExecutor
	    executorPool = new ActiveCodeExecutor(numProcesses, numProcesses, 0, TimeUnit.SECONDS, 
	    		new LinkedBlockingQueue<Runnable>(), 
	    		//new SynchronousQueue<Runnable>(),
	    		threadFactory, new ThreadPoolExecutor.DiscardPolicy());
	    // Start the processes
	    executorPool.prestartAllCoreThreads();
	    System.out.println("##################### All threads have been started! ##################");
	    //System.out.println("There are "+executorPool.getActiveCount() + " threads running, and " + executorPool.getPoolSize()+" threads available.");
	    
	    scheduler = new ActiveCodeScheduler(executorPool);
	    (new Thread(scheduler)).start();
	    
	    clientPool.startSpareWorkers();
	    
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
	 * return the local address
	 * @return the address being bound with the local name server
	 */
	protected InetSocketAddress getAddress(){
		return addr;
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
		long startTime = System.nanoTime();
		
		//Construct Value parameters
		String code = new String(Base64.decodeBase64(code64));
		String values = valuesMap.toString();
		
		ActiveCodeParams acp = new ActiveCodeParams(guid, field, action, code, values, activeCodeTTL);
		ActiveCodeTask act = new ActiveCodeTask(acp, clientPool, guard);
		FutureTask<ValuesMap> futureTask = new FutureTask<ValuesMap>(act);
		guard.registerFutureTask(act, futureTask);
		
		DelayProfiler.updateDelayNano("activeHandlerPrepare", startTime);		
		
		scheduler.submit(futureTask, guid);
		
		ValuesMap result = null;
		
		try {
			result = futureTask.get();
			guard.deregisterFutureTask(act);
		} catch (ExecutionException ee) {
			System.out.println("Execution "+guid);
			//ee.printStackTrace();
			act.deregisterTask();
			scheduler.finish(guid);
			System.out.println(">>>>>>>>>>>>> Handler returns result "+valuesMap);
			return valuesMap;
		} catch(CancellationException ce) {
			System.out.println("Cancel "+guid);
			//ce.printStackTrace();
			act.deregisterTask();
			scheduler.finish(guid);
			System.out.println(">>>>>>>>>>>>> Handler returns result "+valuesMap);
			return valuesMap;
		} catch(InterruptedException ie) {
			System.out.println("Interrupt "+guid);
			ie.printStackTrace();
			act.deregisterTask();
			scheduler.finish(guid);
			System.out.println(">>>>>>>>>>>>> Handler returns result "+valuesMap);
			return valuesMap;
		} catch (Exception e){
			System.out.println("Other");
			//e.printStackTrace();
			act.deregisterTask();
			scheduler.finish(guid);
			System.out.println(">>>>>>>>>>>>> Handler returns result "+valuesMap);
			return valuesMap;
		}
		
		scheduler.finish(guid);
		
		DelayProfiler.updateDelayNano("activeHandler", startTime);
		
		System.out.println(">>>>>>>>>>>>> Handler returns result "+result);
	    return result;
	}
}
