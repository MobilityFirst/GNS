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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;


/**
 * This class is a hotfix for catching exceptions when using Future
 * See: afterExecute
 * @author mbadov
 *
 */
public class ActiveCodeExecutor extends ThreadPoolExecutor {
	private ActiveCodeGuardian guard;
	private ClientPool clientPool;
	
	protected ActiveCodeExecutor(int numCoreThreads, int numMaxThreads, int timeout,
			TimeUnit timeUnit,
			BlockingQueue<Runnable> queue,
			ThreadFactory threadFactory,
			RejectedExecutionHandler handler,
			ActiveCodeGuardian guard,
			ClientPool clientPool) {
		super(numCoreThreads, numMaxThreads, timeout, timeUnit, queue, threadFactory, handler);
		this.guard = guard;
		this.clientPool = clientPool;
	}
	
	//private static final long MAX_CLIENT_READY_WAIT_TIME = 500;
	
	@Override
	protected void beforeExecute(Thread t, Runnable r){
		// register the runnable here
		ActiveCodeFutureTask task = (ActiveCodeFutureTask) r;
		ActiveCodeClient client = clientPool.getClient(t.getId());
		assert(client != null);
		
		ActiveCodeClient previousClient = task.getWrappedTask().setClient(client);

		assert(previousClient == null);
		if(ActiveCodeHandler.enableDebugging)
			ActiveCodeHandler.getLogger().log(Level.INFO, task + " waiting on client "+client+" to be ready");    	
    	
		//check the state of the client's worker
    	while(!client.isReady()){
    		// wait until it's ready, no limit on waiting time 
    		synchronized(client){
    			try {
					client.wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    		}
    	}
    	/*
    	 * Invariant: client must be ready to proceed
    	 */
    	assert(client.isReady());
    	
    	if(ActiveCodeHandler.enableDebugging)
    		ActiveCodeHandler.getLogger().log(Level.INFO, this + "'s client"+client+" is ready; before calling task "+task);
    	
    	guard.register(task); 

    	
		if(ActiveCodeHandler.enableDebugging)
			ActiveCodeHandler.getLogger().log(Level.INFO, this + " successfully registers task "+task);
		
		super.beforeExecute(t, r);
		
	}
	
	public String toString() {
		return this.getClass().getSimpleName();
	}
	
    @Override
	protected void afterExecute(Runnable r, Throwable t) {	
        super.afterExecute(r, t);
        // deregister the runnable (ActiveCodeFutureTask) here
        ActiveCodeFutureTask task = (ActiveCodeFutureTask) r;
        if(ActiveCodeHandler.enableDebugging)
        	ActiveCodeHandler.getLogger().log(Level.INFO, this + " received throwable " + t + " for task " + task);
        try {
        	if(ActiveCodeHandler.enableDebugging)
        		ActiveCodeHandler.getLogger().log(Level.INFO, this + " waiting to deregister " + task + "...");
        	  
        	guard.deregister(task);
        	
        	if(ActiveCodeHandler.enableDebugging)
        		ActiveCodeHandler.getLogger().log(Level.INFO, " successfully deregistered " + task);
        } catch(Exception | Error e) {
        	e.printStackTrace();
        } finally {
        	//assert(task.setRunning(false) == true);
        }
	}

}
