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
	
	
	@Override
	protected void beforeExecute(Thread t, Runnable r){
		// register the runnable here
		ActiveCodeFutureTask task = (ActiveCodeFutureTask) r;
		if(true)
        	System.out.println(this + " runs thread " + t + " for task " + task);
		ActiveCodeClient previousClient = task.getWrappedTask().setClient(clientPool.getClient(t.getId()));
		assert(task.setRunning(true) == false);
		assert(previousClient == null);
		guard.register(task); 	
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
        	System.out.println(this + " received throwable " + t + " for task " + task);
        try {
        	if(ActiveCodeHandler.enableDebugging)
        		System.out.print(this + " waiting to deregister " + task + "...");
        	
        	/*
        	 * Invariant: if derigistrating a task results in a null, then the task 
        	 * must have been cancelled or have been completed.
        	 */
        	//if(guard.deregister(task) == null)
        		//assert( task.isCancelled() || task.isDone());  
        	guard.deregister(task);
        	
        	if(ActiveCodeHandler.enableDebugging)
        		System.out.println(" successfully deregistered " + task);
        } catch(Exception | Error e) {
        	e.printStackTrace();
        } finally {
        	//assert(task.setRunning(false) == true);
        }
	}

}
