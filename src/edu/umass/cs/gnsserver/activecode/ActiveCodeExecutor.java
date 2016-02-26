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
	protected synchronized void beforeExecute(Thread t, Runnable r){
		// register the runnable here
		ActiveCodeFutureTask task = (ActiveCodeFutureTask) r;
		// The running task should not be cancelled
		//assert(task.isCancelled());
		ActiveCodeClient previousClient = task.getWrappedTask().setClient(clientPool.getClient(t.getId()));
		assert(previousClient == null);
		guard.register(task); 	
		super.beforeExecute(t, r);
		
	}
	
    @Override
	protected synchronized void afterExecute(Runnable r, Throwable t) {	
        super.afterExecute(r, t);
        // deregister the runnable (ActiveCodeFutureTask) here
        ActiveCodeFutureTask task = (ActiveCodeFutureTask) r;
        guard.deregister(task);        
       
	}

}
