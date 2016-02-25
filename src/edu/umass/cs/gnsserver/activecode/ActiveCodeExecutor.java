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

import edu.umass.cs.utils.DelayProfiler;



/**
 * This class is a hotfix for catching exceptions when using Future
 * See: afterExecute
 * @author mbadov
 *
 */
public class ActiveCodeExecutor extends ThreadPoolExecutor {
	private ActiveCodeGuardian guard;
	
	protected ActiveCodeExecutor(int numCoreThreads, int numMaxThreads, int timeout,
			TimeUnit timeUnit,
			BlockingQueue<Runnable> queue,
			ThreadFactory threadFactory,
			RejectedExecutionHandler handler,
			ActiveCodeGuardian guard) {
		super(numCoreThreads, numMaxThreads, timeout, timeUnit, queue, threadFactory, handler);
		this.guard = guard;
	}
	
	
	@Override
	protected void beforeExecute(Thread t, Runnable r){
		long t1 = System.currentTimeMillis();
		// register the runnable here
		guard.register((ActiveCodeFutureTask) r); 
		super.beforeExecute(t, r);
		DelayProfiler.updateDelay("ActiveCodeExecutorBeforeExecute", t1);
	}
	
    @Override
	protected void afterExecute(Runnable r, Throwable t) {	
    	long t1 =System.currentTimeMillis();
        super.afterExecute(r, t);
        // deregister the runnable (ActiveCodeFutureTask) here
        guard.deregister((ActiveCodeFutureTask) r);
        DelayProfiler.updateDelay("ActiveCodeExecutorAfterExecute", t1);
	}

}
