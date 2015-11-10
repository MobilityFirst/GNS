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
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
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
	public ActiveCodeExecutor(int numCoreThreads, int numMaxThreads, int timeout,
			TimeUnit timeUnit,
			BlockingQueue<Runnable> queue,
			ThreadFactory threadFactory,
			RejectedExecutionHandler handler) {
		super(numCoreThreads, numMaxThreads, timeout, timeUnit, queue, threadFactory, handler);
	}

	/**
	 * This is a fix for catching exceptions when using Future
	 * For more details see: 
	 * https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/ThreadPoolExecutor.html#afterExecute(java.lang.Runnable,%20java.lang.Throwable)
	 */
        @Override
	protected void afterExecute(Runnable r, Throwable t) {
		super.afterExecute(r, t);
		
		if (t == null && r instanceof Future<?>) {
			try {
				Future<?> future = (Future<?>) r;
				if (future.isDone())
					future.get();
			} catch (CancellationException ce) {
				t = ce;
			} catch (ExecutionException ee) {
				t = ee.getCause();
				throw new RuntimeException();
			} catch (InterruptedException ie) {
				Thread.currentThread().interrupt(); // ignore/reset
			}
		}
	}
}
