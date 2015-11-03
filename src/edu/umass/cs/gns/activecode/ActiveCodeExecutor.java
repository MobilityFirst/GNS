/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 */
package edu.umass.cs.gns.activecode;

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
