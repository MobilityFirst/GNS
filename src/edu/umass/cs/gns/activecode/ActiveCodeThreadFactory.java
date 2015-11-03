/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 */
package edu.umass.cs.gns.activecode;

import java.util.concurrent.ThreadFactory;

/**
 * This ThreadFactory allocates an ActiveCodeClient for every new worker thread.
 * @author mbadov
 *
 */
public class ActiveCodeThreadFactory implements ThreadFactory {
	ClientPool clientPool;
	
	public ActiveCodeThreadFactory(ClientPool clientPool) {
		this.clientPool = clientPool;
	}
	
	/**
	 * Creates a new thread and also spawns a new worker associated with the thread
	 */
        @Override
	public Thread newThread(Runnable r) {
		Thread t = new Thread(r);
		clientPool.addClient(t);
	    return t;
	}
}
