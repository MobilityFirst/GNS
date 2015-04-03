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
	
	public Thread newThread(Runnable r) {
		Thread t = new Thread(r);
		clientPool.addClient(t);
	    return t;
	}
}
