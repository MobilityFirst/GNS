package edu.umass.cs.gnsclient.examples;
import java.util.concurrent.ThreadFactory;

/**
 * A simple implementation of ThreadFactory
 * 
 * @author Zhaoyu Gao
 */
public class MyThreadFactory implements ThreadFactory {
	
	public Thread newThread(Runnable r) {
		Thread t = new Thread(r);
	    return t;
	}
}
