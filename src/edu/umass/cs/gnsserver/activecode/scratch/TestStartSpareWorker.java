package edu.umass.cs.gnsserver.activecode.scratch;

import edu.umass.cs.gnsserver.activecode.ClientPool;

/**
 * @author gaozy
 *
 */
public class TestStartSpareWorker {
	
	/**
	 * @param args
	 */
	public static void main(String[] args){
		long total = 0;
		
		ClientPool clientPool = new ClientPool(null);
		clientPool.addSpareWorker();
		for(int i=0; i<100; i++){
			long t = System.currentTimeMillis();
			clientPool.addSpareWorker();
			long eclapsed = System.currentTimeMillis() - t;
			total = total + eclapsed;
		}
		clientPool.shutdown();
		System.out.println("The average time is "+total/100+"ms");
	}
}
