package edu.umass.cs.gns.activereplica;

import edu.umass.cs.gns.util.Util;

/**
@author V. Arun
 */
public class ActiveReplicaStats {
	private static final double ALPHA = Util.ALPHA; 
	
	private int numRequests = 0;
	private double interRequestDelay = 0.0; // in milliseconds
	private long prevRequestTime = System.currentTimeMillis(); 

	public int registerRequest() {
		long curDelaySample = System.currentTimeMillis() - this.prevRequestTime;
		this.interRequestDelay = Util.movingAverage(curDelaySample, interRequestDelay, ALPHA);
		return ++this.numRequests;
	}

	public int getNumRequests() {return this.numRequests;}
	
	public double getRequestRate() {return 1.0/this.interRequestDelay*1000;}
	
}
