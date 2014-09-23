package edu.umass.cs.gns.reconfiguration.reconfigurationutils;

import java.net.InetAddress;

import edu.umass.cs.gns.reconfiguration.InterfaceRequest;
import edu.umass.cs.gns.util.Util;

/**
@author V. Arun
 */
public class DemandProfile {
	private static final int DEFAULT_NUM_REQUESTS = 1;

	private final String name;
	private double interArrivalTime=0.0;
	private long lastRequestTime=0;
	private int numRequests=0;
	private int numTotalRequests=0;

	public DemandProfile(String name) {
		this.name = name;
	}
	// deep copy constructor
	public DemandProfile(DemandProfile dp) {
		this.name = dp.name;
		this.interArrivalTime = dp.interArrivalTime;
		this.lastRequestTime = dp.lastRequestTime;
		this.numRequests = dp.numRequests;
		this.numTotalRequests = dp.numTotalRequests;
	}
	public String getName() {return this.name;}
	/* FIXME: Ignoring sender argument for now. Need to use it to develop
	 * a demand geo-distribution profile.
	 */
	public void register(InterfaceRequest request, InetAddress sender) {
		if(!request.getServiceName().equals(this.name)) return;
		this.numRequests++; this.numTotalRequests++;
		long iaTime=0;
		if(lastRequestTime>0) {
			iaTime = System.currentTimeMillis() - this.lastRequestTime;
			this.interArrivalTime = Util.movingAverage(iaTime, interArrivalTime);
		}
		else lastRequestTime = System.currentTimeMillis(); // initialization
	}
	public double getRequestRate() {
		return this.interArrivalTime>0 ? 1.0/this.interArrivalTime : 1.0/(this.interArrivalTime+1000);
	}
	public double getNumRequests() {
		return this.numRequests;
	}
	public double getNumTotalRequests() {
		return this.numTotalRequests;
	}
	public boolean shouldReport() {
		if(getNumRequests() >= DEFAULT_NUM_REQUESTS) return true;
		return false;
	}

	public void reset() {
		this.interArrivalTime=0.0;
		this.lastRequestTime=0;
		this.numRequests=0;
	}
}
