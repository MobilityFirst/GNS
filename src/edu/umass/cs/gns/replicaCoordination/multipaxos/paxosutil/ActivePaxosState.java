package edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil;

import edu.umass.cs.gns.replicaCoordination.multipaxos.PaxosManager;

/**
@author V. Arun
 */
public class ActivePaxosState {
	private static final long MIN_RESYNC_DELAY = 1000; //ms
	private static final long MAX_IDLE_PERIOD = PaxosManager.DEACTIVATION_PERIOD; //ms
	
	public final String paxosID;
	
	private long lastActive=0;
	private long lastSyncd=0;

	public ActivePaxosState(String paxosID) {
		this.paxosID=paxosID;
		this.lastActive=System.currentTimeMillis();
	}
	
	public synchronized boolean isLongIdle() {return System.currentTimeMillis() - this.lastActive > MAX_IDLE_PERIOD;}
	public synchronized boolean canSync() {return System.currentTimeMillis()-this.lastSyncd > MIN_RESYNC_DELAY;}
	public synchronized void justSyncd() {
		this.lastSyncd=System.currentTimeMillis();
		this.lastActive=this.lastSyncd;
	}
}
