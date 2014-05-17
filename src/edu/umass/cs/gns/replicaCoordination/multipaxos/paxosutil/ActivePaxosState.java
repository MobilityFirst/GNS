package edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil;

import edu.umass.cs.gns.replicaCoordination.multipaxos.PaxosManager;

/**
@author V. Arun
 */

/* This class maintains state for active paxos instances that
 * is not needed by idle ones, e.g., for retransmissions or 
 * sync requests for missing decisions, so we store this 
 * separately in a smaller hashmap in PaxosManager and clean
 * out long-idle objects periodically.
 */
public class ActivePaxosState {
	private static final long MIN_RESYNC_DELAY = 1000; //ms
	private static final long MAX_IDLE_PERIOD = PaxosManager.getDeactivationPeriod(); //ms

	public final String paxosID;

	/* FIXME: This could be a place to put things we need to check for periodically, 
	 * e.g., checkRunForLocalCoordinator or pokeLocalCoordinator (to resend accepts). 
	 * These are currently checked for upon receipt of every packet in 
	 * PaxosInstanceStateMachine, which suffices but is a bit more overhead compared 
	 * to scheduling it here as a periodic task over just active instances.
	 * 
	 * Sync'ing needed to be moved here, otherwise a sync request gets sent out upon
	 * receipt of most decisions as most of them arrive quite out-of-order under
	 * high load.
	 */
	private long lastActive=0; // last time we did anything of interest (currently just syncing)
	private long lastSyncd=0; // last time we sent out a request for missing decisions.
	private boolean hasRecovered=false;

	public ActivePaxosState(String paxosID) {
		this.paxosID=paxosID;
		this.lastActive=System.currentTimeMillis();
	}

	public synchronized boolean isLongIdle() {
		return System.currentTimeMillis() - this.lastActive > MAX_IDLE_PERIOD;
	}
	public synchronized boolean canSync() {
		return System.currentTimeMillis()-this.lastSyncd > MIN_RESYNC_DELAY;
	}
	public synchronized void justSyncd() {
		this.lastSyncd=System.currentTimeMillis();
		this.lastActive=this.lastSyncd;
	}
	public synchronized void justActive() {
		this.lastActive = System.currentTimeMillis();
	}
	public synchronized void setRecovered() {this.hasRecovered=true;}
	public synchronized boolean hasRecovered() {return this.hasRecovered;}
}
