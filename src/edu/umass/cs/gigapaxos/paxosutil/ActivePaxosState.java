/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.gigapaxos.paxosutil;


/**
 * @author V. Arun
 * 
 *         This class maintains state for active paxos instances that is not
 *         needed by idle ones, e.g., for retransmissions or sync requests for
 *         missing decisions, so we store this separately in a smaller hashmap
 *         in PaxosManager and clean out long-idle objects periodically.
 * 
 *         This class is now deprecated and will be removed. It used to be used
 *         by PaxosManager and PaxosInstanceStateMachine to keep track of the
 *         last active time and the last sync'd time. These were kept in a
 *         separate structure because there wasn't an efficient iterator over
 *         MultiArrayHashMap, a problem that no longer exists.
 */
@Deprecated
public class ActivePaxosState {
	/**
	 * Minimum delay between successive sync decisions requests.
	 */
	public static final long MIN_RESYNC_DELAY = 1000; // ms
	private static final long MAX_IDLE_PERIOD = 30000;//PaxosManager.getDeactivationPeriod(); // ms

	/**
	 * ID of paxos group.
	 */
	public final String paxosID;

	/*
	 * FIXME: This could be a place to put things we need to check for
	 * periodically, e.g., checkRunForLocalCoordinator or pokeLocalCoordinator
	 * (to resend accepts). These are currently checked for upon receipt of
	 * every packet in PaxosInstanceStateMachine, which suffices but is a bit
	 * more overhead compared to scheduling it here as a periodic task over just
	 * active instances.
	 * 
	 * Sync'ing needed to be moved here, otherwise a sync request gets sent out
	 * upon receipt of most decisions as most of them arrive quite out-of-order
	 * under high load.
	 */
	private long lastActive = 0; // last time we did anything of interest
									// (currently just syncing)
	private long lastSyncd = 0; // last time we sent out a request for missing
								// decisions.

	/**
	 * @param paxosID
	 */
	public ActivePaxosState(String paxosID) {
		this.paxosID = paxosID;
		this.lastActive = System.currentTimeMillis();
	}

	/**
	 * @return True if idle for longer than MAX_IDLE_PERIOD.
	 */
	public synchronized boolean isLongIdle() {
		return System.currentTimeMillis() - this.lastActive > MAX_IDLE_PERIOD;
	}

	/**
	 * @return True if last time sync'd was at least MIN_RESYNC_DELAY in the
	 *         past.
	 */
	public synchronized boolean canSync() {
		return System.currentTimeMillis() - this.lastSyncd > MIN_RESYNC_DELAY;
	}

	/**
	 * 
	 */
	public synchronized void justSyncd() {
		this.lastSyncd = System.currentTimeMillis();
		// this.lastActive=this.lastSyncd;
	}

	/**
	 * 
	 */
	public synchronized void justActive() {
		this.lastActive = System.currentTimeMillis();
	}
}
