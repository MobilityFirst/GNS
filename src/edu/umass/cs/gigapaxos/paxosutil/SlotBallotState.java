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

import java.util.Set;

/**
 * @author V. Arun
 * 
 *         Used by PaxosInstanceStateMachine and PaxosLogger to get checkpoint
 *         information. Just a container class.
 */
@SuppressWarnings("javadoc")
public class SlotBallotState {
	public final int slot;
	public final int ballotnum;
	public final int coordinator;
	/**
	 * The checkpointed state.
	 */
	public final String state;
	
	public final int version;
	public final long createTime;
	public final Set<String> members;

	public SlotBallotState(int s, int bn, int c, String st, int version, long createTime, Set<String> members) {
		this.slot = s;
		this.ballotnum = bn;
		this.coordinator = c;
		this.state = st;
		this.version = version;
		this.createTime = createTime;
		this.members = members;
	}

	public int getSlot() {
		return this.slot;
	}

	public int getBallotnum() {
		return this.ballotnum;
	}

	public int getCoordinator() {
		return this.coordinator;
	}
	
	public int getVersion() {
		return this.version;
	}
	public long getCreateTime() {
		return this.createTime;
	}

	public String toString() {
		return "[slot=" + slot + ", ballot=" + ballotnum + ":" + coordinator
				+ ", state = " + state + "]";
	}
}
