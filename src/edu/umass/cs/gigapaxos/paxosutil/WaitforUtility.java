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
 *         A waitfor utility for integer node identifiers that uses roughly
 *         8(N+1) bytes when waiting for responses from N nodes. Note that the
 *         final array members will not occupy space in each instance as it will
 *         simply be a pointer to the corresponding array in the paxos instance
 *         that created it.
 * 
 *         There is a similar Waitfor structure in the utils package that is
 *         inspired by this one and is slightly cleaner and more manageable as
 *         it uses sets instead of arrays.
 */
@SuppressWarnings("javadoc")
public class WaitforUtility {

	private final int[] members;
	private final boolean[] responded;
	private int heardCount = 0;
	private long initTime = System.currentTimeMillis(); // to calculate how long we have been waiting
	private int retransmissionCount = 0;

	public WaitforUtility(int[] m) {
		this.members = m;
		//this.initTime = System.currentTimeMillis();
		responded = new boolean[m.length];
		for (int i = 0; i < m.length; i++) {
			responded[i] = false;
		}
	}

	public boolean updateHeardFrom(int node) {
		boolean changed = false;
		int index = this.getIndex(node);
		if (index >= 0 && index < this.members.length) {
			if (!responded[index]) {
				changed = true;
				heardCount++;
			}
			responded[index] = true;
		}
		return changed;
	}

	public boolean heardFromMajority() {
		if (this.heardCount > this.members.length / 2)
			return true;
		return false;
	}

	public boolean alreadyHeardFrom(int node) {
		int index = this.getIndex(node);
		if (index >= 0 && index < this.members.length) {
			if (responded[index])
				return true;
		}
		return false;
	}

	public int getHeardCount() {
		return this.heardCount;
	}

	public boolean contains(int node) {
		return getIndex(node) >= 0;
	}

	public int[] getMembers() {
		return this.members;
	}

	public int[] getMembersHeardFrom() {
		int[] membersHF = new int[this.heardCount];
		int i = 0;
		for (int member : this.getMembers()) {
			if (alreadyHeardFrom(member))
				membersHF[i++] = member;
		}
		return membersHF;
	}

	public long totalWaitTime() {
		return System.currentTimeMillis() - this.initTime;
	}
	public long waitTime() {
		return System.currentTimeMillis() - this.initTime;
	}

	private int getIndex(int node) {
		int index = -1;
		for (int i = 0; i < this.members.length; i++) {
			if (this.members[i] == node)
				index = i;
		}
		return index;
	}

	public void incrRetransmissonCount() {
		this.retransmissionCount++;
	}
	public void incrRetransmissonCount(boolean resetInitTime) {
		this.retransmissionCount++;
		if(resetInitTime) this.initTime = System.currentTimeMillis();
	}

	public int getRetransmissionCount() {
		return this.retransmissionCount;
	}

	public String toString() {
		String s = "{Members: [ ";
		for (int i = 0; i < members.length; i++)
			s += members[i] + " ";
		s += "], Responded: [";
		for (int i = 0; i < members.length; i++) {
			if (responded[i])
				s += members[i] + " ";
		}
		s += "]}";
		return s;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int[] members = { 0, 9, 4, 23 };
		WaitforUtility wfor = new WaitforUtility(members);
		assert (!wfor.contains(32));
		assert (wfor.contains(9));
		assert (wfor.getMembers() == members);
		assert (wfor.getIndex(4) == 2);
		assert (wfor.updateHeardFrom(9));
		assert (!wfor.heardFromMajority());
		assert (wfor.updateHeardFrom(23));
		assert (!wfor.heardFromMajority());
		assert (wfor.updateHeardFrom(0));
		assert (wfor.heardFromMajority());

		System.out
				.println("Success. The following methods were tested: [heardFrom, heardFromMajority, contains, getMembers, getIndex].");
	}

}
