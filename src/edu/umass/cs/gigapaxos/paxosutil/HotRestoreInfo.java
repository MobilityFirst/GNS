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

import edu.umass.cs.utils.Util;

/**
 * @author V. Arun
 * 
 *         A simple utility class to serialize and deserialize paxos state for
 *         pause and hot restore.
 */
@SuppressWarnings("javadoc")
public class HotRestoreInfo {
	/**
	 * Paxos group name.
	 */
	public final String paxosID;
	/**
	 * Paxos group epoch number. A paxosID:version two-tuple corresponds to a
	 * unique replicated state machine.
	 */
	public final int version;
	/**
	 * Paxos group members.
	 */
	public final int[] members;
	/**
	 * Next unused request slot at acceptor.
	 */
	public final int accSlot;
	/**
	 * Ballot corresponding to most recent executed request.
	 */
	public final Ballot accBallot;
	/**
	 * Slot up to which acceptor can garbage collect logged messages.
	 */
	public final int accGCSlot;
	/**
	 * Ballot at acceptor.
	 */
	public final Ballot coordBallot;
	/**
	 * Next proposal slot at coordinator.
	 */
	public final int nextProposalSlot;
	/**
	 * An array at the coordinator indicating which nodes have executed or
	 * checkpointed up which slot numbers in order to do garbage collection.
	 */
	public final int[] nodeSlots;

	public HotRestoreInfo(String paxosID, int version, int[] members,
			int accSlot, Ballot accBallot, int accGCSlot, Ballot coordBallot,
			int nextProposalSlot, int[] nodeSlots) {
		this.paxosID = paxosID;
		this.version = version;
		this.members = members;
		this.accBallot = accBallot;
		this.accSlot = accSlot;
		this.accGCSlot = accGCSlot;
		this.coordBallot = coordBallot;
		this.nextProposalSlot = nextProposalSlot;
		this.nodeSlots = nodeSlots;
	}

	/* NOTE: The order of the following operations is important */
	public HotRestoreInfo(String serialized) {
		String[] tokens = serialized.split("\\|");
		this.paxosID = tokens[0];
		this.version = Integer.parseInt(tokens[1]);
		this.members = Util.stringToIntArray(tokens[2]);
		this.accSlot = Integer.parseInt(tokens[3]);
		this.accGCSlot = Integer.parseInt(tokens[5]);
		this.accBallot = new Ballot(tokens[4]);
		this.coordBallot = !tokens[6].equals("null") ? new Ballot(tokens[6])
				: null;
		this.nextProposalSlot = Integer.parseInt(tokens[7]);
		this.nodeSlots = !tokens[8].equals("null") ? Util
				.stringToIntArray(tokens[8]) : null;
	}

	private static final char SEP = '|';

	public String toString() {
		return paxosID
				+ SEP
				+ version
				+ SEP
				+ Util.arrayOfIntToString(members)
				+ SEP
				+ accSlot
				+ SEP
				+ accBallot
				+ SEP
				+ accGCSlot
				+ SEP
				+ (coordBallot != null ? coordBallot : "null")
				+ SEP
				+ nextProposalSlot
				+ SEP
				+ (nodeSlots != null ? Util.arrayOfIntToString(nodeSlots)
						: "null");
	}
	
	public boolean isCreateHRI() {
		return this.accSlot == 1 && this.version == 0 && this.accGCSlot == -1
				&& this.coordBallot.ballotNumber == 0
				&& this.nextProposalSlot == 1;
	}

	public static HotRestoreInfo createHRI(String paxosID, int[] members,
			int coordinator) {
		assert(Util.contains(coordinator, members));
		return new HotRestoreInfo(paxosID, 0, members, 1, new Ballot(0,
				coordinator), -1, new Ballot(0, coordinator), 1,
				new int[members.length]);
	}

	public static void main(String[] args) {
		int[] members = { 1, 4, 67 };
		int[] nodeSlots = { 1, 3, 5 };
		HotRestoreInfo hri1 = new HotRestoreInfo("paxos0", 2, members,
				5, new Ballot(3, 4), 3, new Ballot(45, 67), 34, nodeSlots);
		System.out.println(hri1.toString());
		String str1 = hri1.toString();
		HotRestoreInfo hri2 = new HotRestoreInfo(str1);
		String str2 = hri2.toString();
		System.out.println(str2);
		assert (str1.equals(str2));
	}

}
