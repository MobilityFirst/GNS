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
 *         A ballot is simply a <ballotNumber, ballotCoordinator> two-tuple of
 *         integers. Ballots are completely ordered. In paxos, every decision,
 *         i.e., a committed request, is committed with some slot number and a
 *         ballot. A proposal, i.e., <slot, request> two-tuple can possibly get
 *         committed as a decision in multiple ballots, but paxos ensures the
 *         safety property that two different requests never get committed with
 *         the same slot irrespective of the ballot.
 * 
 */

@SuppressWarnings("javadoc")
public class Ballot implements Comparable<Ballot> {
	private static final String SEPARATOR = ":";

	/**
	 * Ballot number that increases monotonically at acceptors for a given
	 * coordinator ID.
	 */
	public final int ballotNumber;
	/**
	 * Ballot coordinator that breaks ordering ties at acceptors when the ballot
	 * number is the same.
	 */
	public final int coordinatorID;

	public Ballot(int ballotNumber, int coordinatorID) {
		this.ballotNumber = ballotNumber;
		this.coordinatorID = coordinatorID;
	}

	public Ballot(String s) {
		String[] tokens = s.split(":");
		this.ballotNumber = new Integer(tokens[0]);
		this.coordinatorID = new Integer(tokens[1]);
	}

	@Override
	public int compareTo(Ballot b) {
		if (ballotNumber != b.ballotNumber)
			return ballotNumber - b.ballotNumber; // will handle wraparounds
													// correctly
		else
			return coordinatorID - b.coordinatorID;
	}

	public int compareTo(int bnum, int coord) {
		if (ballotNumber != bnum)
			return ballotNumber - bnum; // will handle wraparounds correctly
		else
			return coordinatorID - coord;
	}

	@Override
	public boolean equals(Object b) {
		return b instanceof Ballot ? (compareTo((Ballot)b) == 0) : false;
	}

	/*
	 * Need to implement hashCode if we modify equals. The only property we need
	 * is equals() => hashcodes are also equal. The method below just ensures
	 * that. It also roughly tries to incorporate that ballotnum is the more
	 * significant component, but that is not strictly necessary for anything.
	 */
	public int hashCode() {
		return (100 + ballotNumber) * (100 + ballotNumber) + coordinatorID;
	}

	public static String getBallotCoordString(String ballotString) {
		String[] pieces = ballotString.split(SEPARATOR);
		if (pieces.length == 2 && pieces[1] != null)
			return pieces[1];
		return null;
	}

	public static Integer getBallotNumString(String ballotString) {
		String[] pieces = ballotString.split(SEPARATOR);
		if (pieces.length == 2 && pieces[0] != null)
			return Integer.parseInt(pieces[0].trim());
		return null;
	}

	@Override
	public String toString() {
		return ballotNumber + ":" + coordinatorID;
	}

	public static String getBallotString(int bnum, int coord) {
		return bnum + SEPARATOR + coord;
	}

	public static String getBallotString(int ballotnum, Object ballotCoord) {
		return ballotnum + SEPARATOR + ballotCoord.toString();
	}
}
