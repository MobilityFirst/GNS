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

import edu.umass.cs.utils.Util;

/**
 * @author V. Arun
 * @author arun
 *
 *         Utility class needed by PaxosManager and PaxosLogger for recovery.
 *         Just a container class.
 */
@SuppressWarnings("javadoc")
public class RecoveryInfo {
	final String paxosID;
	final int version;
	final Set<String> members;
	private String state = null;

	public RecoveryInfo(String id, int ver, String[] group) {
		this.paxosID = id;
		this.version = ver;
		this.members = Util.arrayOfObjectsToStringSet(group);
	}

	public RecoveryInfo(String id, int ver, String[] group, String state) {
		this.paxosID = id;
		this.version = ver;
		this.members = Util.arrayOfObjectsToStringSet(group);
		this.state = state;
	}

	public String getPaxosID() {
		return paxosID;
	}

	public int getVersion() {
		return version;
	}

	public Set<String> getMembers() {
		return members;
	}

	public String getState() {
		return this.state;
	}

	public String toString() {
		String s = "", group = "[";
		for (String member : this.members)
			group += member + " ";
		group += "]";
		s += "[ " + paxosID + ", " + version + ", " + group + ", " + this.state
				+ " ]";
		return s;
	}
}
