package edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil;

/**
@author V. Arun
 */

/* Utility class needed by PaxosManager and PaxosLogger for recovery.
 * Just a container class.
 */
	public class RecoveryInfo {
		final String paxosID;
		final short version;
		final int[] members;
		private String state=null;

		public RecoveryInfo(String id, short ver, int[] group) {
			this.paxosID = id;
			this.version = ver;
			this.members = group;
		}
		public RecoveryInfo(String id, short ver, int[] group, String state) {
			this.paxosID = id;
			this.version = ver;
			this.members = group;
			this.state=state;
		}
		public String getPaxosID() {return paxosID;}
		public short getVersion() {return version;}
		public int[] getMembers() {return members;}
		public String getState() {return this.state;}
		public String toString() {
			String s="", group="[";
			for(int i=0; i<members.length; i++) group+=members[i] + " ";
			group+="]";
			s+= "[ " + paxosID + ", " + version + ", " + group + ", " + this.state + " ]";
			return s;
		}
	}
