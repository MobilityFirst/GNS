package edu.umass.cs.gns.paxos;

import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;
import java.io.Serializable;

public class Ballot implements Comparable<Ballot>, Serializable{
	public int ballotNumber;
	public NodeId<String> coordinatorID;

	public Ballot(int ballotNumber, NodeId<String> coordinatorID) {
		this.ballotNumber = ballotNumber;
		this.coordinatorID = coordinatorID;
	}

	public Ballot(String s) {
		String[] tokens = s.split(":");
		ballotNumber = new Integer(tokens[0]);
		coordinatorID = new NodeId<String>(tokens[1]);
	}
	
	@Override
	public  int compareTo(Ballot b) {
		if (ballotNumber > b.ballotNumber ) {
			return 1;
		}
		else if (ballotNumber < b.ballotNumber) {
			return -1;
		}
		else {
                  return coordinatorID.compareTo(b.coordinatorID);
		}
	}

    public int getBallotNumber() {
      return ballotNumber;
    }
    public NodeId<String> getCoordinatorID() {
      return  coordinatorID;
    }

	@Override
	public   String toString() {
		return ballotNumber + ":" + coordinatorID;
	}

}
