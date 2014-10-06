package edu.umass.cs.gns.paxos;

import java.io.Serializable;

public class Ballot<NodeIDType> implements Comparable<Ballot>, Serializable{
	public int ballotNumber;
	public NodeIDType coordinatorID;

	public Ballot(int ballotNumber, NodeIDType coordinatorID) {
		this.ballotNumber = ballotNumber;
		this.coordinatorID = coordinatorID;
	}

	public Ballot(String s) {
		String[] tokens = s.split(":");
		ballotNumber = new Integer(tokens[0]);
		coordinatorID = (NodeIDType) tokens[1];
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
                  return coordinatorID.toString().compareTo(b.coordinatorID.toString());
		}
	}

    public int getBallotNumber() {
      return ballotNumber;
    }
    public NodeIDType getCoordinatorID() {
      return  coordinatorID;
    }

	@Override
	public   String toString() {
		return ballotNumber + ":" + coordinatorID;
	}

}
