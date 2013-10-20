package edu.umass.cs.gns.paxos;

public class Ballot implements Comparable<Ballot>{
	public int ballotNumber;
	public int coordinatorID;

	public Ballot(int ballotNumber, int coordinatorID) {
		this.ballotNumber = ballotNumber;
		this.coordinatorID = coordinatorID;
	}

	public Ballot(String s) {
		String[] tokens = s.split(":");
		ballotNumber = new Integer(tokens[0]);
		coordinatorID = new Integer(tokens[1]);
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
			if (coordinatorID > b.coordinatorID ) return 1;
			else if (coordinatorID < b.coordinatorID) return -1;
			return 0;
		}
	}

    public int getBallotNumber() {
      return ballotNumber;
    }
    public int getCoordinatorID() {
      return  coordinatorID;
    }

	@Override
	public   String toString() {
		return ballotNumber + ":" + coordinatorID;
	}

//    public synchronized int getBallotNumber() {
//        return ballotNumber;
//    }
//
//    public synchronized int getCoordinatorID() {
//        return coordinatorID;
//    }

//    public  synchronized boolean testAndUpdateBallot(Ballot b) {
//        if (compareTo(b) >0) {
//            this.ballotNumber = b.ballotNumber;
//            this.coordinatorID = b.coordinatorID;
//            return true;
//        }
//        return  false;
//    }
}
