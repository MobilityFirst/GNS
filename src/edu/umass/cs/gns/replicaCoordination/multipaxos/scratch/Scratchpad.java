package edu.umass.cs.gns.replicaCoordination.multipaxos.scratch;

import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.Ballot;

/**
@author V. Arun
 */
public class Scratchpad {

	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		class Test1 {
			Ballot ballot;
			int num;
			Test1(Ballot b, int n ) {ballot=b; num=n;}
		}
		class Test2 {
			int bnum;
			int coord;
			int num;
			Test2(Ballot b, int n ) {bnum=b.ballotNumber; coord=b.coordinatorID; num=n;}
			Test2(int b, int c, int n ) {bnum=b; coord=c; num=n;}
		}
		int million=1000000;
		int size = 20*million;
		Test1[] ballots = new Test1[size];
		for(int i=0; i<size; i++) {
			ballots[i] = new Test1(new Ballot(i, i+23), i+32);
			if(i%1000==0) System.out.println("Created "+i + " instances");
		}
	}
}
