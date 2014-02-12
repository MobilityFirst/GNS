package edu.umass.cs.gns.localnameserver.original;

import java.util.Random;

/*************************************************************
* This class implements the object for maintaining name record
* statistic. It also provides method to calculate votes.
* 
* @author Hardeep Uppal
 ************************************************************/
public class NameRecordStats {

	/** Total number of lookup request **/
	private int totalLookup;

	/** Total number of update request **/
	private int totalUpdate;

	/** Keeps track of the number of lookup votes per interval **/
	private int lookupVoteCount;
	
	/** Keeps track of the number of lookup votes per interval **/
	private int updateVoteCount;
	
	/** Number of lookup response **/
	private int totalLookupResponse;

	/** Number of update response **/
	private int totalUpdateResponse;

	/*************************************************************
	 * Constructs a new NameRecordStats object with lookup and 
	 * update count initialized to 0.
	 ************************************************************/
	public NameRecordStats() {
		this.totalLookup = 0;
		this.totalUpdate = 0;
		this.lookupVoteCount = 0;
		this.updateVoteCount = 0;
		this.totalLookupResponse = 0;
		this.totalUpdateResponse = 0;
	}

	/*************************************************************
	 * Returns the total number of lookup request.
	 ************************************************************/
	public synchronized int getTotalLookup() {
		return totalLookup;
	}

	/*************************************************************
	 * Returns the total number of update request.
	 ************************************************************/
	public synchronized int getTotalUpdate() {
		return totalUpdate;
	}
	
	/*************************************************************
	 * Returns the total number of lookup response.
	 ************************************************************/
	public synchronized int getTotalLookupResponse() {
		return totalLookupResponse;
	}

	/*************************************************************
	 * Returns the total number of update response.
	 ************************************************************/
	public synchronized int getTotalUpdateResponse() {
		return totalUpdateResponse;
	}

	/*************************************************************
	 * Increments the lookup request count by 1.
	 ************************************************************/
	public synchronized void incrementLookupCount() {
		totalLookup += 1;
	}

	/*************************************************************
	 * Increments the update request count by 1.
	 ************************************************************/
	public synchronized void incrementUpdateCount() {
		totalUpdate += 1;
	}
	
	/*************************************************************
	 * Increments the lookup response count by 1.
	 ************************************************************/
	public synchronized void incrementLookupResponse() {
		totalLookupResponse += 1;
	}

	/*************************************************************
	 * Increments the update response count by 1.
	 ************************************************************/
	public synchronized void incrementUpdateResponse() {
		totalUpdateResponse += 1;
	}

	/**************************************************************
	 * Returns the average lookup vote for the current interval
	 *************************************************************/
	public synchronized int getVotes() {
		int lookupVote = totalLookup - lookupVoteCount;
		lookupVoteCount += lookupVote;
		return lookupVote;
	}

  public synchronized int getUpdateVotes() {
    int updateVote = totalUpdate - updateVoteCount;
    updateVoteCount += updateVote;
    return updateVote;
  }

  /*************************************************************
	 * Returns a String representation of this object.
	 ************************************************************/
	public synchronized String toString() {
		return "TotalLookup:" + totalLookup + " LookupResponse:" + totalLookupResponse 
				+ " TotalUpdate:" + totalUpdate + " UpdateResponse:" + totalUpdateResponse;
	}
	
	/** Test **/
	public static void main(String[] args) {
		NameRecordStats nrs = new NameRecordStats();
		Random random = new Random( System.currentTimeMillis() );
		for(int i = 0; i < 10; i++) {
			int j = 0;
			for( ; j < (random.nextInt(20) + 10); j++ ) {
				nrs.incrementLookupCount();
			}
			System.out.println("Time:" + i + " Lookup:" + j + " Votes:" + nrs.getVotes());
			System.out.println("NameRecordStats: " + nrs.toString() + "\n");
		}
	}
}
