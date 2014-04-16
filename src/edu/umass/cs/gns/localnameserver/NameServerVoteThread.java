package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nsdesign.packet.NameServerSelectionPacket;

import java.util.Random;
import java.util.Set;

/**************************************************************
 * This class implements the thread that periodically multicasts
 * nameserver selection votes to primary nameservers of a name.
 * <br/>
 * The votes are reported to the primary nameservers and used
 * to select active nameservers with the highest votes during 
 * replication period.
 * <br/>
 * We have add support to also report the number of updates for a
 * name received at local name server. The ratio of lookups
 * to updates for a name is used to decide the number of active
 * replicas.
 * <br/>
 * @see edu.umass.cs.gns.localnameserver.NameRecordStats
 * @see edu.umass.cs.gns.nameserver.replicacontroller.ListenerNameRecordStats
 * @author Hardeep Uppal, Abhigyan
 *************************************************************/
public class NameServerVoteThread extends Thread {


	/** Time interval in ms between transmitting votes **/
	private long voteIntervalMillis;


//  public static ConcurrentHashMap<Integer,Integer> unackedVotes = new ConcurrentHashMap<Integer,Integer>();
	
	/**************************************************************
	 * Constructs a new NameServerVoteThread that periodically 
	 * multicast nameserver selection votes to primary nameservers 
	 * of a name.
	 * @param voteIntervalMillis Time interval in ms between transmitting
	 * votes
	 *************************************************************/
	public NameServerVoteThread(long voteIntervalMillis) {
		super("NameServerVoteThread");
		this.voteIntervalMillis = voteIntervalMillis;

	}

	/*************************************************************
	 * Starts executing this thread.
	 ************************************************************/
	@Override
	public void run() {
		long interval;
		int count = 0;

    Random r = new Random();
		
		try {
			long x = voteIntervalMillis / 2 + r.nextInt((int) voteIntervalMillis / 2);
			Thread.sleep(x);
			GNS.getLogger().fine("NameServerVoteThread: Sleeping for " + x + "ms");
		} catch (InterruptedException e) {
			if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Initial thread sleeping period.");
		}
		/* Start interval */
    long startInterval = System.currentTimeMillis();
		while (true) {
			// sleep between loops
			interval = System.currentTimeMillis() - startInterval;
			if (count >=1 && interval < voteIntervalMillis) {
				try {
//					long x = voteInterval - interval;
					Thread.sleep(voteIntervalMillis);
          GNS.getLogger().fine("NameServerVoteThread: Sleeping for " + voteIntervalMillis + "ms");
				} catch (InterruptedException e) {
					if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Thread sleeping interrupted.");
				}
			}
			count++;
			startInterval = System.currentTimeMillis();
			int vote;
      int update;
			NameServerSelectionPacket nsSelectionPacket;
      int nsToVoteFor = selectNSToVoteFor(); // name server selection does not depend on name
      if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" NameRecordStats Key Set: " + LocalNameServer.getNameRecordStatsKeySet());
      int nameCount = 0;
      int allNames = 0;
      long t0  = System.currentTimeMillis();
			for (String name : LocalNameServer.getNameRecordStatsKeySet()) {
        allNames ++;
        if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" BEGIN VOTING: " + name);
				try {
          NameRecordStats stats = LocalNameServer.getStats(name);

					vote = stats.getVotes();
          update = stats.getUpdateVotes();
          if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" VOTE COUNT: " + vote);
					if (vote == 0 && update == 0) {
						continue;
					}
          nameCount++;
          if (StartLocalNameServer.debugMode) GNS.getLogger().fine("\tVoteSent\t" + name +"\t" + vote +"\t" + update+"\t");
//					int uniqueVoteID = r.nextInt();
					nsSelectionPacket = new NameServerSelectionPacket(name, vote, update, nsToVoteFor, LocalNameServer.getNodeID(), 0);

          // send to all primaries.
          Set<Integer> primaryNameServers = LocalNameServer.getPrimaryNameServers(name);
          if (StartLocalNameServer.debugMode) GNS.getLogger().info("Primary name servers = " + primaryNameServers + " name = " + name);
          for (int primary: primaryNameServers) {
            LocalNameServer.sendToNS(nsSelectionPacket.toJSONObject(), primary);
          }
          Thread.sleep(5); // we are sleeping between sending votes. if we do not sleep, there will be a period
          // where all resources are used for sending votes, which will affect other traffic at LNS.
        } catch (Exception e) {
          e.printStackTrace();
        }
			}
      long t1 = System.currentTimeMillis();
      GNS.getLogger().info("Round " + count +  ". Votes sent for " + nameCount + " names / " + allNames + " names. " +
              "Time = " + (t1 - t0) + " ms");
		}
	}

	private int selectNSToVoteFor() {
    if (StartLocalNameServer.loadDependentRedirection) {
      Set<Integer> allNS = LocalNameServer.getGnsNodeConfig().getNameServerIDs();
      return LocalNameServer.selectBestUsingLatecyPlusLoad(allNS);
    } else {
			return LocalNameServer.getGnsNodeConfig().getClosestNameServer();
		}
	}

}

