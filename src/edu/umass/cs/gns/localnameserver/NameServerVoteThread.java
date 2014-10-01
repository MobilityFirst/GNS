package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nsdesign.packet.NameServerSelectionPacket;
import java.util.Random;
import java.util.Set;

/**
 * This class implements the thread that periodically multicasts
 * nameserver selection votes to replica controllers of a name.
 * <br/>
 * The votes are reported to the replica controllers and used
 * to select active replicas with the highest votes during
 * replication period.
 * <br/>
 * We have added support to also report the number of updates for a
 * name received at local name server. The ratio of lookups
 * to updates for a name is used to decide the number of active
 * replicas.
 * <br/>
 *
 * @param <NodeIDType>
 * @see edu.umass.cs.gns.localnameserver.NameRecordStats
 * @see edu.umass.cs.gns.nsdesign.replicaController.NameStats
 * @author Hardeep Uppal, Abhigyan
 */
public class NameServerVoteThread<NodeIDType> extends Thread {

  /**
   * Time interval in ms between transmitting votes *
   */
  private long voteIntervalMillis;

  /**
   * ************************************************************
   * Constructs a new NameServerVoteThread that periodically
   * multicast nameserver selection votes to primary nameservers
   * of a name.
   *
   * @param voteIntervalMillis Time interval in ms between transmitting
   * votes
   ************************************************************
   */
  public NameServerVoteThread(long voteIntervalMillis) {
    super("NameServerVoteThread");
    this.voteIntervalMillis = voteIntervalMillis;

  }

  /**
   * ***********************************************************
   * Starts executing this thread.
   ***********************************************************
   */
  @Override
  public void run() {
    long interval;
    int count = 0;

    try {
      long x = voteIntervalMillis + new Random(4000).nextInt((int) voteIntervalMillis);
      Thread.sleep(x);
      GNS.getLogger().fine("NameServerVoteThread: Sleeping for " + x + "ms");
    } catch (InterruptedException e) {
      if (StartLocalNameServer.debugMode) {
        GNS.getLogger().fine("Initial thread sleeping period.");
      }
    }
    long startInterval = System.currentTimeMillis();
    while (true) {
      // sleep between loops
      interval = System.currentTimeMillis() - startInterval;
      if (count >= 1 && interval < voteIntervalMillis) {
        try {
          Thread.sleep(voteIntervalMillis);
          GNS.getLogger().fine("NameServerVoteThread: Sleeping for " + voteIntervalMillis + "ms");
        } catch (InterruptedException e) {
          if (StartLocalNameServer.debugMode) {
            GNS.getLogger().fine("Thread sleeping interrupted.");
          }
        }
      }
      count++;
      startInterval = System.currentTimeMillis();
      int vote;
      int update;
      NameServerSelectionPacket nsSelectionPacket;
      NodeIDType nsToVoteFor = selectNSToVoteFor(); // name server selection does not depend on name
      GNS.getLogger().info("LNS ID" + LocalNameServer.getAddress() + " Closest NS " + nsToVoteFor.toString());
      if (StartLocalNameServer.debugMode) {
        GNS.getLogger().fine(" NameRecordStats Key Set Size: " + LocalNameServer.getNameRecordStatsKeySet().size());
      }
      int nameCount = 0;
      int allNames = 0;
      long t0 = System.currentTimeMillis();
      for (String name : LocalNameServer.getNameRecordStatsKeySet()) {
        allNames++;
        if (StartLocalNameServer.debugMode) {
          GNS.getLogger().fine(" BEGIN VOTING: " + name);
        }
        try {
          NameRecordStats stats = LocalNameServer.getStats(name);

          vote = stats.getVotes();
          update = stats.getUpdateVotes();
          if (StartLocalNameServer.debugMode) {
            GNS.getLogger().fine(" VOTE COUNT: " + vote);
          }
          if (vote == 0 && update == 0) {
            continue;
          }
          nameCount++;
          if (StartLocalNameServer.debugMode) {
            GNS.getLogger().fine("\tVoteSent\t" + name + "\t" + vote + "\t" + update + "\t");
          }
          nsSelectionPacket = new NameServerSelectionPacket(name, vote, update, nsToVoteFor, LocalNameServer.getAddress());

          // send to all primaries.
          Set<NodeIDType> primaryNameServers = LocalNameServer.getReplicaControllers(name);
          if (StartLocalNameServer.debugMode) {
            GNS.getLogger().info("Primary name servers = " + primaryNameServers + " name = " + name);
          }
          for (NodeIDType primary : primaryNameServers) {
            LocalNameServer.sendToNS(nsSelectionPacket.toJSONObject(), primary);
          }
          Thread.sleep(100); // rate limit the sending of votes. if we do limit rate, there will be a period
          // where all resources are used for sending votes, which will affect other traffic at LNS.
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      long t1 = System.currentTimeMillis();
      GNS.getLogger().info("Round " + count + ". Votes sent for " + nameCount + " names / " + allNames + " names. "
              + "Time = " + (t1 - t0) + " ms");
    }
  }

  private NodeIDType selectNSToVoteFor() {
    if (StartLocalNameServer.loadDependentRedirection) {
      Set allNS = LocalNameServer.getGnsNodeConfig().getNodeIDs();
      return (NodeIDType) LocalNameServer.selectBestUsingLatencyPlusLoad(allNS);
    } else {
      return (NodeIDType) LocalNameServer.getGnsNodeConfig().getClosestServer();
    }
  }

}
