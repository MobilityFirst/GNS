package edu.umass.cs.gns.nsdesign.replicationframework;

import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.nsdesign.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.recordmap.ReplicaControllerRecord;
import edu.umass.cs.gns.nsdesign.replicaController.ReplicaController;

import java.util.*;

/*************************************************************
 * This class implements the ReplicationFramework interface
 * and is used to randomly select new active nameservers.
 * 
 * @author Hardeep Uppal
 ************************************************************/
public class RandomReplication implements ReplicationFrameworkInterface {

  @Override
  public Set<Integer> newActiveReplica(ReplicaController rc, ReplicaControllerRecord nameRecordPrimary, int numReplica, int count) throws FieldNotFoundException {
    // random replicas will be selected deterministically for each name.

    if (numReplica == rc.getGnsNodeConfig().getNumberOfNameServers()) {
      Set<Integer> activeNameServerSet = new HashSet<Integer>();
      for (int i = 0; i < rc.getGnsNodeConfig().getNumberOfNameServers(); i++) {
        activeNameServerSet.add(i);
      }
      return activeNameServerSet;
    }

    Set<Integer> activeNameServers = nameRecordPrimary.getActiveNameservers();

    int numActiveNameServers = activeNameServers.size();

    if (numReplica > numActiveNameServers && count > 1) {
      //Randomly add new active name server
      int add = numReplica - numActiveNameServers;
      Set<Integer> newActiveNameServerSet = new HashSet<Integer>(activeNameServers);
      //Randomly choose active name servers from a uniform distribution between
      //0 and N where N is 'add'
      for (int i = 1; i <= add; i++) {
        Random random = new Random(new Integer(nameRecordPrimary.getName().hashCode()));
        boolean added;
        int numTries = 0;
        do {
          numTries += 1;
          int newActiveNameServerId = random.nextInt(rc.getGnsNodeConfig().getNumberOfNameServers());
          added = newActiveNameServerSet.add(newActiveNameServerId)
                  && rc.getGnsNodeConfig().getPingLatency(newActiveNameServerId) != -1;
        } while (!added && numTries < NUM_RETRY);
      }

      return newActiveNameServerSet;
    } else if (numReplica < numActiveNameServers && count > 1) {
      //Randomly remove old active name server

      int sub = numActiveNameServers - numReplica;
      List<Integer> oldActiveNameServerSet = new ArrayList<Integer>(activeNameServers);

      // remove elements from the end of list.
      for (int i = 1; i <= sub; i++) {
        oldActiveNameServerSet.remove(oldActiveNameServerSet.size() - 1);
      }

      return new HashSet<Integer>(oldActiveNameServerSet);
    } else {
      if (count == 1) {
        Set<Integer> newActiveNameServerSet = new HashSet<Integer>();
        for (int i = 1; i <= numReplica; i++) {
          Random random = new Random(new Integer(nameRecordPrimary.getName().hashCode()));
          boolean added;
          int numTries = 0;
          do {
            numTries += 1;
            int newActiveNameServerId = random.nextInt(rc.getGnsNodeConfig().getNumberOfNameServers());
            added = newActiveNameServerSet.add(newActiveNameServerId)
                    && rc.getGnsNodeConfig().getPingLatency(newActiveNameServerId) != GNSNodeConfig.INVALID_PING_LATENCY;
          } while (!added && numTries < NUM_RETRY);
        }

        return newActiveNameServerSet;
      } else {
        //Return the old set of active name servers
        return activeNameServers;
      }
    }
  }


  /**
   * Putting this code here because it is related to the above random replication strategy.
   * Implements the algorithm local name server uses to select a name server when we experiment with beehive
   * replication. It chooses the closest name server if available, otherwise picks a name server randomly.
   *
   * @param gnsNodeConfig
   * @param nameserverQueried
   * @param activeNameServers
   * @return
   */
  public static int getBeehiveNameServer(GNSNodeConfig gnsNodeConfig, Set<Integer> activeNameServers, Set<Integer> nameserverQueried) {
    ArrayList<Integer> allServers = new ArrayList<Integer>();
    if (activeNameServers != null) {
      for (int x : activeNameServers) {
        if (!allServers.contains(x) && nameserverQueried != null && !nameserverQueried.contains(x)) {
          allServers.add(x);
        }
      }
    }

    if (allServers.size() == 0) {
      return -1;
    }

    if (allServers.contains(gnsNodeConfig.getClosestNameServer())) {
      return gnsNodeConfig.getClosestNameServer();
    }
    return beehiveNSChoose(gnsNodeConfig.getClosestNameServer(), allServers, nameserverQueried);

  }



  private static int beehiveNSChoose(int closestNS, ArrayList<Integer> nameServers, Set<Integer> nameServersQueried) {

    if (nameServers.contains(closestNS) && (nameServersQueried == null || !nameServersQueried.contains(closestNS))) {
      return closestNS;
    }

    Collections.sort(nameServers);
    for (int x : nameServers) {
      if (x > closestNS && (nameServersQueried == null || !nameServersQueried.contains(x))) {
        return x;
      }
    }

    for (int x : nameServers) {
      if (x < closestNS && (nameServersQueried == null || !nameServersQueried.contains(x))) {
        return x;
      }
    }

    return -1;
  }

}
