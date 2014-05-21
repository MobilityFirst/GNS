package edu.umass.cs.gns.nsdesign.replicationframework;

import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.recordmap.ReplicaControllerRecord;
import edu.umass.cs.gns.nsdesign.replicaController.ReplicaController;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/*************************************************************
 * This class implements the ReplicationFramework interface
 * and is used to select active name servers based on location
 * of the demand.
 * 
 * @author Hardeep Uppal, Abhigyan
 ************************************************************/
public class LocationBasedReplication implements ReplicationFrameworkInterface {

  /*************************************************************
   * Returns a new set of active name servers based on votes 
   * received for replica selection for this record. NameServers
   * with the highest votes are selected as the new active name
   * servers.
   * @param rcRecord NameRecord whose active name server set is
   * generated.
   * @param numReplica Size of the new active name server set.
   * @param count  Number of times replicas have been computed
   ************************************************************/
  @Override
  public ReplicationOutput newActiveReplica(ReplicaController rc, ReplicaControllerRecord rcRecord, int numReplica, int count) throws FieldNotFoundException {

    Set<Integer> newActiveNameServerSet;
    Set<Integer> localityBasedReplicas;
    //		 Use top-K based on locality.
    if (numReplica > Config.nameServerVoteSize) {
      newActiveNameServerSet = rcRecord.getHighestVotedReplicaID(rc.getGnsNodeConfig(), Config.nameServerVoteSize);
      localityBasedReplicas = new HashSet<Integer>(newActiveNameServerSet);
    } else {
      newActiveNameServerSet = rcRecord.getHighestVotedReplicaID(rc.getGnsNodeConfig(), numReplica);
      localityBasedReplicas = new HashSet<Integer>(newActiveNameServerSet);
    }
    // remove highly loaded replicas that are chosen based on locality
    HashSet<Integer> highlyLoaded = new HashSet<>();
    for (int nodeID: newActiveNameServerSet) {
      if (isHighlyLoaded(rc, nodeID)) {
        highlyLoaded.add(nodeID);
      }
    }

    if (highlyLoaded.size() > 0) {
      GNS.getStatLogger().info(" Removing highly loaded servers from replica set. Name: " +
              rcRecord.getName() + " Loaded Servers: " + highlyLoaded);
      newActiveNameServerSet.removeAll(highlyLoaded);
      localityBasedReplicas.removeAll(highlyLoaded);
    } else {
      if (Config.debugMode) GNS.getLogger().fine("No highly loaded nodes among: " + newActiveNameServerSet);
    }

    if (numReplica >= rc.getGnsNodeConfig().getNameServerIDs().size()) {
      newActiveNameServerSet = new HashSet<Integer>(rc.getGnsNodeConfig().getNameServerIDs());
    } else {

      // Select based on votes as much as you can.

      if (newActiveNameServerSet.size() < numReplica) {
        int difference = numReplica - newActiveNameServerSet.size();
        //Randomly select the other active name servers
        for (int i = 1; i <= difference; i++) {
          if (newActiveNameServerSet.size() >= rc.getGnsNodeConfig().getNameServerIDs().size()) {
            break;
          }
          boolean added = false;

          // Ensures that random selection will still be deterministic for each name.
          Random random = new Random(rcRecord.getName().hashCode());
          int retries = 0;
          do {
            retries += 1;
            int nsIndex = random.nextInt(rc.getGnsNodeConfig().getNameServerIDs().size());
            int newActiveNameServerId = getSetIndex(rc.getGnsNodeConfig().getNameServerIDs(), nsIndex);
            if (rc.getGnsNodeConfig().getPingLatency(newActiveNameServerId) == -1) {
              continue;
            }
            added = newActiveNameServerSet.add(newActiveNameServerId);
          } while (!added && retries < 5*rc.getGnsNodeConfig().getNameServerIDs().size());
        }
      }
    }

    return new ReplicationOutput(newActiveNameServerSet, localityBasedReplicas);
  }

  private boolean isHighlyLoaded(ReplicaController rc, int nodeID) {
    return rc.getNsRequestRates().get(nodeID) != null && rc.getNsRequestRates().get(nodeID) >= Config.maxReqRate;
  }

  private int getSetIndex(Set<Integer> nodeIds, int index) {
    int count = 0;
    for (int node: nodeIds) {
      if  (count == index) return node;
      count++;

    }
    return -1;
  }
}
