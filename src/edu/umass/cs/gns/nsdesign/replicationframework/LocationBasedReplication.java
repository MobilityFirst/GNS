package edu.umass.cs.gns.nsdesign.replicationframework;

import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.recordmap.ReplicaControllerRecord;
import edu.umass.cs.gns.nsdesign.replicaController.ReconfiguratorInterface;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * This class implements the ReplicationFramework interface
 * and is used to select active name servers based on location
 * of the demand.
 *
 * @author Hardeep Uppal, Abhigyan
 * @param <NodeIDType>
 */
public class LocationBasedReplication<NodeIDType> implements ReplicationFrameworkInterface {

  /**
   * Returns a new set of active name servers based on votes
   * received for replica selection for this record. NameServers
   * with the highest votes are selected as the new active name
   * servers.
   *
   * @param rc
   * @param rcRecord NameRecord whose active name server set is
   * generated.
   * @param numReplica Size of the new active name server set.
   * @param count Number of times replicas have been computed
   * @throws edu.umass.cs.gns.exceptions.FieldNotFoundException
   */
  @Override
  public ReplicationOutput newActiveReplica(ReconfiguratorInterface rc, ReplicaControllerRecord rcRecord, int numReplica, int count) throws FieldNotFoundException {

    Set<NodeIDType> newActiveNameServerSet;
    Set<NodeIDType> localityBasedReplicas;
    //		 Use top-K based on locality.
    if (numReplica > Config.nameServerVoteSize) {
      newActiveNameServerSet = rcRecord.getHighestVotedReplicaID(rc, rc.getGnsNodeConfig(), Config.nameServerVoteSize);
      localityBasedReplicas = new HashSet<NodeIDType>(newActiveNameServerSet);
    } else {
      newActiveNameServerSet = rcRecord.getHighestVotedReplicaID(rc, rc.getGnsNodeConfig(), numReplica);
      localityBasedReplicas = new HashSet<NodeIDType>(newActiveNameServerSet);
    }

    if (numReplica >= rc.getGnsNodeConfig().getNumberOfNodes()) {
      newActiveNameServerSet = new HashSet<NodeIDType>(rc.getGnsNodeConfig().getNodeIDs());
    } else {

      // Select based on votes as much as you can.
      if (newActiveNameServerSet.size() < numReplica) {
        int difference = numReplica - newActiveNameServerSet.size();
        //Randomly select the other active name servers
        for (int i = 1; i <= difference; i++) {
          if (newActiveNameServerSet.size() >= rc.getGnsNodeConfig().getNumberOfNodes()) {
            break;
          }
          boolean added = false;

          // Ensures that random selection will still be deterministic for each name.
          Random random = new Random(rcRecord.getName().hashCode());
          int retries = 0;
          do {
            retries += 1;
            int nsIndex = random.nextInt(rc.getGnsNodeConfig().getNumberOfNodes());
            NodeIDType newActiveNameServerId = (NodeIDType) getSetIndex(rc.getGnsNodeConfig().getNodeIDs(), nsIndex);
            if (rc.getGnsNodeConfig().getPingLatency(newActiveNameServerId) == -1) {
              continue;
            }
            added = newActiveNameServerSet.add(newActiveNameServerId);
          } while (!added && retries < 5 * rc.getGnsNodeConfig().getNumberOfNodes());
        }
      }
    }

    return new ReplicationOutput(newActiveNameServerSet, localityBasedReplicas);
  }

  private boolean isHighlyLoaded(ReconfiguratorInterface rc, NodeIDType nodeID) {
    return rc.getNsRequestRates().get(nodeID) != null && (Double) rc.getNsRequestRates().get(nodeID) >= Config.maxReqRate;
  }

  private NodeIDType getSetIndex(Set<NodeIDType> nodeIds, int index) {
    int count = 0;
    for (NodeIDType node : nodeIds) {
      if (count == index) {
        return node;
      }
      count++;

    }
    return null;
  }
}
