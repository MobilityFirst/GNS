package edu.umass.cs.gns.nsdesign.replicationframework;

import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;
import edu.umass.cs.gns.nsdesign.recordmap.ReplicaControllerRecord;
import edu.umass.cs.gns.nsdesign.replicaController.ReconfiguratorInterface;

import java.util.*;

/*************************************************************
 * This class implements the ReplicationFramework interface
 * and is used to randomly select new active nameservers.
 * 
 * @author Hardeep Uppal, Abhigyan
 ************************************************************/
public class RandomReplication implements ReplicationFrameworkInterface{
  int NUM_RETRY = 1000;
  @Override
  public ReplicationOutput newActiveReplica(ReconfiguratorInterface rc, ReplicaControllerRecord rcRecord, int numReplica, int count)
          throws FieldNotFoundException {
    // random replicas will be selected deterministically for each name.

    if (numReplica == rc.getGnsNodeConfig().getNumberOfNodes()) {
      return new ReplicationOutput(new HashSet<NodeId<String>>(rc.getGnsNodeConfig().getNodeIDs()));
    }

    Set<NodeId<String>> activeNameServers = new HashSet<NodeId<String>>();
    if (count > 0) {
      activeNameServers = rcRecord.getActiveNameservers();
    }

    int numActiveNameServers = activeNameServers.size();

    if (numReplica > numActiveNameServers) {
      //Randomly add new active name server
      int add = numReplica - numActiveNameServers;
      Set<NodeId<String>> newActiveNameServerSet = new HashSet<NodeId<String>>(activeNameServers);
      //Randomly choose active name servers from a uniform distribution between
      //0 and N where N is 'add'
      for (int i = 1; i <= add; i++) {
        Random random = new Random(rcRecord.getName().hashCode());

        boolean added;
        int numTries = 0;
        do {
          numTries += 1;
          int nsIndex = random.nextInt(rc.getGnsNodeConfig().getNumberOfNodes());
          NodeId<String> newActiveNameServerId = getSetIndex(rc.getGnsNodeConfig().getNodeIDs(),nsIndex);
          added = newActiveNameServerSet.add(newActiveNameServerId)
                  && rc.getGnsNodeConfig().getPingLatency(newActiveNameServerId) != -1;
        } while (!added && numTries < NUM_RETRY);
      }

      return new ReplicationOutput(newActiveNameServerSet);
    } else if (numReplica < numActiveNameServers && count > 1) {
      //Randomly remove old active name server

      int sub = numActiveNameServers - numReplica;
      List<NodeId<String>> oldActiveNameServerSet = new ArrayList<NodeId<String>>(activeNameServers);

      // remove elements from the end of list.
      for (int i = 1; i <= sub; i++) {
        oldActiveNameServerSet.remove(oldActiveNameServerSet.size() - 1);
      }

      return new ReplicationOutput(new HashSet<NodeId<String>>(oldActiveNameServerSet));
    } else {
      if (count == 1) {
        Set<NodeId<String>> newActiveNameServerSet = new HashSet<NodeId<String>>();
        for (int i = 1; i <= numReplica; i++) {
          Random random = new Random(rcRecord.getName().hashCode());
          boolean added;
          int numTries = 0;
          do {
            numTries += 1;
            int nsIndex = random.nextInt(rc.getGnsNodeConfig().getNumberOfNodes());
            NodeId<String> newActiveNameServerId = getSetIndex(rc.getGnsNodeConfig().getNodeIDs(),nsIndex);           
            added = newActiveNameServerSet.add(newActiveNameServerId)
                    && rc.getGnsNodeConfig().getPingLatency(newActiveNameServerId) != GNSNodeConfig.INVALID_PING_LATENCY;
          } while (!added && numTries < NUM_RETRY);
        }

        return new ReplicationOutput(newActiveNameServerSet);
      } else {
        //Return the old set of active name servers
        return new ReplicationOutput(activeNameServers);
      }
    }
  }

  private NodeId<String> getSetIndex(Set<NodeId<String>> nodeIds, int index) {
    int count = 0;
    for (NodeId<String> node: nodeIds) {
      if  (count == index) return node;
      count++;

    }
    return GNSNodeConfig.INVALID_NAME_SERVER_ID;
  }


}
