/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.nsdesign.replicationframework;

import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.recordmap.ReplicaControllerRecord;
import edu.umass.cs.gns.nsdesign.replicaController.ReconfiguratorInterface;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

/*************************************************************
 * This class implements the ReplicationFramework interface
 * and is used to randomly select new active nameservers.
 * 
 * @author Abhigyan
 * @param <NodeIDType>
 ************************************************************/
@Deprecated
public class RandomReplication<NodeIDType> implements ReplicationFrameworkInterface<NodeIDType> {
  @Override
  public ReplicationOutput newActiveReplica(ReconfiguratorInterface<NodeIDType> rc, ReplicaControllerRecord<NodeIDType> rcRecord, int numReplica, int count)
          throws FieldNotFoundException {
    // random replicas will be selected deterministically for each name.

    if (numReplica == rc.getGnsNodeConfig().getNumberOfNodes()) {
      return new ReplicationOutput<NodeIDType>(new HashSet<NodeIDType>(rc.getGnsNodeConfig().getNodeIDs()));
    }

    Set<NodeIDType> activeNameServers = new HashSet<NodeIDType>();
    if (count > 0) {
      activeNameServers = rcRecord.getActiveNameservers();
    }

    int numActiveNameServers = activeNameServers.size();

    if (numReplica > numActiveNameServers) {
      //Randomly add new active name server
      int add = numReplica - numActiveNameServers;
      Set<NodeIDType> newActiveNameServerSet = new HashSet<NodeIDType>(activeNameServers);
      //Randomly choose active name servers from a uniform distribution between
      //0 and N where N is 'add'
      for (int i = 1; i <= add; i++) {
        Random random = new Random(rcRecord.getName().hashCode());

        boolean added;
        int numTries = 0;
        do {
          numTries += 1;
          int nsIndex = random.nextInt(rc.getGnsNodeConfig().getNumberOfNodes());
          NodeIDType newActiveNameServerId = getSetIndex(rc.getGnsNodeConfig().getNodeIDs(),nsIndex);
          added = newActiveNameServerSet.add(newActiveNameServerId)
                  && rc.getGnsNodeConfig().getPingLatency(newActiveNameServerId) != GNSNodeConfig.INVALID_PING_LATENCY;
        } while (!added && numTries < NUM_RETRY);
      }

      return new ReplicationOutput<NodeIDType>(newActiveNameServerSet);
    } else if (numReplica < numActiveNameServers && count > 1) {
      //Randomly remove old active name server

      int sub = numActiveNameServers - numReplica;
      List<NodeIDType> oldActiveNameServerSet = new ArrayList<NodeIDType>(activeNameServers);

      // remove elements from the end of list.
      for (int i = 1; i <= sub; i++) {
        oldActiveNameServerSet.remove(oldActiveNameServerSet.size() - 1);
      }

      return new ReplicationOutput<NodeIDType>(new HashSet<NodeIDType>(oldActiveNameServerSet));
    } else {
      if (count == 1) {
        Set<NodeIDType> newActiveNameServerSet = new HashSet<NodeIDType>();
        for (int i = 1; i <= numReplica; i++) {
          Random random = new Random(rcRecord.getName().hashCode());
          boolean added;
          int numTries = 0;
          do {
            numTries += 1;
            int nsIndex = random.nextInt(rc.getGnsNodeConfig().getNumberOfNodes());
            NodeIDType newActiveNameServerId = getSetIndex(rc.getGnsNodeConfig().getNodeIDs(),nsIndex);           
            added = newActiveNameServerSet.add(newActiveNameServerId)
                    && rc.getGnsNodeConfig().getPingLatency(newActiveNameServerId) != GNSNodeConfig.INVALID_PING_LATENCY;
          } while (!added && numTries < NUM_RETRY);
        }

        return new ReplicationOutput<NodeIDType>(newActiveNameServerSet);
      } else {
        //Return the old set of active name servers
        return new ReplicationOutput<NodeIDType>(activeNameServers);
      }
    }
  }

  private NodeIDType getSetIndex(Set<NodeIDType> nodeIds, int index) {
    int count = 0;
    for (NodeIDType node: nodeIds) {
      if  (count == index) return node;
      count++;

    }
    return null;
  }


}
