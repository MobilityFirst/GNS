package edu.umass.cs.gns.nsdesign.replicationframework;

import edu.umass.cs.gns.exceptions.FieldNotFoundException;
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
 * @author Hardeep Uppal
 ************************************************************/
public class LocationBasedReplication implements ReplicationFrameworkInterface {

  //  private Random random = new Random(System.currentTimeMillis());
  /*************************************************************
   * Returns a new set of active name servers based on votes 
   * received for replica selection for this record. NameServers
   * with the highest votes are selected as the new active name
   * servers.
   * @param nameRecordPrimary NameRecord whose active name server set is
   * generated.
   * @param numReplica Size of the new active name server set.
   * @param count
   ************************************************************/
  @Override
  public Set<Integer> newActiveReplica(ReplicaController rc, ReplicaControllerRecord nameRecordPrimary, int numReplica, int count) throws FieldNotFoundException {

    Set<Integer> newActiveNameServerSet;
    if (numReplica >= rc.getGnsNodeConfig().getNumberOfNameServers()) {
      newActiveNameServerSet = new HashSet<Integer>();
      for (int i = 0; i < rc.getGnsNodeConfig().getNumberOfNameServers(); i++) {
//				if (nameRecordPrimary.getPrimaryNameservers().contains(i)) continue;
        newActiveNameServerSet.add(i);
      }
      return newActiveNameServerSet;
    }
    //		 Use top-K based on locality.
    if (numReplica > Config.nameServerVoteSize) {
      newActiveNameServerSet = nameRecordPrimary.getHighestVotedReplicaID(rc.getGnsNodeConfig(), Config.nameServerVoteSize);
    } else {
      newActiveNameServerSet = nameRecordPrimary.getHighestVotedReplicaID(rc.getGnsNodeConfig(), numReplica);
    }

    // Select based on votes as much as you can.
    //		newActiveNameServerSet = nameRecord.getHighestVotedReplicaID(numReplica);

    if (newActiveNameServerSet.size() < numReplica) {
      int difference = numReplica - newActiveNameServerSet.size();
      //Randomly select the other active name servers
      for (int i = 1; i <= difference; i++) {
        if (newActiveNameServerSet.size() >= //+ nameRecordPrimary.getPrimaryNameservers().size()
                rc.getGnsNodeConfig().getNumberOfNameServers()) {
          break;
        }
        boolean added = false;
        // Ensures that random selection will still be deterministic for each name. 
        Random random = new Random(nameRecordPrimary.getName().hashCode());
        do {
          int newActiveNameServerId = random.nextInt(rc.getGnsNodeConfig().getNumberOfNameServers());
          if (rc.getGnsNodeConfig().getPingLatency(newActiveNameServerId) == -1) {
            // nameRecordPrimary.getPrimaryNameservers().contains(newActiveNameServerId) ||
            continue;
          }
          added = newActiveNameServerSet.add(newActiveNameServerId);
        } while (!added);
      }
    }

    return newActiveNameServerSet;
  }
}
