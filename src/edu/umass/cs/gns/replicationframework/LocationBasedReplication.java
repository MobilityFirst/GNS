package edu.umass.cs.gns.replicationframework;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaControllerRecord;
import edu.umass.cs.gns.util.ConfigFileInfo;

/*************************************************************
 * This class implements the ReplicationFramework interface
 * and is used to select active name servers based on location
 * of the demand.
 * 
 * @author Hardeep Uppal
 ************************************************************/
public class LocationBasedReplication implements ReplicationFramework {

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
	public Set<Integer> newActiveReplica(ReplicaControllerRecord nameRecordPrimary, int numReplica, int count) {
		
		Set<Integer> newActiveNameServerSet;
		if (numReplica >= ConfigFileInfo.getNumberOfNameServers()) {
			newActiveNameServerSet = new HashSet<Integer>();
			for (int i = 0; i < ConfigFileInfo.getNumberOfNameServers(); i++) {
				if (nameRecordPrimary.getPrimaryNameservers().contains(i)) continue;
				newActiveNameServerSet.add(i);
			}
			return newActiveNameServerSet;
		}
		//		 Use top-K based on locality.
		if (numReplica > StartNameServer.nameServerVoteSize) {
			newActiveNameServerSet = nameRecordPrimary.getHighestVotedReplicaID(StartNameServer.nameServerVoteSize);
		} else {
			newActiveNameServerSet = nameRecordPrimary.getHighestVotedReplicaID(numReplica);
		}

		// Select based on votes as much as you can.
		//		newActiveNameServerSet = nameRecord.getHighestVotedReplicaID(numReplica);

        // TODO once primary and actives can co-exist on same node, this method must be re-written
		if (newActiveNameServerSet.size() < numReplica) {
			int difference = numReplica - newActiveNameServerSet.size();
			//Randomly select the other active name servers
			for (int i = 1; i <= difference; i++) {
                if (newActiveNameServerSet.size() + nameRecordPrimary.getPrimaryNameservers().size() >=
                        ConfigFileInfo.getNumberOfNameServers()) break;
				boolean added = false;
				// Ensures that random selection will still be deterministic for each name. 
				Random random = new Random(nameRecordPrimary.getName().hashCode());
				do {
					int newActiveNameServerId = random.nextInt(ConfigFileInfo.getNumberOfNameServers());
					if ( nameRecordPrimary.getPrimaryNameservers().contains(newActiveNameServerId) ||
							ConfigFileInfo.getPingLatency(newActiveNameServerId) == -1) {
						continue;
					}
					added = newActiveNameServerSet.add(newActiveNameServerId);
				} while (!added);
			}
		}

		return newActiveNameServerSet;
	}
}
