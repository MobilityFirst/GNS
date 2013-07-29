package edu.umass.cs.gnrs.replicationframework;

import java.util.Set;

//import edu.umass.cs.gnrs.nameserver.NameRecord;
import edu.umass.cs.gnrs.nameserver.replicacontroller.ReplicaControllerRecord;
import edu.umass.cs.gnrs.util.ConfigFileInfo;

/*************************************************************
 * This is a replication framework interface. Classes that 
 * want to be part of the replication framework need to 
 * implement this interface.
 * 
 * @author Hardeep Uppal
 ************************************************************/
public interface ReplicationFramework {
	
	public final static int NUM_RETRY = ConfigFileInfo.getNumberOfNameServers() * 50;

	/*************************************************************
	 * Returns a new set of active nameservers for a NameRecord
	 * @param record NameRecord
	 * @param numReplica Number of replicas
	 * @return A Set containing active nameservers id.
	 ************************************************************/
	public Set<Integer> newActiveReplica( ReplicaControllerRecord record, int numReplica, int count );
	
}
