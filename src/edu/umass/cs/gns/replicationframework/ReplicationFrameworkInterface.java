package edu.umass.cs.gns.replicationframework;

import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.nameserver.recordmap.ReplicaControllerRecord;
import edu.umass.cs.gns.util.ConfigFileInfo;
import java.util.Set;

//import edu.umass.cs.gnrs.nameserver.NameRecord;
/*************************************************************
 * This is a replication framework interface. Classes that 
 * want to be part of the replication framework need to 
 * implement this interface.
 * 
 * @author Hardeep Uppal
 ************************************************************/
public interface ReplicationFrameworkInterface {

  public final static int NUM_RETRY = ConfigFileInfo.getNumberOfNameServers() * 50;

  /*************************************************************
   * Returns a new set of active nameservers for a NameRecord
   * @param record NameRecord
   * @param numReplica Number of replicas
   * @return A Set containing active nameservers id.
   ************************************************************/
  public Set<Integer> newActiveReplica(ReplicaControllerRecord record, int numReplica, int count) throws FieldNotFoundException;
}
