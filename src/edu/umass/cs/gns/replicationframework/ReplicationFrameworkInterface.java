package edu.umass.cs.gns.replicationframework;

import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.nameserver.recordmap.ReplicaControllerRecord;
import edu.umass.cs.gns.util.ConfigFileInfo;
import java.util.Set;
/**************** FIXME Package deprecated by nsdesign/replicationFramework. this will soon be deleted. **/

/*************************************************************
 * This is a replication framework interface. Classes that 
 * want to be part of the replication framework need to 
 * implement this interface.
 * 
 * @author Hardeep Uppal
 * @deprecated
 ************************************************************/
public interface ReplicationFrameworkInterface {
  // TODO : fix replica framework package.
  public final static int NUM_RETRY = ConfigFileInfo.getNumberOfNameServers() * 50;

  /*************************************************************
   * Returns a new set of active nameservers for a name
   * @param record ReplicaControllerRecord for this name.
   * @param numReplica Number of replicas
   * @return A Set containing active nameservers id.
   ************************************************************/
  public Set<Integer> newActiveReplica(ReplicaControllerRecord record, int numReplica, int count) throws FieldNotFoundException;
}
