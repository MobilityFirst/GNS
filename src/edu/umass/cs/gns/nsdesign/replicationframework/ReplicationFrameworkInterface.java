/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.nsdesign.replicationframework;

import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.nsdesign.recordmap.ReplicaControllerRecord;
import edu.umass.cs.gns.nsdesign.replicaController.ReconfiguratorInterface;

/**
 * This is a replication framework interface. Classes that 
 * want to be part of the replication framework need to 
 * implement this interface.
 * 
 * @param <NodeIDType>
 */
@Deprecated
public interface ReplicationFrameworkInterface<NodeIDType> {

  public final static int NUM_RETRY = 1000;

  /**
   * Returns a new set of active nameservers for a name
   * @param rc
   * @param record ReplicaControllerRecord for this name.
   * @param numReplica Number of replicas
   * @param count
   * @return A Set containing active nameservers id.
   * @throws edu.umass.cs.gns.exceptions.FieldNotFoundException
   **/
  public ReplicationOutput newActiveReplica(ReconfiguratorInterface<NodeIDType> rc, ReplicaControllerRecord<NodeIDType> record, int numReplica, int count) throws FieldNotFoundException;
}
