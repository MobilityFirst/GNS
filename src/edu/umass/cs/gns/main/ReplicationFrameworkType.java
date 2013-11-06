/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.main;

import edu.umass.cs.gns.replicationframework.BeehiveReplication;
import edu.umass.cs.gns.replicationframework.KMediods;
import edu.umass.cs.gns.replicationframework.LocationBasedReplication;
import edu.umass.cs.gns.replicationframework.RandomReplication;
import edu.umass.cs.gns.replicationframework.ReplicationFrameworkInterface;

/**
 *
 * @author westy
 */
public enum ReplicationFrameworkType {

  STATIC,
  RANDOM,
  LOCATION,
  BEEHIVE,
  KMEDIODS,
  OPTIMAL;

  public static ReplicationFrameworkInterface instantiateReplicationFramework(ReplicationFrameworkType type) {
    ReplicationFrameworkInterface framework;
    // what type of replication?
    switch (StartNameServer.replicationFramework) {
      case LOCATION:
        framework = new LocationBasedReplication();
        break;
      case RANDOM:
        framework = new RandomReplication();
        break;
      case BEEHIVE:
        BeehiveReplication.generateReplicationLevel(StartNameServer.C,
                StartNameServer.regularWorkloadSize + StartNameServer.mobileWorkloadSize,
                StartNameServer.alpha, StartNameServer.base);
        framework = new RandomReplication();
        break;
      case KMEDIODS:
        framework = new KMediods();
        break;
      default:
        throw new RuntimeException("Invalid replication framework");
    }
    return framework;
  }
}
