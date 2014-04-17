/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.nsdesign.replicationframework;


import edu.umass.cs.gns.nsdesign.Config;

/**
 *
 * @author westy
 */
public enum ReplicationFrameworkType {

  STATIC,
  RANDOM,
  LOCATION,
  BEEHIVE,
  OPTIMAL;

  public static ReplicationFrameworkInterface instantiateReplicationFramework(ReplicationFrameworkType type) {
    ReplicationFrameworkInterface framework;
    // what type of replication?
    switch (Config.replicationFramework) {
      case LOCATION:
        framework = new LocationBasedReplication();
        break;
      case RANDOM:
        framework = new RandomReplication();
        break;
      case BEEHIVE:
        // Abhigyan: we will enable beehive if we again need to run experiments with it.
        throw new UnsupportedOperationException();
//        BeehiveReplication.generateReplicationLevel(StartNameServer.C,
//                StartNameServer.regularWorkloadSize + StartNameServer.mobileWorkloadSize,
//                StartNameServer.alpha, StartNameServer.base);
//        framework = new RandomReplication();
//        break;
      default:
        throw new RuntimeException("Invalid replication framework");
    }
    return framework;
  }
}
