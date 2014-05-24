/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.nsdesign.replicationframework;


/**
 *
 * @author westy
 */
public enum ReplicationFrameworkType {

  STATIC,
  RANDOM,
  LOCATION,
  BEEHIVE,
  OPTIMAL, ReplicationFrameworkType;

  public static ReplicationFrameworkInterface instantiateReplicationFramework(ReplicationFrameworkType type) {
    ReplicationFrameworkInterface framework = null;
    // what type of replication?
    switch (type) {
      case LOCATION:
        framework = new LocationBasedReplication();
        break;
      case RANDOM:
        assert false: "FIXME: RandomReplication should implement ReplicationFrameworkInterface";
//        framework = new RandomReplication();
        break;
      case BEEHIVE:
        // Abhigyan: we will enable beehive if we again need to run experiments with it.
        throw new UnsupportedOperationException();
//        BeehiveReplication.generateReplicationLevel(StartNameServer.C,
//                StartNameServer.regularWorkloadSize + StartNameServer.mobileWorkloadSize,
//                StartNameServer.alpha, StartNameServer.base);
//        framework = new RandomReplication();
//        break;
      case STATIC:
        framework = null;
        break;
      default:
        throw new RuntimeException("Invalid replication framework");
    }
    return framework;
  }
}
