/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.nsdesign.replicationframework;


import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;

/**
 * We choose the type of replication needed. The options that currently work are LOCATION, and RANDOM.
 * LOCATION chooses replica locations for a name at those name servers that are close to the local name servers
 * sending requests for tha name.
 * RANDOM chooses replica locations randomly.
 * Both LOCATION and RANDOM choose the same number of replicas for a name; this number is determined by the
 * read-to-write ratio of that name.
 * <p>
 * In this package, there are two classes that implement ReplicationFrameworkInterface.
 * These are LocationBasedReplication and RandomReplication, that are respectively used to implement LOCATION, and
 * RANDOM schemes respectively.
 * </p>
 * <p>STATIC, BEEHIVE, and OPTIMAL are used only for running experiments for paper.</p>
 *
 * @see edu.umass.cs.gns.nsdesign.replicaController.ComputeNewActivesTask
 * @see edu.umass.cs.gns.nsdesign.replicaController.Add
 *
 * @author westy, Abhigyan
 */
public enum ReplicationFrameworkType {
  LOCATION,  // Main replication algorithm
  RANDOM,
  STATIC,    // used only for experiments
  BEEHIVE;   // used only for experiments// used only for experiments// used only for experiments// used only for experiments

  public static ReplicationFrameworkInterface instantiateReplicationFramework(ReplicationFrameworkType type, GNSNodeConfig gnsNodeConfig) {
    ReplicationFrameworkInterface framework;
    // what type of replication?
    switch (type) {
      case LOCATION:
        framework = new LocationBasedReplication();
        break;
      case RANDOM:
        framework = new RandomReplication();
        break;
      case STATIC: // used only for experiments
        framework = null;
        break;
      case BEEHIVE: // used only for experiments
        BeehiveReplication.generateReplicationLevel(gnsNodeConfig.getNumberOfNodes(), Config.beehiveC,
                Config.beehiveWorkloadSize, Config.beehiveAlpha, Config.beehiveBase);
        framework = null;
        break;
      default:
        throw new RuntimeException("Invalid replication framework");
    }
    return framework;
  }
}
