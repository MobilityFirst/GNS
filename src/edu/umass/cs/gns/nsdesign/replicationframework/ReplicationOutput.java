package edu.umass.cs.gns.nsdesign.replicationframework;

import java.util.Set;

/**
 * Output of replication decision by a class implementing
 * {@link edu.umass.cs.gns.nsdesign.replicationframework.ReplicationFrameworkInterface}.
 *
 * The purpose of this class is for a ReplicationFrameworkInterface to specify which replicas are chosen based on
 * locality.
 *
 * Created by abhigyan on 4/30/14.
 */
public class ReplicationOutput {

  // all replicas for a name
  private Set<Integer> replicas;

  // replica chosen based on demand locality
  private Set<Integer> localityBasedReplicas;

  public ReplicationOutput(Set<Integer> replicas) {
    this(replicas, null);
  }
  public ReplicationOutput(Set<Integer> replicas, Set<Integer> localityBasedReplicas) {

    this.replicas = replicas;
    this.localityBasedReplicas = localityBasedReplicas;
  }

  public Set<Integer> getReplicas() {
    return replicas;
  }

  public Set<Integer> getLocalityBasedReplicas() {
    return localityBasedReplicas;
  }
}
