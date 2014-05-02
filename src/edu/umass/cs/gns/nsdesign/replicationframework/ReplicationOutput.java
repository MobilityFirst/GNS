package edu.umass.cs.gns.nsdesign.replicationframework;

import java.util.Set;

/**
 * Output of replication decision by a class implementing
 * {@link edu.umass.cs.gns.nsdesign.replicationframework.ReplicationFrameworkInterface}.
 *
 * Created by abhigyan on 4/30/14.
 */
public class ReplicationOutput {

  private Set<Integer> replicas;

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
