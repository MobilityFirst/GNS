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
 * @param <NodeIDType>
 */
@Deprecated
public class ReplicationOutput<NodeIDType> {

  // all replicas for a name
  private Set<NodeIDType> replicas;

  // replica chosen based on demand locality
  private Set<NodeIDType> localityBasedReplicas;

  public ReplicationOutput(Set<NodeIDType> replicas) {
    this(replicas, null);
  }
  public ReplicationOutput(Set<NodeIDType> replicas, Set<NodeIDType> localityBasedReplicas) {

    this.replicas = replicas;
    this.localityBasedReplicas = localityBasedReplicas;
  }

  public Set<NodeIDType> getReplicas() {
    return replicas;
  }

  public Set<NodeIDType> getLocalityBasedReplicas() {
    return localityBasedReplicas;
  }
}
