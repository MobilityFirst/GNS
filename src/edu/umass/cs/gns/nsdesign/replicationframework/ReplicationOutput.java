package edu.umass.cs.gns.nsdesign.replicationframework;

import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;
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
  private Set<NodeId<String>> replicas;

  // replica chosen based on demand locality
  private Set<NodeId<String>> localityBasedReplicas;

  public ReplicationOutput(Set<NodeId<String>> replicas) {
    this(replicas, null);
  }
  public ReplicationOutput(Set<NodeId<String>> replicas, Set<NodeId<String>> localityBasedReplicas) {

    this.replicas = replicas;
    this.localityBasedReplicas = localityBasedReplicas;
  }

  public Set<NodeId<String>> getReplicas() {
    return replicas;
  }

  public Set<NodeId<String>> getLocalityBasedReplicas() {
    return localityBasedReplicas;
  }
}
