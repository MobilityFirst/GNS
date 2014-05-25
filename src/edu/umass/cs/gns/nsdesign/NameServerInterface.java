package edu.umass.cs.gns.nsdesign;

import edu.umass.cs.gns.nsdesign.activeReconfiguration.ActiveReplica;
import edu.umass.cs.gns.replicaCoordination.ActiveReplicaCoordinator;
import edu.umass.cs.gns.replicaCoordination.ReplicaControllerCoordinator;

/**
 * A name server in GNS implement this interface.
 *
 * The main purpose of the <code>NameServerInterface</code> is to provide the <code>NSPacketDemultiplexer</code>
 * a reference to <code>GnsReconfigurableInterface</code> and  <code>ReplicaControllerInterface</code> so it
 * can forward incoming packets accordingly.
 *
 *
 * Created by abhigyan on 2/27/14.
 * 
 * Arun: Edited to remove a warning about using raw types. FIXME: This is an
 * odd interface to have. If the goal of this interface is just as a container
 * for the members being retrieved below, they might as well be defined as
 * public final members here and accessed directly.
 */
public interface NameServerInterface {

  public ActiveReplicaCoordinator getActiveReplicaCoordinator();

  public ActiveReplica<?> getActiveReplica();

  public ReplicaControllerCoordinator getReplicaControllerCoordinator();

}
