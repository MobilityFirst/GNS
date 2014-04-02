package edu.umass.cs.gns.nsdesign;

import edu.umass.cs.gns.nsdesign.activeReconfiguration.ActiveReplica;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurable;
import edu.umass.cs.gns.nsdesign.replicaController.ReplicaController;

/**
 * A name server in GNS implement this interface.
 *
 * The main purpose of the <code>NameServerInterface</code> is to provide the <code>NSPacketDemultiplexer</code>
 * a reference to <code>GnsReconfigurableInterface</code> and  <code>ReplicaControllerInterface</code> so it
 * can forward incoming packets accordingly.
 *
 *
 * Created by abhigyan on 2/27/14.
 */
public interface NameServerInterface {

  public GnsReconfigurable getGnsReconfigurable();

  public ActiveReplica getActiveReplica();

  public ReplicaController getReplicaController();

}
