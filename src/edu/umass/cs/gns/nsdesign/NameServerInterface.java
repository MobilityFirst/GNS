package edu.umass.cs.gns.nsdesign;

import edu.umass.cs.gns.nsdesign.activeReplica.ActiveReplicaInterface;
import edu.umass.cs.gns.nsdesign.replicaController.ReplicaControllerInterface;

/*** DONT not use any class in package edu.umass.cs.gns.nsdesign ***/

/**
 * A name server in GNS implement this interface.
 *
 * The main purpose of the <code>NameServerInterface</code> is to provide the <code>NSPacketDemultiplexer</code>
 * a reference to <code>ActiveReplicaInterface</code> and  <code>ReplicaControllerInterface</code> so it
 * can forward incoming packets accordingly.
 *
 *
 * Created by abhigyan on 2/27/14.
 */
public interface NameServerInterface {

  public ActiveReplicaInterface getActiveReplica();

  public ReplicaControllerInterface getReplicaController();

}
