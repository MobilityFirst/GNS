package edu.umass.cs.gns.nsdesign;

import edu.umass.cs.gns.nsdesign.activeReplica.ActiveReplica;
import edu.umass.cs.gns.nsdesign.replicaController.ReplicaController;
import org.json.JSONObject;

/**
 * Work in progress. Inactive code.
 *
 * This class represents a name server in GNS. It contains an ActiveReplica and  a ReplicaController object
 * to handle functionality related to active replica and replica controller respectively.
 *
 * Created by abhigyan on 2/26/14.
 */
public class NameServerInterface {

  ActiveReplica activeReplica;

  ReplicaController replicaController;

  // nio server

  // executor service

  public NameServerInterface() {

    // create nio server

    // create executor service

    // create active replica (nio, executor)
    // create replica controller object (nio, executor)





  }

  /**
   * Entry point for all packets received at name server.
   * @param json
   */
  public void handleIncomingPacket(JSONObject json) {
    // based on the packet type it forwards to active replica
    // or replica controller

  }


}
