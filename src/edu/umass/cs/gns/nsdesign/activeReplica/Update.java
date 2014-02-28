package edu.umass.cs.gns.nsdesign.activeReplica;

import edu.umass.cs.gns.packet.UpdateAddressPacket;

/*** DONT not use any class in package edu.umass.cs.gns.nsdesign ***/
/**
 *
 * Contains code for executing an address update locally at each active replica. If name servers are replicated,
 * then methods in this class will be executed after  the coordination among active replicas at name servers
 * is complete.
 *
 * Created by abhigyan on 2/27/14.
 */
public class Update {

  public static void executeUpdateLocal(UpdateAddressPacket packet, ActiveReplica replica) {

  }
}
