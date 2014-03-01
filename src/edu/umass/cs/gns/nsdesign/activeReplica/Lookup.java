package edu.umass.cs.gns.nsdesign.activeReplica;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.packet.DNSPacket;

/**
 * This class executes lookup requests sent by an LNS to an active replica. If name servers are replicated,
 * then methods in this class will be executed after  the coordination among active replicas at name servers
 * is complete.
 *
 * Created by abhigyan on 2/27/14.
 */
public class Lookup {

  /**
   *
   * @param packet
   * @param activeReplica
   */
  public static void executeLookupLocal(DNSPacket packet, ActiveReplica activeReplica) {
    GNS.getLogger().info(" Processing LOOKUP: " + packet.toString());
  }
}
