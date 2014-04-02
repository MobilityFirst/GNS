/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.packet.Transport;
import org.json.JSONObject;

import java.io.IOException;

/**
 * Listens on a UDP port for requests from end-users, and responses from name servers.
 *
 * @author abhigyan
 */
public class LNSListenerUDP extends Thread {

  public static Transport udpTransport;

  private static LNSPacketDemultiplexer lnsPacketDemultiplexer = new LNSPacketDemultiplexer();

  public LNSListenerUDP(GNSNodeConfig gnsNodeConfig) throws IOException {
    super("LNSListenerUDP");
    GNS.getLogger().info("LNS Node " + LocalNameServer.getNodeID() + " starting LNSListenerUDP on port " 
            + gnsNodeConfig.getLNSUdpPort(LocalNameServer.getNodeID()));
    udpTransport = new Transport(LocalNameServer.getNodeID(), gnsNodeConfig.getLNSUdpPort(LocalNameServer.getNodeID()));

  }

  @Override
  public void run() {
    while (true) {
      JSONObject json = udpTransport.readPacket();
      lnsPacketDemultiplexer.handleJSONObject(json);
    }
  }


}


