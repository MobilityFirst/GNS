/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.packet.Transport;
import edu.umass.cs.gns.util.ConfigFileInfo;
import org.json.JSONObject;

import java.io.IOException;

/**
 * Listens on a UDP port for requests from end-users, and responses from name servers.
 *
 * @author abhigyan
 * @deprecated
 */
public class LNSListenerUDP extends Thread {

  public static Transport udpTransport;

  private static LNSPacketDemultiplexer lnsPacketDemultiplexer = new LNSPacketDemultiplexer();

  public LNSListenerUDP() throws IOException {
    super("LNSListenerUDP");
    GNS.getLogger().info("LNS Node " + LocalNameServer.getNodeID() + " starting LNSListenerUDP on port " 
            + ConfigFileInfo.getLNSUdpPort(LocalNameServer.getNodeID()));
    udpTransport = new Transport(LocalNameServer.getNodeID(), ConfigFileInfo.getLNSUdpPort(LocalNameServer.getNodeID()));

  }

  @Override
  public void run() {
    while (true) {
      JSONObject json = udpTransport.readPacket();
      lnsPacketDemultiplexer.handleJSONObject(json);
    }
  }


}


