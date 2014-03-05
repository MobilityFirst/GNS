/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.packet.Transport;
import edu.umass.cs.gns.util.ConfigFileInfo;
import java.io.IOException;
import java.util.ArrayList;
import org.json.JSONObject;

/**
 * Listens on a UDP port for requests from end-users, and responses from name servers.
 *
 * @author abhigyan
 *
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
      ArrayList<JSONObject> jsonObjects = new ArrayList<JSONObject>();
      jsonObjects.add(json);
      lnsPacketDemultiplexer.handleJSONObjects(jsonObjects);
    }
  }


}


