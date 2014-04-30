/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.clientsupport.Intercessor;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.packet.CommandPacket;
import org.json.JSONException;
import org.json.JSONObject;
import java.net.UnknownHostException;

/**
 * Handles sending and receiving of commands.
 *
 * @author westy
 */
public class CommandRequest {

  public static void handlePacketCommandRequest(JSONObject incomingJSON) throws JSONException, UnknownHostException {

    CommandPacket packet = new CommandPacket(incomingJSON);
    if (packet.getResponseCode() == null) {
      // PACKET IS GONE OUT TO A NAME SERVER
      // Pick one NS to send it to
      // This should pick a Nameserver using the same method as a query!!
      int serverID = LocalNameServer.getGnsNodeConfig().getClosestNameServer();
      GNS.getLogger().info("LNS" + LocalNameServer.getNodeID() + " transmitting CommandPacket " + incomingJSON + " to " + serverID);
      LocalNameServer.sendToNS(incomingJSON, serverID);
    } else {
      // PACKET IS COMING BACK FROM A NAMESERVER
       Intercessor.handleIncomingPackets(incomingJSON);
    }
  }
}
