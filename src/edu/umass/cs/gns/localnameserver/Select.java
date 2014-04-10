/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.clientsupport.Intercessor;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.packet.SelectRequestPacket;
import edu.umass.cs.gns.nsdesign.packet.SelectResponsePacket;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.UnknownHostException;
import java.util.Set;

//import edu.umass.cs.gns.util.BestServerSelection;
//import edu.umass.cs.gns.util.ConfigFileInfo;
/**
 * Handles sending and receiving of queries.
 *
 * @author westy
 */
public class Select {

  public static void handlePacketSelectRequest(JSONObject incomingJSON) throws JSONException, UnknownHostException {

    SelectRequestPacket packet = new SelectRequestPacket(incomingJSON);

    int queryId = LocalNameServer.addSelectInfo(packet.getKey(), packet);
    packet.setLnsQueryId(queryId);
    JSONObject outgoingJSON = packet.toJSONObject();
    // Pick one NS to send it to
    // This should pick a Nameserver using the same method as a query!!
    int serverID = pickNameServer(packet.getGuid());
    GNS.getLogger().fine("LNS" + LocalNameServer.getNodeID() + " transmitting QueryRequest " + outgoingJSON + " to " + serverID);
    LocalNameServer.sendToNS(outgoingJSON, serverID);
  }

  private static int pickNameServer(String guid) {
    if (guid != null) {
      CacheEntry cacheEntry = LocalNameServer.getCacheEntry(guid);
      if (cacheEntry != null && cacheEntry.getActiveNameServers() != null && !cacheEntry.getActiveNameServers().isEmpty()) {
        int id = LocalNameServer.getGnsNodeConfig().getClosestServer(cacheEntry.getActiveNameServers());
        if (id != GNSNodeConfig.INVALID_NAME_SERVER_ID) {
          return id;
        }
      }
    }
    return LocalNameServer.getGnsNodeConfig().getClosestServer(LocalNameServer.getGnsNodeConfig().getAllNameServerIDs());
  }

  public static void handlePacketSelectResponse(JSONObject json) throws JSONException {
    GNS.getLogger().finer("LNS" + LocalNameServer.getNodeID() + " recvd QueryResponse: " + json);
    SelectResponsePacket packet = new SelectResponsePacket(json);
    GNS.getLogger().fine("LNS" + LocalNameServer.getNodeID() + " recvd from NS" + packet.getNameServer());
    SelectInfo info = LocalNameServer.getSelectInfo(packet.getLnsQueryId());
    // send a response back to the client
    Intercessor.handleIncomingPackets(packet.toJSONObject());
    LocalNameServer.removeSelectInfo(packet.getLnsQueryId());
  }
}
