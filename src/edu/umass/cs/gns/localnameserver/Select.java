/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.packet.SelectRequestPacket;
import edu.umass.cs.gns.nsdesign.packet.SelectResponsePacket;
import org.json.JSONException;
import org.json.JSONObject;
import java.net.UnknownHostException;

/**
 * Handles sending and receiving of queries.
 *
 * @author westy
 */
public class Select {

  public static void handlePacketSelectRequest(JSONObject incomingJSON, ClientRequestHandlerInterface handler) throws JSONException, UnknownHostException {

    SelectRequestPacket packet = new SelectRequestPacket(incomingJSON);

    int queryId = handler.addSelectInfo(packet.getKey(), packet);
    packet.setLnsQueryId(queryId);
    JSONObject outgoingJSON = packet.toJSONObject();
    // Pick one NS to send it to
    // This should pick a Nameserver using the same method as a query!!
    Object serverID = pickNameServer(packet.getGuid(), handler);
    if (Config.debuggingEnabled) {
      GNS.getLogger().fine("LNS" + handler.getNodeAddress() + " transmitting QueryRequest " + outgoingJSON + " to " + serverID.toString());
    }
    handler.sendToNS(outgoingJSON, serverID);
  }

  private static Object pickNameServer(String guid, ClientRequestHandlerInterface handler) {
    if (guid != null) {
      CacheEntry cacheEntry = handler.getCacheEntry(guid);
      if (cacheEntry != null && cacheEntry.getActiveNameServers() != null && !cacheEntry.getActiveNameServers().isEmpty()) {
        Object id = handler.getGnsNodeConfig().getClosestServer(cacheEntry.getActiveNameServers());
        if (id != null) {
          return id;
        }
      }
    }
    return LocalNameServer.getGnsNodeConfig().getClosestServer(LocalNameServer.getGnsNodeConfig().getNodeIDs());
  }

  public static void handlePacketSelectResponse(JSONObject json, ClientRequestHandlerInterface handler) throws JSONException {
    if (Config.debuggingEnabled) {
      GNS.getLogger().finer("LNS" + handler.getNodeAddress() + " recvd QueryResponse: " + json);
    }
    SelectResponsePacket packet = new SelectResponsePacket(json);
    if (Config.debuggingEnabled) {
      GNS.getLogger().fine("LNS" + handler.getNodeAddress() + " recvd from NS" + packet.getNameServerID().toString());
    }
    SelectInfo info = handler.getSelectInfo(packet.getLnsQueryId());
    // send a response back to the client
    LocalNameServer.getIntercessorInterface().handleIncomingPacket(packet.toJSONObject());
    handler.removeSelectInfo(packet.getLnsQueryId());
  }
}
