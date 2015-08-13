/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.newApp.AppReconfigurableNodeOptions;
import edu.umass.cs.gns.newApp.clientCommandProcessor.EnhancedClientRequestHandlerInterface;
import edu.umass.cs.gns.newApp.packet.SelectRequestPacket;
import edu.umass.cs.gns.newApp.packet.SelectResponsePacket;
import org.json.JSONException;
import org.json.JSONObject;
import java.net.UnknownHostException;

/**
 * Handles sending and receiving of queries.
 *
 * @author westy
 */
public class Select {

  public static void handlePacketSelectRequest(JSONObject incomingJSON, EnhancedClientRequestHandlerInterface handler) throws JSONException, UnknownHostException {

    SelectRequestPacket<String> packet = new SelectRequestPacket<String>(incomingJSON, handler.getGnsNodeConfig());

    int queryId = handler.addSelectInfo(packet.getKey(), packet);
    packet.setCCPQueryId(queryId);
    JSONObject outgoingJSON = packet.toJSONObject();
    String serverID = pickNameServer(packet.getGuid(), handler);
    if (AppReconfigurableNodeOptions.debuggingEnabled) {
      GNS.getLogger().fine("LNS" + handler.getNodeAddress() + " transmitting QueryRequest " + outgoingJSON + " to " + serverID.toString());
    }
//    if (AppReconfigurableNodeOptions.debuggingEnabled) {
//      GNS.getLogger().fine("CCP" + handler.getNodeAddress() + " adding QueryRequest " + packet + " to queue.");
//    }
    //handler.getApp().handleRequest(packet);
    handler.sendToNS(outgoingJSON, serverID);
  }

  // This should pick a Nameserver using the same method as a query!!
  private static String pickNameServer(String guid, ClientRequestHandlerInterface handler) {
    if (guid != null) {
      CacheEntry<String> cacheEntry = handler.getCacheEntry(guid);
      if (cacheEntry != null && cacheEntry.getActiveNameServers() != null
              && !cacheEntry.getActiveNameServers().isEmpty()) {
        String id = handler.getGnsNodeConfig().getClosestServer(cacheEntry.getActiveNameServers());
        if (id != null) {
          return id;
        }
      }
    }
    return handler.getGnsNodeConfig().getClosestServer(handler.getGnsNodeConfig().getActiveReplicas());
  }

  public static void handlePacketSelectResponse(JSONObject json, ClientRequestHandlerInterface handler) throws JSONException {
    if (AppReconfigurableNodeOptions.debuggingEnabled) {
      GNS.getLogger().fine("LNS" + handler.getNodeAddress() + " recvd QueryResponse: " + json);
    }
    SelectResponsePacket<String> packet = new SelectResponsePacket<String>(json, handler.getGnsNodeConfig());
    //SelectInfo info = handler.getSelectInfo(packet.getCcpQueryId());
    // send a response back to the client
    handler.getIntercessor().handleIncomingPacket(packet.toJSONObject());
    handler.removeSelectInfo(packet.getLnsQueryId());
  }
}
