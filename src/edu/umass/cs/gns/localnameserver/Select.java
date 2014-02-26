/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.clientsupport.Intercessor;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.packet.SelectRequestPacket;
import edu.umass.cs.gns.packet.SelectResponsePacket;
import edu.umass.cs.gns.packet.Transport;
import edu.umass.cs.gns.util.BestServerSelection;
import edu.umass.cs.gns.util.ConfigFileInfo;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Set;

/**
 * Handles sending and receiving of queries.
 * 
 * @author westy
 */
public class Select {

  public static void handlePacketSelectRequest(JSONObject incomingJSON) throws JSONException, UnknownHostException {

    SelectRequestPacket packet = new SelectRequestPacket(incomingJSON);

    //extract the address and port so we can send a reponse back to the client
    InetAddress address = null;
    int port = Transport.getReturnPort(incomingJSON);
    if (port > 0 && Transport.getReturnAddress(incomingJSON) != null) {
      address = InetAddress.getByName(Transport.getReturnAddress(incomingJSON));
    }
    Set<Integer> serverIds = ConfigFileInfo.getAllNameServerIDs();
    int queryId = LocalNameServer.addQueryInfo(packet.getKey(), packet, address, port);
    packet.setLnsQueryId(queryId);
    JSONObject outgoingJSON = packet.toJSONObject();
    // Pick one NS to send it to

    GNS.getLogger().info("LNS" + LocalNameServer.nodeID + " potential servers are " + serverIds.toString());
    int serverID = BestServerSelection.simpleLatencyLoadHeuristic(serverIds);
    // above might return -1 if the configuration info is not set.. so pick one randomly
    if (serverID == -1) {
      serverID = BestServerSelection.randomServer(serverIds);
    }
    GNS.getLogger().info("LNS" + LocalNameServer.nodeID + " transmitting QueryRequest " + outgoingJSON + " to " + serverID);
    LocalNameServer.sendToNS(outgoingJSON, serverID);

  }

  public static void handlePacketSelectResponse(JSONObject json) throws JSONException {
    GNS.getLogger().finer("LNS" + LocalNameServer.nodeID + " recvd QueryResponse: " + json);
    SelectResponsePacket packet = new SelectResponsePacket(json);
    GNS.getLogger().fine("LNS" + LocalNameServer.nodeID + " recvd from NS" + packet.getNameServer());
    SelectInfo info = LocalNameServer.getQueryInfo(packet.getLnsQueryId());
    // send a response back to the client
    Intercessor.handleIncomingPackets(packet.toJSONObject());
    LocalNameServer.removeQueryInfo(packet.getLnsQueryId());
  }
}