/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Abhigyan Sharma, Westy
 *
 */
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport;

import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.gnsApp.AppReconfigurableNodeOptions;
import edu.umass.cs.gnsserver.gnsApp.packet.SelectRequestPacket;
import edu.umass.cs.gnsserver.gnsApp.packet.SelectResponsePacket;
import org.json.JSONException;
import org.json.JSONObject;
import java.net.UnknownHostException;

/**
 * Handles sending and receiving of queries.
 *
 * @author westy
 */
public class Select {

  /**
   * Handles the select request packet coming from a client.
   * 
   * @param incomingJSON
   * @param handler
   * @throws JSONException
   * @throws UnknownHostException
   */
  public static void handlePacketSelectRequest(JSONObject incomingJSON, ClientRequestHandlerInterface handler) throws JSONException, UnknownHostException {

    SelectRequestPacket<String> packet = new SelectRequestPacket<String>(incomingJSON, handler.getGnsNodeConfig());

    int queryId = handler.addSelectInfo(packet.getKey(), packet);
    packet.setCCPQueryId(queryId);
    JSONObject outgoingJSON = packet.toJSONObject();
    String serverID = pickNameServer(packet.getGuid(), handler);
    if (AppReconfigurableNodeOptions.debuggingEnabled) {
      GNS.getLogger().info("CCP" + handler.getNodeAddress() + " transmitting QueryRequest " + outgoingJSON + " to " + serverID.toString());
    }
//    if (AppReconfigurableNodeOptions.debuggingEnabled) {
//      GNS.getLogger().fine("CCP" + handler.getNodeAddress() + " adding QueryRequest " + packet + " to queue.");
//    }
    //handler.getApp().handleRequest(packet);
    handler.sendToNS(outgoingJSON, serverID);
  }

  // This should pick a Nameserver using the same method as a query!!
  private static String pickNameServer(String guid, ClientRequestHandlerInterface handler) {
    return handler.getGnsNodeConfig().getClosestServer(handler.getGnsNodeConfig().getActiveReplicas());
  }

  /**
   * Handles the responding to a select request packet being sent back to a client.
   * 
   * @param json
   * @param handler
   * @throws JSONException
   */
  public static void handlePacketSelectResponse(JSONObject json, ClientRequestHandlerInterface handler) throws JSONException {
    if (AppReconfigurableNodeOptions.debuggingEnabled) {
      GNS.getLogger().info("LNS" + handler.getNodeAddress() + " recvd QueryResponse: " + json);
    }
    SelectResponsePacket<String> packet = new SelectResponsePacket<String>(json, handler.getGnsNodeConfig());
    //SelectInfo info = handler.getSelectInfo(packet.getCcpQueryId());
    // send a response back to the client
    handler.getIntercessor().handleIncomingPacket(packet.toJSONObject());
    handler.removeSelectInfo(packet.getLnsQueryId());
  }
}
