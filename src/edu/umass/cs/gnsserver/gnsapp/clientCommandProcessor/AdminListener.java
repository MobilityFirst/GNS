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
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor;

import edu.umass.cs.gnsserver.utils.Shutdownable;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet;
import edu.umass.cs.gnsserver.gnsapp.packet.admin.AdminRequestPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.admin.AdminResponsePacket;
import edu.umass.cs.gnsserver.gnsapp.packet.admin.DumpRequestPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.admin.SentinalPacket;

import edu.umass.cs.gnsserver.nodeconfig.PortOffsets;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

/**
 * Handles administrative (AKA non-data( type operations. 
 * All of the things in here are for server administration and debugging.
 * Called directly by the client support code. Also runs a listener that
 * server nodes send things back to. 
 *
 * @author Westy
 */
public class AdminListener extends Thread implements Shutdownable {

  /**
   * Socket over which active name server request arrive. *
   */
  private ServerSocket serverSocket;
  /**
   * Keeps track of how many responses are outstanding for a request *
   */
  private final Map<Integer, Integer> replicationMap;

  private final ClientRequestHandlerInterface handler;

  /**
   *
   * Creates a new listener thread for handling response packet
   *
   * @param handler
   * @throws IOException
   */
  public AdminListener(ClientRequestHandlerInterface handler) throws IOException {
    super("ListenerAdmin");
    this.serverSocket
            = new ServerSocket(handler.getGnsNodeConfig().getCollatingAdminPort(handler.getActiveReplicaID()));
    replicationMap = new HashMap<>();
    this.handler = handler;
  }

  /**
   * Start executing the thread.
   */
  @Override
  public void run() {
    ClientCommandProcessorConfig.getLogger().log(Level.INFO,
            "Server Node {0} starting Admin Server on port {1}", 
            new Object[]{handler.getNodeAddress(), 
              serverSocket.getLocalPort()});
    while (true) {
      Socket socket;
      JSONObject incomingJSON;
      try {
        socket = serverSocket.accept();
        //Read the packet from the input stream
        incomingJSON = Packet.getJSONObjectFrame(socket);
      } catch (IOException | JSONException e) {
        ClientCommandProcessorConfig.getLogger().log(Level.WARNING,
                "Ignoring error accepting socket connection: {0}", e);
        e.printStackTrace();
        continue;
      }
      handlePacket(incomingJSON, socket, handler);
      try {
        socket.close();
      } catch (IOException e) {
        ClientCommandProcessorConfig.getLogger().log(Level.WARNING, "Error closing socket: {0}", e);
        e.printStackTrace();
      }
    }
  }

  /**
   * Handle an incoming admin packet.
   *
   * @param incomingJSON
   * @param incomingSocket
   * @param handler
   */
  public void handlePacket(JSONObject incomingJSON, Socket incomingSocket, ClientRequestHandlerInterface handler) {
    try {
      switch (Packet.getPacketType(incomingJSON)) {
        case DUMP_REQUEST:
          DumpRequestPacket<String> dumpRequestPacket = new DumpRequestPacket<>(incomingJSON, handler.getGnsNodeConfig());
          if (dumpRequestPacket.getPrimaryNameServer() == null) {
            // OUTGOING - multicast it to all the nameservers
            int id = dumpRequestPacket.getId();
            ClientCommandProcessorConfig.getLogger().info("ListenerAdmin: Dump request from local server");
            JSONObject json = dumpRequestPacket.toJSONObject();
            Set<String> serverIds = handler.getNodeConfig().getActiveReplicas();
            //Set<NodeIDType> serverIds = handler.getGnsNodeConfig().getNodeIDs();
            replicationMap.put(id, serverIds.size());
            Packet.multicastTCP(handler.getGnsNodeConfig(), serverIds, json, 2, PortOffsets.SERVER_ADMIN_PORT, null);
            ClientCommandProcessorConfig.getLogger().log(Level.INFO, "ListenerAdmin: Multicast out to {0} hosts for {1} --> {2}", new Object[]{serverIds.size(), id, dumpRequestPacket.toString()});
          } else {
            // INCOMING - send it out to original requester
            DumpRequestPacket<String> incomingPacket = new DumpRequestPacket<>(incomingJSON, handler.getGnsNodeConfig());
            int incomingId = incomingPacket.getId();
            handler.getAdmintercessor().handleIncomingDumpResponsePackets(incomingJSON, handler);
            ClientCommandProcessorConfig.getLogger().log(Level.FINEST, "ListenerAdmin: Relayed response for {0} --> {1}", new Object[]{incomingId, dumpRequestPacket.toJSONObject()});
            int remaining = replicationMap.get(incomingId);
            remaining -= 1;
            if (remaining > 0) {
              replicationMap.put(incomingId, remaining);
            } else {
              ClientCommandProcessorConfig.getLogger().log(Level.INFO, "ListenerAdmin: Saw last response for {0}", incomingId);
              replicationMap.remove(incomingId);
              SentinalPacket sentinelPacket = new SentinalPacket(incomingId);
              handler.getAdmintercessor().handleIncomingDumpResponsePackets(sentinelPacket.toJSONObject(), handler);
            }
          }
          break;
        case ADMIN_REQUEST:
          AdminRequestPacket incomingPacket = new AdminRequestPacket(incomingJSON);
          switch (incomingPacket.getOperation()) {
            // deliberately nothing here for now
            default:
              ClientCommandProcessorConfig.getLogger().log(Level.SEVERE,
                      "Unknown admin request in packet: {0}", incomingJSON);
              break;
          }
          break;
        case ADMIN_RESPONSE:
          // forward any admin response packets recieved from NSs back to client
          AdminResponsePacket responsePacket = new AdminResponsePacket(incomingJSON);
          handler.getAdmintercessor().handleIncomingAdminResponsePackets(responsePacket.toJSONObject());
          break;
        default:
          ClientCommandProcessorConfig.getLogger().log(Level.SEVERE,
                  "Unknown packet type in packet: {0}", incomingJSON);
          break;
      }
    } catch (JSONException | IllegalArgumentException | SecurityException | ParseException e) {
      ClientCommandProcessorConfig.getLogger().log(Level.WARNING,
              "Ignoring error handling packets: {0}", e);
      e.printStackTrace();
    }
  }

  @Override
  public void shutdown() {
    try {
      this.serverSocket.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
