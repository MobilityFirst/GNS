/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.clientsupport.Admintercessor;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.nsdesign.packet.admin.*;
import edu.umass.cs.gns.ping.PingManager;
import edu.umass.cs.gns.statusdisplay.StatusClient;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;

/**
 * A separate thread that runs in the LNS that handles administrative (AKA non-data related, non-user)
 * type operations. All of the things in here are for server administration and debugging.
 *
 * @author Westy
 */
public class LNSListenerAdmin extends Thread {

  /**
   * Socket over which active name server request arrive *
   */
  private ServerSocket serverSocket;
  private static Random randomID;
  /**
   * Keeps track of which responses socket should be used for which request *
   */
  private static Map<Integer, InetAddress> hostMap;
  /**
   * Keeps track of how many responses are outstanding for a request *
   */
  private static Map<Integer, Integer> replicationMap;

  /**
   *
   * Creates a new listener thread for handling response packet
   *
   * @throws IOException
   */
  public LNSListenerAdmin() throws IOException {
    super("ListenerAdmin");
    this.serverSocket = new ServerSocket(GNS.DEFAULT_LNS_ADMIN_PORT);
    randomID = new Random(System.currentTimeMillis());;
    replicationMap = new HashMap<Integer, Integer>();
  }

  /**
   *
   * Start executing the thread.
   */
  @Override
  public void run() {
    int numRequest = 0;
    GNS.getLogger().info("LNS Node " + LocalNameServer.getAddress() + " starting Admin Server on port " + serverSocket.getLocalPort());
    while (true) {
      Socket socket;
      JSONObject incomingJSON;
      try {
        socket = serverSocket.accept();
        //Read the packet from the input stream
        incomingJSON = Packet.getJSONObjectFrame(socket);
      } catch (Exception e) {
        GNS.getLogger().warning("Ignoring error accepting socket connection: " + e);
        e.printStackTrace();
        continue;
      }
      handlePacket(incomingJSON, socket);
      try {
        socket.close();
      } catch (IOException e) {
        GNS.getLogger().warning("Error closing socket: " + e);
        e.printStackTrace();
      }
    }

  }

  public static void handlePacket(JSONObject incomingJSON, Socket incomingSocket) {
    try {
      switch (Packet.getPacketType(incomingJSON)) {
        case DUMP_REQUEST:
          DumpRequestPacket dumpRequestPacket = new DumpRequestPacket(incomingJSON);
          if (dumpRequestPacket.getPrimaryNameServer().equals(GNSNodeConfig.INVALID_NAME_SERVER_ID)) {
            // OUTGOING - multicast it to all the nameservers
            int id = dumpRequestPacket.getId();
            GNS.getLogger().fine("ListenerAdmin: Request from local HTTP server");
            //dumpRequestPacket.setId(id);
            //dumpRequestPacket.setLnsAddress(LocalNameServer.getAddress()); // done before it gets here
            JSONObject json = dumpRequestPacket.toJSONObject();
            Set<String> serverIds = LocalNameServer.getGnsNodeConfig().getNodeIDs();
            replicationMap.put(id, serverIds.size());
            Packet.multicastTCP(LocalNameServer.getGnsNodeConfig(), serverIds, json, 2, GNS.PortType.NS_ADMIN_PORT);
            GNS.getLogger().fine("ListenerAdmin: Multicast out to " + serverIds.size() + " hosts for " + id + " --> " + dumpRequestPacket.toString());
          } else {
            // INCOMING - send it out to original requester
            DumpRequestPacket incomingPacket = new DumpRequestPacket(incomingJSON);
            int incomingId = incomingPacket.getId();
            Admintercessor.handleIncomingDumpResponsePackets(incomingJSON);
            GNS.getLogger().fine("ListenerAdmin: Relayed response for " + incomingId + " --> " + dumpRequestPacket.toJSONObject());
            int remaining = replicationMap.get(incomingId);
            remaining = remaining - 1;
            if (remaining > 0) {
              replicationMap.put(incomingId, remaining);
            } else {
              GNS.getLogger().fine("ListenerAdmin: Saw last response for " + incomingId);
              replicationMap.remove(incomingId);
              SentinalPacket sentinelPacket = new SentinalPacket(incomingId);
              Admintercessor.handleIncomingDumpResponsePackets(sentinelPacket.toJSONObject());
            }
          }
          break;
        case ADMIN_REQUEST:
          AdminRequestPacket incomingPacket = new AdminRequestPacket(incomingJSON);
          switch (incomingPacket.getOperation()) {
            // Calls remove record on every record
            case DELETEALLRECORDS:
            // Clears the database and reinitializes all indices.
            case RESETDB:
              GNS.getLogger().fine("LNSListenerAdmin (" + LocalNameServer.getAddress() + ") "
                      + ": Forwarding " + incomingPacket.getOperation().toString() + " request");
              Set<String> serverIds = LocalNameServer.getGnsNodeConfig().getNodeIDs();
              Packet.multicastTCP(LocalNameServer.getGnsNodeConfig(), serverIds, incomingJSON, 2, GNS.PortType.NS_ADMIN_PORT);
              // clear the cache
              LocalNameServer.invalidateCache();
              break;
            case CLEARCACHE:
              GNS.getLogger().fine("LNSListenerAdmin (" + LocalNameServer.getAddress() + ") Clearing Cache as requested");
              LocalNameServer.invalidateCache();
              break;
            case DUMPCACHE:
              JSONObject jsonResponse = new JSONObject();
              jsonResponse.put("CACHE", LocalNameServer.getCacheLogString("CACHE:\n"));
              AdminResponsePacket responsePacket = new AdminResponsePacket(incomingPacket.getId(), jsonResponse);
              Admintercessor.handleIncomingAdminResponsePackets(responsePacket.toJSONObject());
              break;
            case PINGTABLE:
              String node = new String(incomingPacket.getArgument());
              // -1 means return the LNS data
              if (node.equals(GNSNodeConfig.INVALID_NAME_SERVER_ID) || LocalNameServer.getGnsNodeConfig().getNodeIDs().contains(node)) {
                if (node.equals(GNSNodeConfig.INVALID_NAME_SERVER_ID)) {
                  jsonResponse = new JSONObject();
                  jsonResponse.put("PINGTABLE", LocalNameServer.getPingManager().tableToString(PingManager.LOCALNAMESERVERID));
                  // send a response back to where the request came from
                  responsePacket = new AdminResponsePacket(incomingPacket.getId(), jsonResponse);
                  returnResponsePacketToSender(incomingPacket.getLnsAddress(), responsePacket);
                } else {
                incomingPacket.setLnsAddress(new InetSocketAddress(LocalNameServer.getAddress().getAddress(), GNS.DEFAULT_LNS_ADMIN_PORT));
                //incomingPacket.setLocalNameServerId(LocalNameServer.getNodeID()); // so the receiver knows where to return it
                Packet.sendTCPPacket(LocalNameServer.getGnsNodeConfig(), incomingPacket.toJSONObject(), node, GNS.PortType.NS_ADMIN_PORT);
                }
              } else { // the incoming packet contained an invalid host number
                jsonResponse = new JSONObject();
                jsonResponse.put("ERROR", "Bad host number");
                responsePacket = new AdminResponsePacket(incomingPacket.getId(), jsonResponse);
                returnResponsePacketToSender(incomingPacket.getLnsAddress(), responsePacket);
                //returnResponsePacketToSender(incomingPacket.getLocalNameServerId(), responsePacket);
              }
              break;
            case PINGVALUE:
              String node1 = new String(incomingPacket.getArgument());
              String node2 = new String(incomingPacket.getArgument2());
              // -1 means return the LNS data
              if (node1.equals(GNSNodeConfig.INVALID_NAME_SERVER_ID) || LocalNameServer.getGnsNodeConfig().nodeExists(node1)
                      && LocalNameServer.getGnsNodeConfig().nodeExists(node2)) {
                if (node1.equals(GNSNodeConfig.INVALID_NAME_SERVER_ID)) {
                  // handle it here
                  jsonResponse = new JSONObject();
                  jsonResponse.put("PINGVALUE", LocalNameServer.getPingManager().nodeAverage(node2));
                  // send a response back to where the request came from
                  responsePacket = new AdminResponsePacket(incomingPacket.getId(), jsonResponse);
                  returnResponsePacketToSender(incomingPacket.getLnsAddress(), responsePacket);
                } else {
                // send it to the server that can handle it
                incomingPacket.setLnsAddress(new InetSocketAddress(LocalNameServer.getAddress().getAddress(), GNS.DEFAULT_LNS_ADMIN_PORT));
                //incomingPacket.setLocalNameServerId(LocalNameServer.getNodeID()); // so the receiver knows where to return it
                Packet.sendTCPPacket(LocalNameServer.getGnsNodeConfig(), incomingPacket.toJSONObject(), node1, GNS.PortType.NS_ADMIN_PORT);
                }
              } else { // the incoming packet contained an invalid host number
                jsonResponse = new JSONObject();
                jsonResponse.put("ERROR", "Bad host number");
                responsePacket = new AdminResponsePacket(incomingPacket.getId(), jsonResponse);
                returnResponsePacketToSender(incomingPacket.getLnsAddress(), responsePacket);
                //returnResponsePacketToSender(incomingPacket.getLocalNameServerId(), responsePacket);
              }
              break;
            case CHANGELOGLEVEL:
              Level level = Level.parse(incomingPacket.getArgument());
              GNS.getLogger().info("Changing log level to " + level.getName());
              GNS.getLogger().setLevel(level);
              // send it on to the NSs
              GNS.getLogger().fine("LNSListenerAdmin (" + LocalNameServer.getAddress() + ") "
                      + ": Forwarding " + incomingPacket.getOperation().toString() + " request");
              serverIds = LocalNameServer.getGnsNodeConfig().getNodeIDs();
              Packet.multicastTCP(LocalNameServer.getGnsNodeConfig(), serverIds, incomingJSON, 2, GNS.PortType.NS_ADMIN_PORT);
              break;
            default:
              GNS.getLogger().severe("Unknown admin request in packet: " + incomingJSON);
              break;
          }
          break;
        case ADMIN_RESPONSE:
          // forward and admin response packets recieved from NSs back to client
          AdminResponsePacket responsePacket = new AdminResponsePacket(incomingJSON);
          Admintercessor.handleIncomingAdminResponsePackets(responsePacket.toJSONObject());
          break;
        case STATUS_INIT:
          StatusClient.handleStatusInit(incomingSocket.getInetAddress());
          // FIXME: Should send address instead for LNSs (fix other end of this)
          StatusClient.sendStatus(GNSNodeConfig.INVALID_NAME_SERVER_ID, "LNS Ready");
          break;
        default:
          GNS.getLogger().severe("Unknown packet type in packet: " + incomingJSON);
          break;
      }
    } catch (Exception e) {
      GNS.getLogger().warning("Ignoring error handling packets: " + e);
      e.printStackTrace();
    }
  }

  private static void returnResponsePacketToSender(InetSocketAddress address, AdminResponsePacket packet) throws IOException, JSONException {
    if (address == null) {
      // it came from our client
      Admintercessor.handleIncomingAdminResponsePackets(packet.toJSONObject());
    } else {
      // it came from another LNS
      Packet.sendTCPPacket(packet.toJSONObject(), address);
      //Packet.sendTCPPacket(LocalNameServer.getGnsNodeConfig(), packet.toJSONObject(), senderId, GNS.PortType.LNS_ADMIN_PORT);
    }
  }
}
