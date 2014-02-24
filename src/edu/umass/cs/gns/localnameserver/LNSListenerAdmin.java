package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.client.Admintercessor;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.packet.admin.AdminRequestPacket;
import edu.umass.cs.gns.packet.admin.DumpRequestPacket;
import edu.umass.cs.gns.packet.Packet;
import edu.umass.cs.gns.packet.admin.AdminResponsePacket;
import edu.umass.cs.gns.packet.admin.SentinalPacket;
import edu.umass.cs.gns.statusdisplay.StatusClient;
import edu.umass.cs.gns.util.ConfigFileInfo;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import org.json.JSONObject;

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
    this.serverSocket = new ServerSocket(ConfigFileInfo.getLNSAdminRequestPort(LocalNameServer.nodeID));
    randomID = new Random(System.currentTimeMillis());
    hostMap = new HashMap<Integer, InetAddress>();
    replicationMap = new HashMap<Integer, Integer>();
  }

  private int nextID() {
    int id;
    do {
      id = randomID.nextInt();
    } while (hostMap.containsKey(id));
    return id;
  }

  /**
   *
   * Start executing the thread.
   */
  @Override
  public void run() {
    int numRequest = 0;
    GNS.getLogger().info("LNS Node " + LocalNameServer.nodeID + " starting Admin Server on port " + serverSocket.getLocalPort());
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
          if (dumpRequestPacket.getPrimaryNameServer() == -1) {
            // OUTGOING - multicast it to all the nameservers
            int id = dumpRequestPacket.getId();
            if (!StartLocalNameServer.runHttpServer) {
              GNS.getLogger().fine("ListenerAdmin: Request from " + incomingSocket.getInetAddress().getHostName() + " port: " + incomingSocket.getLocalPort());
              hostMap.put(id, incomingSocket.getInetAddress());
            } else {
              GNS.getLogger().fine("ListenerAdmin: Request from local HTTP server");
            }
            //dumpRequestPacket.setId(id);
            dumpRequestPacket.setLocalNameServer(LocalNameServer.nodeID);
            JSONObject json = dumpRequestPacket.toJSONObject();
            Set<Integer> serverIds = ConfigFileInfo.getAllNameServerIDs();
            replicationMap.put(id, serverIds.size());
            Packet.multicastTCP(serverIds, json, 2, GNS.PortType.NS_ADMIN_PORT);
            GNS.getLogger().fine("ListenerAdmin: Multicast out to " + serverIds.size() + " hosts for " + id + " --> " + dumpRequestPacket.toString());
          } else {
            // INCOMING - send it out to original requester

            DumpRequestPacket incomingPacket = new DumpRequestPacket(incomingJSON);
            int incomingId = incomingPacket.getId();
            if (StartLocalNameServer.runHttpServer) {
              Admintercessor.processDumpResponsePackets(incomingJSON);
            } else {
              InetAddress host = hostMap.get(incomingId);
              Socket socketOut = new Socket(host, ConfigFileInfo.getLNSAdminDumpReponsePort(LocalNameServer.nodeID));
              Packet.sendTCPPacket(dumpRequestPacket.toJSONObject(), socketOut);
            }
            GNS.getLogger().fine("ListenerAdmin: Relayed response for " + incomingId + " --> " + dumpRequestPacket.toJSONObject());
            int remaining = replicationMap.get(incomingId);
            remaining = remaining - 1;
            if (remaining > 0) {
              replicationMap.put(incomingId, remaining);
            } else {
              GNS.getLogger().fine("ListenerAdmin: Saw last response for " + incomingId);
              replicationMap.remove(incomingId);
              SentinalPacket sentinelPacket = new SentinalPacket(incomingId);
              if (StartLocalNameServer.runHttpServer) {
                Admintercessor.processDumpResponsePackets(sentinelPacket.toJSONObject());
              } else {
                InetAddress host = hostMap.get(incomingId);
                hostMap.remove(incomingId);
                // send a sentinal
                Socket socketOutAgain = new Socket(host, ConfigFileInfo.getLNSAdminDumpReponsePort(LocalNameServer.nodeID));
                Packet.sendTCPPacket(sentinelPacket.toJSONObject(), socketOutAgain);
              }
            }
          }
          break;
        case ADMIN_REQUEST:
          AdminRequestPacket incomingPacket = new AdminRequestPacket(incomingJSON);
          switch (incomingPacket.getOperation()) {
            case DELETEALLRECORDS:
            case RESETDB:
              GNS.getLogger().fine("LNSListenerAdmin (" + LocalNameServer.nodeID + ") "
                      + ": Forwarding " + incomingPacket.getOperation().toString() + " request");
              Set<Integer> serverIds = ConfigFileInfo.getAllNameServerIDs();
              Packet.multicastTCP(serverIds, incomingJSON, 2, GNS.PortType.NS_ADMIN_PORT);
              // clear the cache
              LocalNameServer.invalidateCache();
              break;
            case CLEARCACHE:
              GNS.getLogger().fine("LNSListenerAdmin (" + LocalNameServer.nodeID + ") Clearing Cache as requested");
              LocalNameServer.invalidateCache();
              break;
            case DUMPCACHE:
              JSONObject jsonResponse = new JSONObject();
              jsonResponse.put("CACHE", LocalNameServer.cacheLogString("CACHE:\n"));
              AdminResponsePacket responsePacket = new AdminResponsePacket(incomingPacket.getId(), jsonResponse);
              if (StartLocalNameServer.runHttpServer) {
                Admintercessor.processAdminResponsePackets(responsePacket.toJSONObject());
              } else {
                Socket socketOut = new Socket(incomingSocket.getInetAddress(), 
                        ConfigFileInfo.getLNSAdminResponsePort(LocalNameServer.nodeID));
                Packet.sendTCPPacket(responsePacket.toJSONObject(), socketOut);
              }
              break;
            case CHANGELOGLEVEL:
              Level level = Level.parse(incomingPacket.getArgument());
              GNS.getLogger().info("Changing log level to " + level.getName());
              GNS.getLogger().setLevel(level);
              // send it on to the NSs
              GNS.getLogger().fine("LNSListenerAdmin (" + LocalNameServer.nodeID + ") "
                      + ": Forwarding " + incomingPacket.getOperation().toString() + " request");
              serverIds = ConfigFileInfo.getAllNameServerIDs();
              Packet.multicastTCP(serverIds, incomingJSON, 2, GNS.PortType.NS_ADMIN_PORT);
              break;
            }
          break;
        case STATUS_INIT:
          StatusClient.handleStatusInit(incomingSocket.getInetAddress());
          StatusClient.sendStatus(LocalNameServer.nodeID, "LNS Ready");
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
}
