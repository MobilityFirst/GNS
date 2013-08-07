package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.packet.ActiveNameServerInfoPacket;
import edu.umass.cs.gns.packet.AdminRequestPacket;
import edu.umass.cs.gns.packet.DumpRequestPacket;
import edu.umass.cs.gns.packet.Packet;
import edu.umass.cs.gns.packet.SentinalPacket;
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
import org.json.JSONException;
import org.json.JSONObject;

/**
 * *************************************************************
 * This class implements a thread that returns a list of active name servers for a name. The thread waits for request packet over a
 * UDP socket and sends a response containing the current active nameserver for a name record.
 *
 * @author Westy ************************************************************
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
   * *************************************************************
   * Creates a new listener thread for handling response packet
   *
   * @throws IOException ************************************************************
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
   * *************************************************************
   * Start executing the thread. ************************************************************
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
      try {
        switch (Packet.getPacketType(incomingJSON)) {
          case DUMP_REQUEST:
            DumpRequestPacket dumpRequestPacket = new DumpRequestPacket(incomingJSON);
            if (dumpRequestPacket.getPrimaryNameServer() == -1) {

              // OUTGOING - multicast it to all the nameservers
              GNS.getLogger().fine("ListenerAdmin: Request from " + socket.getInetAddress().getHostName() + " port: " + socket.getLocalPort());
              int id = nextID();
              hostMap.put(id, socket.getInetAddress());
              dumpRequestPacket.setId(id);
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
              InetAddress host = hostMap.get(incomingId);
              Socket socketOut = new Socket(host, ConfigFileInfo.getDumpReponsePort(LocalNameServer.nodeID));
              Packet.sendTCPPacket(dumpRequestPacket.toJSONObject(), socketOut);
              GNS.getLogger().fine("ListenerAdmin: Relayed response for " + incomingId + " --> " + dumpRequestPacket.toJSONObject());
              int remaining = replicationMap.get(incomingId);
              remaining = remaining - 1;
              if (remaining > 0) {
                replicationMap.put(incomingId, remaining);
              } else {
                GNS.getLogger().fine("ListenerAdmin: Saw last response for " + incomingId);
                replicationMap.remove(incomingId);
                hostMap.remove(incomingId);
                // send a sentinal
                Socket socketOutAgain = new Socket(host, ConfigFileInfo.getDumpReponsePort(LocalNameServer.nodeID));
                Packet.sendTCPPacket(new SentinalPacket().toJSONObject(), socketOutAgain);
//                try {
//                  socketOut.close();
//                } catch (IOException e) {
//                  GNRS.getLogger().warning("Error closing socket: " + e);
//                  e.printStackTrace();
//                }
              }
            }
            break;
          case ADMIN_REQUEST:
            AdminRequestPacket adminRequestPacket = new AdminRequestPacket(incomingJSON);
            switch (adminRequestPacket.getOperation()) {
              case DELETEALLRECORDS:
              case RESETDB:
                GNS.getLogger().fine("LNSListenerAdmin (" + LocalNameServer.nodeID + ") "
                        + ": Forwarding " + adminRequestPacket.getOperation().toString() + " request");
                Set<Integer> serverIds = ConfigFileInfo.getAllNameServerIDs();
                Packet.multicastTCP(serverIds, incomingJSON, 2, GNS.PortType.NS_ADMIN_PORT);
                // clear the cache
                LocalNameServer.invalidateCache();
                break;  
              case CLEARCACHE:
                GNS.getLogger().fine("LNSListenerAdmin (" + LocalNameServer.nodeID + ") Clearing Cache as requested");
                LocalNameServer.invalidateCache();
                break;
//              case DELETEALLGUIDRECORDS:
//                // delete all the records that have a name (GUID) given by the argument in the packet
//                GNS.getLogger().fine("LNSListenerAdmin (" + LocalNameServer.nodeID + ") "
//                        + ": Forwarding " + adminRequestPacket.getOperation().toString()
//                        + " name " + adminRequestPacket.getArgument() + " request");
//                serverIds = ConfigFileInfo.getAllNameServerIDs();
//                Packet.multicastTCP(serverIds, incomingJSON, 2, GNS.PortType.NS_ADMIN_PORT);
//                // do something with the cache, but this is a sledgehammer when we need a gentle tap
//                LocalNameServer.invalidateCache();
//                break;
            }
            break;
          case STATUS_INIT:
            StatusClient.handleStatusInit(socket.getInetAddress());
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
      try {
        socket.close();
      } catch (IOException e) {
        GNS.getLogger().warning("Error closing socket: " + e);
        e.printStackTrace();
      }
    }

  }

//  /**
//   * *************************************************************
//   * Sends active name server information to the sender
//   *
//   * @param activeNSInfoPacket
//   * @param socket
//   * @param numRequest
//   * @throws IOException
//   * @throws JSONException ************************************************************
//   */
//  private void sendactiveNameServerInfo(ActiveNameServerInfoPacket activeNSInfoPacket,
//          Socket socket, int numRequest) throws IOException, JSONException {
//    activeNSInfoPacket.setActiveNameServers(NameServer.getActiveNameServers(activeNSInfoPacket.getName()//, activeNSInfoPacket.getRecordKey()
//            ));
//    activeNSInfoPacket.setPrimaryNameServer(NameServer.nodeID);
//    Packet.sendTCPPacket(activeNSInfoPacket.toJSONObject(), socket);
//    GNS.getLogger().fine("ListenerAdminRequest: Response RequestNum:" + numRequest + " --> " + activeNSInfoPacket.toString());
//  }
}
