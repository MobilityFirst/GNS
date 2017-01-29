
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor;

import edu.umass.cs.gnsserver.gnsapp.packet.Packet;
import edu.umass.cs.gnsserver.gnsapp.packet.admin.AdminRequestPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.admin.AdminResponsePacket;
import edu.umass.cs.gnsserver.gnsapp.packet.admin.DumpRequestPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.admin.SentinalPacket;
import edu.umass.cs.gnsserver.main.OldHackyConstants;
import edu.umass.cs.gnsserver.utils.Shutdownable;
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


public class ListenerAdmin extends Thread implements Shutdownable {


  private ServerSocket serverSocket;

  private final Map<Integer, Integer> replicationMap;

  private final ClientRequestHandlerInterface handler;


  public ListenerAdmin(ClientRequestHandlerInterface handler) throws IOException {
    super("ListenerAdmin");
    this.serverSocket
            = new ServerSocket(handler.getGnsNodeConfig().getCcpAdminPort(handler.getActiveReplicaID()));
    replicationMap = new HashMap<>();
    this.handler = handler;
  }


  @Override
  public void run() {
    int numRequest = 0;
    ClientCommandProcessorConfig.getLogger().log(Level.INFO,
            "Server Node {0} starting Admin Server on port {1}", new Object[]{handler.getNodeAddress(), serverSocket.getLocalPort()});
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


  public void handlePacket(JSONObject incomingJSON, Socket incomingSocket, ClientRequestHandlerInterface handler) {
    try {
      switch (Packet.getPacketType(incomingJSON)) {
        case DUMP_REQUEST:
          DumpRequestPacket<String> dumpRequestPacket = new DumpRequestPacket<>(incomingJSON, handler.getGnsNodeConfig());
          if (dumpRequestPacket.getPrimaryNameServer() == null) {
            // OUTGOING - multicast it to all the nameservers
            int id = dumpRequestPacket.getId();
            ClientCommandProcessorConfig.getLogger().info("ListenerAdmin: Request from local HTTP server");
            JSONObject json = dumpRequestPacket.toJSONObject();
            Set<String> serverIds = handler.getNodeConfig().getActiveReplicas();
            //Set<NodeIDType> serverIds = handler.getGnsNodeConfig().getNodeIDs();
            replicationMap.put(id, serverIds.size());
            Packet.multicastTCP(handler.getGnsNodeConfig(), serverIds, json, 2, OldHackyConstants.PortType.NS_ADMIN_PORT, null);
            ClientCommandProcessorConfig.getLogger().log(Level.INFO, "ListenerAdmin: Multicast out to {0} hosts for {1} --> {2}", new Object[]{serverIds.size(), id, dumpRequestPacket.toString()});
          } else {
            // INCOMING - send it out to original requester
            DumpRequestPacket<String> incomingPacket = new DumpRequestPacket<>(incomingJSON, handler.getGnsNodeConfig());
            int incomingId = incomingPacket.getId();
            handler.getAdmintercessor().handleIncomingDumpResponsePackets(incomingJSON, handler);
            ClientCommandProcessorConfig.getLogger().log(Level.INFO, "ListenerAdmin: Relayed response for {0} --> {1}", new Object[]{incomingId, dumpRequestPacket.toJSONObject()});
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
            // Calls remove record on every record
            case DELETEALLRECORDS:
            // Clears the database and reinitializes all indices.
            case RESETDB:
              ClientCommandProcessorConfig.getLogger().log(Level.FINE, "LNSListenerAdmin ({0}" + ") " + ": Forwarding {1} request", new Object[]{handler.getNodeAddress(), incomingPacket.getOperation().toString()});
              Set<String> serverIds = handler.getNodeConfig().getActiveReplicas();
              //Set<NodeIDType> serverIds = handler.getGnsNodeConfig().getNodeIDs();
              Packet.multicastTCP(handler.getGnsNodeConfig(), serverIds, incomingJSON, 2, OldHackyConstants.PortType.NS_ADMIN_PORT, null);
              // clear the cache
              //handler.invalidateCache();
              break;
            case CHANGELOGLEVEL:
              Level level = Level.parse(incomingPacket.getArgument());
              ClientCommandProcessorConfig.getLogger().log(Level.INFO, "Changing log level to {0}",
                      level.getName());
              ClientCommandProcessorConfig.getLogger().setLevel(level);
              // send it on to the NSs
              ClientCommandProcessorConfig.getLogger().log(Level.FINE,
                      "LNSListenerAdmin ({0}" + ") " + ": Forwarding {1} request",
                      new Object[]{handler.getNodeAddress(), incomingPacket.getOperation().toString()});
              serverIds = handler.getNodeConfig().getActiveReplicas();
              //serverIds = handler.getGnsNodeConfig().getNodeIDs();
              Packet.multicastTCP(handler.getGnsNodeConfig(), serverIds,
                      incomingJSON, 2, OldHackyConstants.PortType.NS_ADMIN_PORT, null);
              break;
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
