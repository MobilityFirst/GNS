package edu.umass.cs.gns.nsdesign;

import edu.umass.cs.gns.clientsupport.AccountAccess;
import edu.umass.cs.gns.clientsupport.GuidInfo;
import edu.umass.cs.gns.database.BasicRecordCursor;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurable;
import edu.umass.cs.gns.nsdesign.packet.ActiveNameServerInfoPacket;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.nsdesign.packet.admin.AdminRequestPacket;
import edu.umass.cs.gns.nsdesign.packet.admin.AdminResponsePacket;
import edu.umass.cs.gns.nsdesign.packet.admin.DumpRequestPacket;
import edu.umass.cs.gns.nsdesign.recordmap.NameRecord;
import edu.umass.cs.gns.nsdesign.recordmap.ReplicaControllerRecord;
import edu.umass.cs.gns.nsdesign.replicaController.ReplicaController;
import edu.umass.cs.gns.replicaCoordination.ActiveReplicaCoordinator;
import edu.umass.cs.gns.replicaCoordination.ReplicaControllerCoordinator;
import edu.umass.cs.gns.statusdisplay.StatusClient;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.logging.Level;

/**
 * A separate thread that runs in the NameServer that handles administrative (AKA non-data related, non-user)
 * type operations. All of the things in here are for server administration and debugging.
 */
public class NSListenerAdmin extends Thread {

  /**
   * Socket over which active name server request arrive *
   */
  private ServerSocket serverSocket;

  private GnsReconfigurable gnsReconfigurable;

  private ActiveReplicaCoordinator appCoordinator;

  private ReplicaController replicaController;

  private ReplicaControllerCoordinator rcCooordinator;

  private GNSNodeConfig gnsNodeConfig;

  /**
   * Creates a new listener thread for handling response packet
   *
   * @throws IOException
   */
  public NSListenerAdmin(GnsReconfigurable gnsReconfigurable, ActiveReplicaCoordinator appCoordinator,
          ReplicaController replicaController, ReplicaControllerCoordinator rcCooordinator,
          GNSNodeConfig gnsNodeConfig) {
    super("NSListenerAdmin");
    this.gnsReconfigurable = gnsReconfigurable;
    this.appCoordinator = appCoordinator;
    this.replicaController = replicaController;
    this.gnsNodeConfig = gnsNodeConfig;
    try {
      this.serverSocket = new ServerSocket(gnsNodeConfig.getNSAdminRequestPort(gnsReconfigurable.getNodeID()));
    } catch (IOException e) {
      GNS.getLogger().severe("Unable to create NSListenerAdmin server: " + e);
    }
  }

  /**
   * Start executing the thread.
   */
  @Override
  public void run() {
    int numRequest = 0;
    GNS.getLogger().info("NS Node " + gnsReconfigurable.getNodeID() + " starting Admin Request Server on port " + serverSocket.getLocalPort());
    while (true) {
      try {
        Socket socket = serverSocket.accept();

        //Read the packet from the input stream
        JSONObject incomingJSON = Packet.getJSONObjectFrame(socket);
        switch (Packet.getPacketType(incomingJSON)) {

          case ACTIVE_NAMESERVER_INFO:

            ActiveNameServerInfoPacket activeNSInfoPacket = new ActiveNameServerInfoPacket(incomingJSON);

            GNS.getLogger().fine("NSListenrAdmin:: ListenerActiveNameServerInfo: Received RequestNum:" + (++numRequest) + " --> " + incomingJSON.toString());

            ReplicaControllerRecord nameRecordPrimary = null;
            try {
              nameRecordPrimary = ReplicaControllerRecord.getNameRecordPrimaryMultiField(replicaController.getDB(),
                      activeNSInfoPacket.getName(), ReplicaControllerRecord.ACTIVE_NAMESERVERS);
            } catch (RecordNotFoundException e) {
              e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
              break;
            }

            try {
              sendactiveNameServerInfo(activeNSInfoPacket, socket, numRequest, nameRecordPrimary.getActiveNameservers());
            } catch (FieldNotFoundException e) {
              GNS.getLogger().severe("Field not found exception. " + e.getMessage());
              e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
            break;

          case DUMP_REQUEST:

            DumpRequestPacket dumpRequestPacket = new DumpRequestPacket(incomingJSON);

            dumpRequestPacket.setPrimaryNameServer(gnsReconfigurable.getNodeID());

            StatusClient.sendStatus(gnsReconfigurable.getNodeID(), "Dumping records");
            JSONArray jsonArray = new JSONArray();
            // if there is an argument it is a TAGNAME we return all the records that have that tag
            if (dumpRequestPacket.getArgument() != null) {
              String tag = dumpRequestPacket.getArgument();
              BasicRecordCursor cursor = NameRecord.getAllRowsIterator(gnsReconfigurable.getDB());
              while (cursor.hasNext()) {
                NameRecord nameRecord = null;
                JSONObject json = cursor.next();
                try {
                  nameRecord = new NameRecord(gnsReconfigurable.getDB(), json);
                } catch (JSONException e) {
                  GNS.getLogger().severe("Problem parsing json into NameRecord: " + e + " JSON is " + json.toString());
                }
                if (nameRecord != null) {
                  try {
                    if (nameRecord.containsKey(AccountAccess.GUID_INFO)) {
                      GuidInfo userInfo = new GuidInfo(nameRecord.getKey(AccountAccess.GUID_INFO).toResultValueString());
                      if (userInfo.containsTag(tag)) {
                        jsonArray.put(nameRecord.toJSONObject());
                      }
                    }
                  } catch (FieldNotFoundException e) {
                    GNS.getLogger().severe("FieldNotFoundException. Field Name =  " + e.getMessage());
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                  }
                }
              }
              // OTHERWISE WE RETURN ALL THE RECORD
            } else {
              //for (NameRecord nameRecord : NameServer.getAllNameRecords()) {
              BasicRecordCursor cursor = NameRecord.getAllRowsIterator(gnsReconfigurable.getDB());
              while (cursor.hasNext()) {
                NameRecord nameRecord = null;
                JSONObject json = cursor.next();
                try {
                  nameRecord = new NameRecord(gnsReconfigurable.getDB(), json);
                } catch (JSONException e) {
                  GNS.getLogger().severe("Problem parsing record cursor into NameRecord: " + e + " JSON is " + json.toString());
                }
                if (nameRecord != null) {
                  jsonArray.put(nameRecord.toJSONObject());
                }
              }
            }
            if (GNS.getLogger().isLoggable(Level.FINER)) {
              GNS.getLogger().finer("NSListenrAdmin for " + gnsReconfigurable.getNodeID() + " is " + jsonArray.toString());
            }
            dumpRequestPacket.setJsonArray(jsonArray);
            Packet.sendTCPPacket(gnsNodeConfig, dumpRequestPacket.toJSONObject(), dumpRequestPacket.getLocalNameServer(), GNS.PortType.LNS_ADMIN_PORT);
            //Packet.sendTCPPacket(dumpRequestPacket.toJSONObject(), socket);
            GNS.getLogger().info("NSListenrAdmin: Response to id:" + dumpRequestPacket.getId() + " --> " + dumpRequestPacket.toString());
            break;
          case ADMIN_REQUEST:
            AdminRequestPacket adminRequestPacket = new AdminRequestPacket(incomingJSON);
            switch (adminRequestPacket.getOperation()) {
              case DELETEALLRECORDS:
                GNS.getLogger().fine("NSListenerAdmin (" + gnsReconfigurable.getNodeID() + ") : Handling DELETEALLRECORDS request");
                long startTime = System.currentTimeMillis();
                int cnt = 0;
                BasicRecordCursor cursor = NameRecord.getAllRowsIterator(gnsReconfigurable.getDB());
                while (cursor.hasNext()) {
                  NameRecord nameRecord = new NameRecord(gnsReconfigurable.getDB(), cursor.next());
                  //for (NameRecord nameRecord : NameServer.getAllNameRecords()) {
                  try {
                    NameRecord.removeNameRecord(gnsReconfigurable.getDB(), nameRecord.getName());
                  } catch (FieldNotFoundException e) {
                    GNS.getLogger().severe("FieldNotFoundException. Field Name =  " + e.getMessage());
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                  }
                  //DBNameRecord.removeNameRecord(nameRecord.getName());
                  cnt++;
                }
                GNS.getLogger().fine("NSListenerAdmin (" + gnsReconfigurable.getNodeID() + ") : Deleting " + cnt + " records took "
                        + (System.currentTimeMillis() - startTime) + "ms");
                break;
              // Clears the database and reinitializes all indices
              case RESETDB:
                GNS.getLogger().fine("NSListenerAdmin (" + gnsReconfigurable.getNodeID() + ") : Handling RESETDB request");
                replicaController.reset();
                rcCooordinator.reset();
                appCoordinator.reset();
                gnsReconfigurable.reset();

                break;
              case PINGTABLE:
                int node = Integer.parseInt(adminRequestPacket.getArgument());
                if (node == gnsReconfigurable.getNodeID()) {
                  JSONObject jsonResponse = new JSONObject();
                  jsonResponse.put("PINGTABLE", gnsReconfigurable.getPingManager().tableToString(gnsReconfigurable.getNodeID()));
                  AdminResponsePacket responsePacket = new AdminResponsePacket(adminRequestPacket.getId(), jsonResponse);
                  Packet.sendTCPPacket(gnsNodeConfig, responsePacket.toJSONObject(), adminRequestPacket.getLocalNameServerId(), GNS.PortType.LNS_ADMIN_PORT);
                } else {
                  GNS.getLogger().warning("NSListenerAdmin wrong node for PINGTABLE!");
                }
                break;
              case PINGVALUE:
                int node1 = Integer.parseInt(adminRequestPacket.getArgument());
                int node2 = Integer.parseInt(adminRequestPacket.getArgument2());
                if (node1 == gnsReconfigurable.getNodeID()) {
                  JSONObject jsonResponse = new JSONObject();
                  jsonResponse.put("PINGVALUE", gnsReconfigurable.getPingManager().nodeAverage(node2));
                  AdminResponsePacket responsePacket = new AdminResponsePacket(adminRequestPacket.getId(), jsonResponse);
                  Packet.sendTCPPacket(gnsNodeConfig, responsePacket.toJSONObject(), adminRequestPacket.getLocalNameServerId(), GNS.PortType.LNS_ADMIN_PORT);
                } else {
                  GNS.getLogger().warning("NSListenerAdmin wrong node for PINGVALUE!");
                }
                break;
              case CHANGELOGLEVEL:
                Level level = Level.parse(adminRequestPacket.getArgument());
                GNS.getLogger().info("Changing log level to " + level.getName());
                GNS.getLogger().setLevel(level);
                break;
              case CLEARCACHE:
                // shouldn't ever get this
                GNS.getLogger().warning("NSListenerAdmin (" + gnsReconfigurable.getNodeID() + ") : Ignoring CLEARCACHE request");
                break;

            }
            break;
          case STATUS_INIT:
            StatusClient.handleStatusInit(socket.getInetAddress());
            StatusClient.sendStatus(gnsReconfigurable.getNodeID(), "NS Ready");
            break;
        }

        socket.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

  }

  /**
   * Sends active name server information to the sender
   *
   * @param activeNSInfoPacket
   * @param socket
   * @param numRequest
   * @throws IOException
   * @throws JSONException
   */
  private void sendactiveNameServerInfo(ActiveNameServerInfoPacket activeNSInfoPacket,
          Socket socket, int numRequest, Set<Integer> activeNameServers) throws IOException, JSONException {
    activeNSInfoPacket.setActiveNameServers(activeNameServers);
    activeNSInfoPacket.setPrimaryNameServer(gnsReconfigurable.getNodeID());
    Packet.sendTCPPacket(activeNSInfoPacket.toJSONObject(), socket);
    GNS.getLogger().fine("NSListenrAdmin: Response RequestNum:" + numRequest + " --> " + activeNSInfoPacket.toString());
  }
}
