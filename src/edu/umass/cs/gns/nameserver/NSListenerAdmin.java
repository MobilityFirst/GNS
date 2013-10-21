package edu.umass.cs.gns.nameserver;

import edu.umass.cs.gns.client.AccountAccess;
import edu.umass.cs.gns.client.GuidInfo;
import edu.umass.cs.gns.database.BasicRecordCursor;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaControllerRecord;
import edu.umass.cs.gns.packet.ActiveNameServerInfoPacket;
import edu.umass.cs.gns.packet.Packet;
import edu.umass.cs.gns.packet.admin.AdminRequestPacket;
import edu.umass.cs.gns.packet.admin.DumpRequestPacket;
import edu.umass.cs.gns.paxos.PaxosManager;
import edu.umass.cs.gns.statusdisplay.StatusClient;
import edu.umass.cs.gns.util.ConfigFileInfo;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.logging.Level;

/**
 * *************************************************************
 * This class implements a thread that returns a list of active name servers for a name. The thread waits for request packet over a
 * UDP socket and sends a response containing the current active nameserver for a name record.
 *
 * @author Hardeep Uppal ************************************************************
 */
public class NSListenerAdmin extends Thread {

  /**
   * Socket over which active name server request arrive *
   */
  private ServerSocket serverSocket;

  /**
   * *************************************************************
   * Creates a new listener thread for handling response packet
   *
   * @throws IOException ************************************************************
   */
  public NSListenerAdmin() {
    super("NSListenerAdmin");
    try {
      this.serverSocket = new ServerSocket(ConfigFileInfo.getNSAdminRequestPort(NameServer.nodeID));
    } catch (IOException e) {
      GNS.getLogger().severe("Unable to create NSListenerAdmin server: " + e);
    }
  }

  /**
   * *************************************************************
   * Start executing the thread. ************************************************************
   */
  @Override
  public void run() {
    int numRequest = 0;
    GNS.getLogger().info("NS Node " + NameServer.nodeID + " starting Admin Request Server on port " + serverSocket.getLocalPort());
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
              nameRecordPrimary = NameServer.getNameRecordPrimaryMultiField(activeNSInfoPacket.getName(), ReplicaControllerRecord.ACTIVE_NAMESERVERS);
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
//            if (!NameServer.containsName(activeNSInfoPacket.getName()//, activeNSInfoPacket.getRecordKey()
//                    )) {
            // This name server does not contain the name
            // If this is not a primary name server for this name we ignore the request
//                            Set<Integer> primaryNameServers = HashFunction.getPrimaryReplicas(activeNSInfoPacket.getName());
//                            if (!primaryNameServers.contains(NameServer.nodeID)) {
//                                socket.close();
//                                continue;
//                            }
//                            if (GNS.USELEGACYDNS) {
//                                // Abhigyan: Commenting this out for now. We do not use ReplicateRecordPacket anymore
//                                // Gin up a record using the legacy DNS
////                NameRecord record = new NameRecord(activeNSInfoPacket.getName()//, activeNSInfoPacket.getRecordKey()
////                        );
////                NameServer.addNameRecord(record);
////                //Send the response packet with the current active name server
////                sendactiveNameServerInfo(activeNSInfoPacket, socket, numRequest);
////                //Inform the primaries about this record
////                ReplicateRecordPacket replicatePacket = new ReplicateRecordPacket(record, NameServer.nodeID);
////                Packet.multicastTCP(primaryNameServers, replicatePacket.toJSONObject(),
////                        2, GNS.PortType.REPLICATION_PORT, NameServer.nodeID);
////                StatusClient.sendTrafficStatus(NameServer.nodeID, primaryNameServers,
////                        NameServer.nodeID, GNS.PortType.REPLICATION_PORT, replicatePacket.getType(), activeNSInfoPacket.getName()//, activeNSInfoPacket.getRecordKey()
////                        );
//                            }
//                        } else if (NameServer.isPrimaryNameServer(activeNSInfoPacket.getName()//, activeNSInfoPacket.getRecordKey()
//                        )) {
//                            //Send the response packet with the current active name server
//                            sendactiveNameServerInfo(activeNSInfoPacket, socket, numRequest);
//                        }
            break;

          case DUMP_REQUEST:

            DumpRequestPacket dumpRequestPacket = new DumpRequestPacket(incomingJSON);

            dumpRequestPacket.setPrimaryNameServer(NameServer.nodeID);

            StatusClient.sendStatus(NameServer.nodeID, "Dumping records");
            JSONArray jsonArray = new JSONArray();
            // if there is an argument it is a TAGNAME we return all the records that have that tag
            if (dumpRequestPacket.getArgument() != null) {
              String tag = dumpRequestPacket.getArgument();
              BasicRecordCursor cursor = NameServer.getAllRowsIterator();
              while (cursor.hasNext()) {
                NameRecord nameRecord = new NameRecord(cursor.next());
                //for (NameRecord nameRecord : NameServer.getAllNameRecords()) {
                // a bit of a hack here
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
              // OTHERWISE WE RETURN ALL THE RECORD
            } else {
              //for (NameRecord nameRecord : NameServer.getAllNameRecords()) {
              BasicRecordCursor cursor = NameServer.getAllRowsIterator();
              while (cursor.hasNext()) {
                NameRecord nameRecord = new NameRecord(cursor.next());
                jsonArray.put(nameRecord.toJSONObject());
              }
            }
            if (GNS.getLogger().isLoggable(Level.FINER)) {
              GNS.getLogger().finer("NSListenrAdmin for " + NameServer.nodeID + " is " + jsonArray.toString());
            }
            dumpRequestPacket.setJsonArray(jsonArray);
            Packet.sendTCPPacket(dumpRequestPacket.toJSONObject(), dumpRequestPacket.getLocalNameServer(), GNS.PortType.LNS_ADMIN_PORT);
            //Packet.sendTCPPacket(dumpRequestPacket.toJSONObject(), socket);

            if (GNS.getLogger().isLoggable(Level.FINER)) {
              GNS.getLogger().finer("NSListenrAdmin: Response to id:" + dumpRequestPacket.getId() + " --> " + dumpRequestPacket.toString());
            }
            break;
          case ADMIN_REQUEST:
            AdminRequestPacket adminRequestPacket = new AdminRequestPacket(incomingJSON);
            switch (adminRequestPacket.getOperation()) {
              case DELETEALLRECORDS:
                GNS.getLogger().fine("NSListenerAdmin (" + NameServer.nodeID + ") : Handling DELETEALLRECORDS request");
                long startTime = System.currentTimeMillis();
                int cnt = 0;
                BasicRecordCursor cursor = NameServer.getAllRowsIterator();
                while (cursor.hasNext()) {
                  NameRecord nameRecord = new NameRecord(cursor.next());
                  //for (NameRecord nameRecord : NameServer.getAllNameRecords()) {
                  try {
                    NameServer.removeNameRecord(nameRecord.getName());
                  } catch (FieldNotFoundException e) {
                    GNS.getLogger().severe("FieldNotFoundException. Field Name =  " + e.getMessage());
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                  }
                  //DBNameRecord.removeNameRecord(nameRecord.getName());
                  cnt++;
                }
                GNS.getLogger().fine("NSListenerAdmin (" + NameServer.nodeID + ") : Deleting " + cnt + " records took "
                        + (System.currentTimeMillis() - startTime) + "ms");
                break;
              case RESETDB:
                GNS.getLogger().fine("NSListenerAdmin (" + NameServer.nodeID + ") : Handling RESETDB request");
                PaxosManager.resetAll();
                NameServer.resetDB();
                break;

//              case DELETEALLGUIDRECORDS:
//                // delete all the records that have a name (GUID) given by the argument in the packet
//                String name = adminRequestPacket.getArgument();
//                if (name != null) {
//                  GNS.getLogger().fine("NSListenerAdmin (" + NameServer.nodeID + ") : Handling delete all name " + name + " request");
//                  startTime = System.currentTimeMillis();
//                  cnt = 0;
//                  for (NameRecord nameRecord : NameServer.getAllNameRecords()) {
//                    if (name.equals(nameRecord.getName())) {
//                      NameServer.removeNameRecord(nameRecord.getName()//, nameRecord.getRecordKey()
//                              );
//                      cnt++;
//                    }
//                  }
//                  GNS.getLogger().fine("NSListenerAdmin (" + NameServer.nodeID + ") : Deleting " + cnt + " records took "
//                          + (System.currentTimeMillis() - startTime) + "ms");
//                }
//                break;
              case CLEARCACHE:
                // shouldn't ever get this
                GNS.getLogger().warning("NSListenerAdmin (" + NameServer.nodeID + ") : Ignoring CLEARCACHE request");
                break;

            }
            break;

          case STATUS_INIT:
            StatusClient.handleStatusInit(socket.getInetAddress());
            StatusClient.sendStatus(NameServer.nodeID, "NS Ready");
            break;

        }

        socket.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

  }

  /**
   * *************************************************************
   * Sends active name server information to the sender
   *
   * @param activeNSInfoPacket
   * @param socket
   * @param numRequest
   * @throws IOException
   * @throws JSONException ************************************************************
   */
  private void sendactiveNameServerInfo(ActiveNameServerInfoPacket activeNSInfoPacket,
          Socket socket, int numRequest, Set<Integer> activeNameServers) throws IOException, JSONException {
    activeNSInfoPacket.setActiveNameServers(activeNameServers);
    activeNSInfoPacket.setPrimaryNameServer(NameServer.nodeID);
    Packet.sendTCPPacket(activeNSInfoPacket.toJSONObject(), socket);
    GNS.getLogger().fine("NSListenrAdmin: Response RequestNum:" + numRequest + " --> " + activeNSInfoPacket.toString());
  }
}
