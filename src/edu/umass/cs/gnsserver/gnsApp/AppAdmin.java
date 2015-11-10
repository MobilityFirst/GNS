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
package edu.umass.cs.gnsserver.gnsApp;

import edu.umass.cs.gnsserver.utils.Shutdownable;
import edu.umass.cs.gnsserver.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.AccountAccess;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.GuidInfo;
import edu.umass.cs.gnsserver.database.AbstractRecordCursor;
import edu.umass.cs.gnsserver.exceptions.FieldNotFoundException;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.gnsApp.packet.Packet;
import edu.umass.cs.gnsserver.gnsApp.packet.admin.AdminRequestPacket;
import edu.umass.cs.gnsserver.gnsApp.packet.admin.AdminResponsePacket;
import edu.umass.cs.gnsserver.gnsApp.packet.admin.DumpRequestPacket;
import edu.umass.cs.gnsserver.gnsApp.recordmap.NameRecord;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Level;

/**
 * A separate thread that runs in the NameServer that handles administrative (AKA non-data related, non-user)
 * type operations. All of the things in here are for server administration and debugging.
 */
@SuppressWarnings("unchecked")
public class AppAdmin extends Thread implements Shutdownable{

  /**
   * Socket over which active name server request arrive *
   */
  private ServerSocket serverSocket;

  private final GnsApplicationInterface<String> app;

  private final GNSNodeConfig<String> gnsNodeConfig;

  /**
   * Creates a new listener thread for handling response packet
   *
   * @param app
   * @param gnsNodeConfig
   */
  
  public AppAdmin(GnsApplicationInterface<String> app, GNSNodeConfig gnsNodeConfig) {
    super("NSListenerAdmin");
    this.app = app;
    this.gnsNodeConfig = gnsNodeConfig;
    try {
      this.serverSocket = new ServerSocket(gnsNodeConfig.getAdminPort(app.getNodeID()));
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
    GNS.getLogger().info("NS Node " + app.getNodeID().toString() + " starting Admin Request Server on port " + serverSocket.getLocalPort());
    while (true) {
      try {
        Socket socket = serverSocket.accept();

        //Read the packet from the input stream
        JSONObject incomingJSON = Packet.getJSONObjectFrame(socket);
        switch (Packet.getPacketType(incomingJSON)) {

          case DUMP_REQUEST:

            DumpRequestPacket dumpRequestPacket = new DumpRequestPacket(incomingJSON, gnsNodeConfig);

            dumpRequestPacket.setPrimaryNameServer(app.getNodeID());
            JSONArray jsonArray = new JSONArray();
            // if there is an argument it is a TAGNAME we return all the records that have that tag
            if (dumpRequestPacket.getArgument() != null) {
              String tag = dumpRequestPacket.getArgument();
              AbstractRecordCursor cursor = NameRecord.getAllRowsIterator(app.getDB());
              while (cursor.hasNext()) {
                NameRecord nameRecord = null;
                JSONObject json = cursor.nextJSONObject();
                try {
                  nameRecord = new NameRecord(app.getDB(), json);
                } catch (JSONException e) {
                  GNS.getLogger().severe("Problem parsing json into NameRecord: " + e + " JSON is " + json.toString());
                }
                if (nameRecord != null) {
                  try {
                    if (nameRecord.containsKey(AccountAccess.GUID_INFO)) {
                      GuidInfo userInfo = new GuidInfo(nameRecord.getValuesMap().getJSONObject(AccountAccess.GUID_INFO));
                      //GuidInfo userInfo = new GuidInfo(nameRecord.getKeyAsArray(AccountAccess.GUID_INFO).toResultValueString());
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
              AbstractRecordCursor cursor = NameRecord.getAllRowsIterator(app.getDB());
              while (cursor.hasNext()) {
                NameRecord nameRecord = null;
                JSONObject json = cursor.nextJSONObject();
                try {
                  nameRecord = new NameRecord(app.getDB(), json);
                } catch (JSONException e) {
                  GNS.getLogger().severe("Problem parsing record cursor into NameRecord: " + e + " JSON is " + json.toString());
                }
                if (nameRecord != null) {
                  jsonArray.put(nameRecord.toJSONObject());
                }
              }
            }
            if (GNS.getLogger().isLoggable(Level.FINER)) {
              GNS.getLogger().finer("NSListenrAdmin for " + app.getNodeID() + " is " + jsonArray.toString());
            }
            dumpRequestPacket.setJsonArray(jsonArray);
            Packet.sendTCPPacket(dumpRequestPacket.toJSONObject(), 
                    dumpRequestPacket.getCCPAddress());
            
            GNS.getLogger().fine("NSListenrAdmin: Response to id:" + dumpRequestPacket.getId() + " --> " + dumpRequestPacket.toString());
            break;
          case ADMIN_REQUEST:
            AdminRequestPacket adminRequestPacket = new AdminRequestPacket(incomingJSON);
            switch (adminRequestPacket.getOperation()) {
              case DELETEALLRECORDS:
                GNS.getLogger().fine("NSListenerAdmin (" + app.getNodeID() + ") : Handling DELETEALLRECORDS request");
                long startTime = System.currentTimeMillis();
                int cnt = 0;
                AbstractRecordCursor cursor = NameRecord.getAllRowsIterator(app.getDB());
                while (cursor.hasNext()) {
                  NameRecord nameRecord = new NameRecord(app.getDB(), cursor.nextJSONObject());
                  //for (NameRecord nameRecord : NameServer.getAllNameRecords()) {
                  try {
                    NameRecord.removeNameRecord(app.getDB(), nameRecord.getName());
                  } catch (FieldNotFoundException e) {
                    GNS.getLogger().severe("FieldNotFoundException. Field Name =  " + e.getMessage());
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                  }
                  //DBNameRecord.removeNameRecord(nameRecord.getName());
                  cnt++;
                }
                GNS.getLogger().fine("NSListenerAdmin (" + app.getNodeID() + ") : Deleting " + cnt + " records took "
                        + (System.currentTimeMillis() - startTime) + "ms");
                break;
              // Clears the database and reinitializes all indices
              case RESETDB:
                  // don't like this anyway
//                GNS.getLogger().fine("NSListenerAdmin (" + app.getNodeID() + ") : Handling RESETDB request");
//                replicaController.reset();
//                rcCoordinator.reset();
//                appCoordinator.reset();
//                app.reset();

                break;
              case PINGTABLE:
                String node = adminRequestPacket.getArgument();
                if (node.equals(app.getNodeID())) {
                  JSONObject jsonResponse = new JSONObject();
                  jsonResponse.put("PINGTABLE", app.getPingManager().tableToString((String)app.getNodeID()));
                  AdminResponsePacket responsePacket = new AdminResponsePacket(adminRequestPacket.getId(), jsonResponse);
                  Packet.sendTCPPacket(responsePacket.toJSONObject(), adminRequestPacket.getCCPAddress());
                } else {
                  GNS.getLogger().warning("NSListenerAdmin wrong node for PINGTABLE!");
                }
                break;
              case PINGVALUE:
                String node1 = adminRequestPacket.getArgument();
                String node2 = adminRequestPacket.getArgument2();
                if (node1.equals(app.getNodeID())) {
                  JSONObject jsonResponse = new JSONObject();
                  jsonResponse.put("PINGVALUE", app.getPingManager().nodeAverage(node2));
                  AdminResponsePacket responsePacket = new AdminResponsePacket(adminRequestPacket.getId(), jsonResponse);
                  Packet.sendTCPPacket(responsePacket.toJSONObject(), adminRequestPacket.getCCPAddress());
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
                // shouldn't ever toString this
                GNS.getLogger().warning("NSListenerAdmin (" + app.getNodeID() + ") : Ignoring CLEARCACHE request");
                break;

            }
            break;
          case STATUS_INIT:
            break;
        }

        socket.close();
      } catch (Exception e) {
        if (serverSocket.isClosed())  {
          GNS.getLogger().warning("NS Admin shutting down.");
          return; // close this thread
        }
        e.printStackTrace();
      }
    }
  }

  /**
   * Closes the server socket in process of shutting down name server.
   * This unblocks the listening thread and and the listening thread shuts down.
   */
  @Override
  public void shutdown() {
    try {
      this.serverSocket.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
