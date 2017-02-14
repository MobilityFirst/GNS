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
package edu.umass.cs.gnsserver.gnsapp;

import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnsserver.utils.Shutdownable;
import edu.umass.cs.gnsserver.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gnsserver.database.AbstractRecordCursor;
import edu.umass.cs.gnscommon.exceptions.server.FieldNotFoundException;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountAccess;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.GuidInfo;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet;
import edu.umass.cs.gnsserver.gnsapp.packet.admin.AdminRequestPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.admin.DumpRequestPacket;
import edu.umass.cs.gnsserver.gnsapp.recordmap.NameRecord;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.ParseException;
import java.util.logging.Level;

/**
 * A separate thread that runs in the NameServer that handles administrative (AKA non-data related, non-user)
 * type operations. All of the things in here are for server administration and debugging.
 * Gets requests from Admintercessor through the AdminListener
 * looks up stuff in the database and returns the results back to the AdminListener.
 */
@SuppressWarnings("unchecked")
public class AppAdminServer extends Thread implements Shutdownable {

  /**
   * Socket over which active name server request arrive *
   */
  private ServerSocket serverSocket;

  private final GNSApplicationInterface<String> app;

  private final GNSNodeConfig<String> gnsNodeConfig;

  /**
   * Creates a new listener thread for handling response packet
   *
   * @param app
   * @param gnsNodeConfig
   */
  public AppAdminServer(GNSApplicationInterface<String> app, GNSNodeConfig<String> gnsNodeConfig) {
    super("NSListenerAdmin");
    this.app = app;
    this.gnsNodeConfig = gnsNodeConfig;
    try {
      this.serverSocket = new ServerSocket(gnsNodeConfig.getServerAdminPort(app.getNodeID()));
    } catch (IOException e) {
      GNSConfig.getLogger().log(Level.SEVERE,
              "Unable to create NSListenerAdmin server on port {0}: {1}",
              new Object[]{gnsNodeConfig.getServerAdminPort(app.getNodeID()), e});
    }
  }

  /**
   * Start executing the thread.
   */
  @Override
  public void run() {
    int numRequest = 0;
    GNSConfig.getLogger().log(Level.INFO,
            "NS Node {0} starting Admin Request Server on port {1}",
            new Object[]{app.getNodeID(), serverSocket.getLocalPort()});
    while (true) {
      try {
        //Read the packet from the input stream
        try (Socket socket = serverSocket.accept()) {
          //Read the packet from the input stream
          JSONObject incomingJSON = Packet.getJSONObjectFrame(socket);
          switch (Packet.getPacketType(incomingJSON)) {

            case DUMP_REQUEST:

              DumpRequestPacket<String> dumpRequestPacket
                      = new DumpRequestPacket<>(incomingJSON, gnsNodeConfig);

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
                    GNSConfig.getLogger().log(Level.SEVERE,
                            "Problem parsing json into NameRecord: {0} JSON is {1}",
                            new Object[]{e, json.toString()});
                  }
                  if (nameRecord != null) {
                    try {
                      if (nameRecord.containsUserKey(AccountAccess.GUID_INFO)) {
                        GuidInfo userInfo = new GuidInfo(nameRecord.getValuesMap().getJSONObject(AccountAccess.GUID_INFO));
                        //GuidInfo userInfo = new GuidInfo(nameRecord.getUserKeyAsArray(AccountAccess.GUID_INFO).toResultValueString());
                        if (userInfo.containsTag(tag)) {
                          jsonArray.put(nameRecord.toJSONObject());
                        }
                      }
                    } catch (FieldNotFoundException e) {
                      GNSConfig.getLogger().log(Level.SEVERE,
                              "FieldNotFoundException. Field Name =  {0}", e.getMessage());
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
                    GNSConfig.getLogger().log(Level.SEVERE,
                            "Problem parsing record cursor into NameRecord: {0} JSON is {1}",
                            new Object[]{e, json.toString()});
                  }
                  if (nameRecord != null) {
                    jsonArray.put(nameRecord.toJSONObject());
                  }
                }
              }
              GNSConfig.getLogger().log(Level.FINER,
                      "AppAdmin for {0} is {1}",
                      new Object[]{app.getNodeID(), jsonArray.toString()});

              dumpRequestPacket.setJsonArray(jsonArray);
              Packet.sendTCPPacket(dumpRequestPacket.toJSONObject(),
                      dumpRequestPacket.getReturnAddress());

              GNSConfig.getLogger().log(Level.FINEST,
                      "AppAdmin: Response to id:{0} --> {1}",
                      new Object[]{dumpRequestPacket.getId(), dumpRequestPacket.toString()});
              break;
            case ADMIN_REQUEST:
              AdminRequestPacket adminRequestPacket = new AdminRequestPacket(incomingJSON);
              switch (adminRequestPacket.getOperation()) {
                case CLEARCACHE:
                  GNSConfig.getLogger().log(Level.WARNING,
                          "NSListenerAdmin ({0}) : Ignoring CLEARCACHE request", app.getNodeID());
                  break;
                case DUMPCACHE:
                  GNSConfig.getLogger().log(Level.WARNING,
                          "NSListenerAdmin ({0}) : Ignoring DUMPCACHE request", app.getNodeID());
                  break;

              }
              break;
          }
        }
      } catch (IOException | JSONException | FailedDBOperationException | ParseException | IllegalArgumentException | SecurityException e) {
        if (serverSocket.isClosed()) {
          GNSConfig.getLogger().warning("NS Admin shutting down.");
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
