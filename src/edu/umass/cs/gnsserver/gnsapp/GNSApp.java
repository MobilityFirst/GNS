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
 *  Initial developer(s): Westy, arun
 *
 */
package edu.umass.cs.gnsserver.gnsapp;

import edu.umass.cs.gigapaxos.interfaces.ClientMessenger;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gnscommon.exceptions.client.GnsClientException;
import edu.umass.cs.gnsserver.activecode.ActiveCodeHandler;
import edu.umass.cs.gnsserver.database.ColumnField;
import edu.umass.cs.gnsserver.database.MongoRecords;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.FieldNotFoundException;
import edu.umass.cs.gnscommon.exceptions.server.RecordExistsException;
import edu.umass.cs.gnscommon.exceptions.server.RecordNotFoundException;
import edu.umass.cs.gnsserver.main.GNSConfig;
import static edu.umass.cs.gnsserver.gnsapp.AppReconfigurableNodeOptions.disableSSL;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.CCPListenerAdmin;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.json.JSONException;
import org.json.JSONObject;
import edu.umass.cs.gnsserver.nodeconfig.GNSConsistentReconfigurableNodeConfig;
import edu.umass.cs.gnsserver.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandler;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.Admintercessor;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandHandler;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandRequestInfo;
import edu.umass.cs.gnsserver.gnsapp.packet.CommandPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.CommandValueReturnPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.NoopPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectRequestPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectResponsePacket;
import edu.umass.cs.gnsserver.gnsapp.packet.StopPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet.PacketType;
import edu.umass.cs.gnsserver.gnsapp.recordmap.BasicRecordMap;
import edu.umass.cs.gnsserver.gnsapp.recordmap.GNSRecordMap;
import edu.umass.cs.gnsserver.gnsapp.recordmap.NameRecord;
import edu.umass.cs.gnsserver.httpserver.GnsHttpServer;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.SSLMessenger;
import edu.umass.cs.reconfiguration.examples.AbstractReconfigurablePaxosApp;
import edu.umass.cs.reconfiguration.interfaces.Reconfigurable;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableNodeConfig;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Util;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

/**
 * @author Westy, arun
 */
public class GNSApp extends AbstractReconfigurablePaxosApp<String>
        implements GNSApplicationInterface<String>, Replicable, Reconfigurable,
        ClientMessenger {

  private final static int INITIAL_RECORD_VERSION = 0;
  private String nodeID;
  private GNSConsistentReconfigurableNodeConfig<String> nodeConfig;
  private boolean constructed = false;
  /**
   * Object provides interface to the database table storing name records
   */
  private BasicRecordMap nameRecordDB;
  /**
   * The Nio server
   */
  private SSLMessenger<String, JSONObject> messenger;
  private ClientRequestHandlerInterface requestHandler;

  // Keep track of commands that are coming in
  /**
   *
   */
  public final ConcurrentMap<Long, CommandRequestInfo> outStandingQueries
          = new ConcurrentHashMap<>(10, 0.75f, 3);

  /**
   * Active code handler
   */
  private ActiveCodeHandler activeCodeHandler;

  /**
   * Constructor invoked via reflection by gigapaxos.
   *
   * @param args
   * @throws IOException
   */
  public GNSApp(String[] args) throws IOException {
    AppReconfigurableNode.initOptions(args);
  }

  /**
   * Actually creates the application. This strange way of constructing the application
   * is because of legacy code that used the createAppCoordinator interface.
   *
   * @param messenger
   * @throws java.io.IOException
   */
  private void GnsAppConstructor(JSONMessenger<String> messenger) throws IOException {
    this.nodeID = messenger.getMyID();
    GNSNodeConfig<String> gnsNodeConfig = new GNSNodeConfig<String>();
    this.nodeConfig = new GNSConsistentReconfigurableNodeConfig<>(gnsNodeConfig);
    MongoRecords<String> mongoRecords = new MongoRecords<>(nodeID, AppReconfigurableNodeOptions.mongoPort);
    this.nameRecordDB = new GNSRecordMap<>(mongoRecords, MongoRecords.DBNAMERECORD);
    GNSConfig.getLogger().info("App " + nodeID + " created " + nameRecordDB);
    this.messenger = messenger;
    // Create the admin object
    Admintercessor admintercessor = new Admintercessor();
    // Create the request handler
    this.requestHandler = new ClientRequestHandler(
            admintercessor,
            new InetSocketAddress(nodeConfig.getBindAddress(this.nodeID),
                    this.nodeConfig.getCcpPort(this.nodeID)),
            nodeID, this,
            gnsNodeConfig,
            AppReconfigurableNodeOptions.debuggingEnabled);
    // Finish admin setup
    CCPListenerAdmin ccpListenerAdmin = new CCPListenerAdmin(requestHandler);
    ccpListenerAdmin.start();
    admintercessor.setListenerAdmin(ccpListenerAdmin);
    new AppAdmin(this, gnsNodeConfig).start();
    GNSConfig.getLogger().info(nodeID.toString() + " Admin thread initialized");
    // Should add this to the shutdown method - do we have a shutdown method?
    GnsHttpServer httpServer = new GnsHttpServer(requestHandler);
    this.activeCodeHandler = AppReconfigurableNodeOptions.enableActiveCode ? new ActiveCodeHandler(this,
            AppReconfigurableNodeOptions.activeCodeWorkerCount,
            AppReconfigurableNodeOptions.activeCodeBlacklistSeconds) : null;
    constructed = true;
  }

  /**
   * Creates the application.
   *
   * @param id
   * @param nodeConfig
   * @param messenger
   * @throws java.io.IOException
   */
  @Deprecated
  public GNSApp(String id, GNSNodeConfig<String> nodeConfig, JSONMessenger<String> messenger) throws IOException {
    this.nodeID = id;
    this.nodeConfig = new GNSConsistentReconfigurableNodeConfig<>(nodeConfig);
    MongoRecords<String> mongoRecords = new MongoRecords<>(nodeID, AppReconfigurableNodeOptions.mongoPort);
    this.nameRecordDB = new GNSRecordMap<>(mongoRecords, MongoRecords.DBNAMERECORD);
    GNSConfig.getLogger().info("App " + nodeID + " created " + nameRecordDB);
    this.messenger = messenger;
    this.requestHandler = new ClientRequestHandler(
            new Admintercessor(),
            new InetSocketAddress(nodeConfig.getBindAddress(this.nodeID), this.nodeConfig.getCcpPort(this.nodeID)),
            nodeID, this,
            ((GNSNodeConfig<String>) messenger.getNodeConfig()),
            AppReconfigurableNodeOptions.debuggingEnabled);
    // Should add this to the shutdown method - do we have a shutdown method?
    GnsHttpServer httpServer = new GnsHttpServer(requestHandler);
    // start the NSListenerAdmin thread
    new AppAdmin(this, (GNSNodeConfig<String>) nodeConfig).start();
    GNSConfig.getLogger().info(nodeID.toString() + " Admin thread initialized");
    this.activeCodeHandler = AppReconfigurableNodeOptions.enableActiveCode ? new ActiveCodeHandler(this,
            AppReconfigurableNodeOptions.activeCodeWorkerCount,
            AppReconfigurableNodeOptions.activeCodeBlacklistSeconds) : null;
    constructed = true;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void setClientMessenger(SSLMessenger<?, JSONObject> messenger) {
    this.messenger = (SSLMessenger<String, JSONObject>) messenger;
    this.nodeID = messenger.getMyID().toString();
    try {
      if (!constructed) {
        this.GnsAppConstructor((JSONMessenger<String>) messenger);
      }
    } catch (IOException e) {
      e.printStackTrace();
      GNSConfig.getLogger().severe("Unable to create app: exiting");
      System.exit(1);
    }
  }

  private static PacketType[] types = {
    PacketType.SELECT_REQUEST,
    PacketType.SELECT_RESPONSE,
    PacketType.STOP,
    PacketType.NOOP,
    PacketType.COMMAND,
    PacketType.COMMAND_RETURN_VALUE};

  @SuppressWarnings("unchecked")
  // we explicitly check type
  @Override
  public boolean execute(Request request, boolean doNotReplyToClient) {
    boolean executed = false;
    try {
      Packet.PacketType packetType = request.getRequestType() 
              instanceof Packet.PacketType 
              ? (Packet.PacketType) request.getRequestType() 
              : null;
      if (AppReconfigurableNodeOptions.debuggingEnabled) {
        GNSConfig.getLogger().log(Level.INFO, "{0} &&&&&&& handling {1} ",
                new Object[]{this, request.getSummary()});
      }
      switch (packetType) {
        case SELECT_REQUEST:
          Select.handleSelectRequest(
                  (SelectRequestPacket<String>) request, this);
          break;
        case SELECT_RESPONSE:
          Select.handleSelectResponse(
                  (SelectResponsePacket<String>) request, this);
          break;
        /* TODO: arun: I assume the GNS doesn't need STOP and NOOP
				 * anymore; if so, remove them. 
         */
        case STOP:
          break;
        case NOOP:
          break;
        case COMMAND:
          CommandHandler.handleCommandPacketForApp(
                  (CommandPacket) request, doNotReplyToClient, this);
          break;
        case COMMAND_RETURN_VALUE:
          CommandHandler.handleCommandReturnValuePacketForApp(
                  (CommandValueReturnPacket) request, doNotReplyToClient, this);
          break;
        default:
          GNSConfig.getLogger().severe(
                  " Packet type not found: " + request.getSummary());
          return false;
      }
      executed = true;
    } catch (JSONException | IOException | GnsClientException e) {
      e.printStackTrace();
    } catch (FailedDBOperationException e) {
      // all database operations throw this exception, therefore we keep
      // throwing this exception upwards and catch this
      // here.
      // A database operation error would imply that the application
      // hasn't been able to successfully execute
      // the request. therefore, this method returns 'false', hoping that
      // whoever calls handleDecision would retry
      // the request.
      GNSConfig.getLogger().severe(
              "Error handling request: " + request.toString());
      e.printStackTrace();
    }
    return executed;
  }

  // For InterfaceApplication
  @Override
  public Request getRequest(String string)
          throws RequestParseException {
    if (AppReconfigurableNodeOptions.debuggingEnabled) {
      GNSConfig.getLogger().info(">>>>>>>>>>>>>>> GET REQUEST: " + string);
    }
    // Special case handling of NoopPacket packets
    if (Request.NO_OP.toString().equals(string)) {
      return new NoopPacket();
    }
    try {
      JSONObject json = new JSONObject(string);
      Request request = (Request) Packet.createInstance(json, nodeConfig);
      return request;
    } catch (JSONException e) {
      throw new RequestParseException(e);
    }
  }

  @Override
  public Set<IntegerPacketType> getRequestTypes() {
    return new HashSet<IntegerPacketType>(Arrays.asList(types));
  }

  @Override
  public boolean execute(Request request) {
    return this.execute(request, false);
  }

  private final static ArrayList<ColumnField> curValueRequestFields = new ArrayList<>();

  static {
    curValueRequestFields.add(NameRecord.VALUES_MAP);
    curValueRequestFields.add(NameRecord.TIME_TO_LIVE);
  }

  @Override
  public String checkpoint(String name) {
    try {
      NameRecord nameRecord = NameRecord.getNameRecordMultiField(nameRecordDB, name, curValueRequestFields);
      NRState state = new NRState(nameRecord.getValuesMap(), nameRecord.getTimeToLive());
      if (AppReconfigurableNodeOptions.debuggingEnabled) {
        GNSConfig.getLogger().log(Level.INFO,
                "&&&&&&& {0} getting state {1} ",
                new Object[]{this, state.getSummary()});
      }
      return state.toString();
    } catch (RecordNotFoundException e) {
      // normal result
    } catch (FieldNotFoundException e) {
      GNSConfig.getLogger().severe("Field not found exception: " + e.getMessage());
      e.printStackTrace();
    } catch (FailedDBOperationException e) {
      GNSConfig.getLogger().severe("State not read from DB: " + e.getMessage());
      e.printStackTrace();
    }
    return null;
  }

  /**
   * Updates the state for the given named record.
   *
   * @param name
   * @param state
   * @return true if we were able to update the state
   */
  @Override
  public boolean restore(String name, String state) {
    long startTime = System.currentTimeMillis();
    if (AppReconfigurableNodeOptions.debuggingEnabled) {
      GNSConfig.getLogger().log(Level.INFO,
              "&&&&&&& {0} updating {1} with state [{2}]",
              new Object[]{this, name, Util.truncate(state, 32, 32)});
    }
    try {
      if (state == null) {
        // If state is null the only thing it means is that we need to delete 
        // the record. If the record does not exists this is just a noop.
        NameRecord.removeNameRecord(nameRecordDB, name);
      } else { //state does not equal null so we either create a new record or update the existing one
        NameRecord nameRecord = null;
        try {
          nameRecord = NameRecord.getNameRecordMultiField(nameRecordDB, name, curValueRequestFields);
        } catch (RecordNotFoundException e) {
          // normal result if the field does not exist
        }
        if (nameRecord == null) { // create a new record
          try {
            NRState nrState = new NRState(state); // parse the new state
            nameRecord = new NameRecord(nameRecordDB, name, INITIAL_RECORD_VERSION,
                    nrState.valuesMap, nrState.ttl,
                    nodeConfig.getReplicatedReconfigurators(name));
            NameRecord.addNameRecord(nameRecordDB, nameRecord);
          } catch (RecordExistsException e) {
            GNSConfig.getLogger().severe("Problem updating state, record already exists: " + e.getMessage());
          } catch (JSONException e) {
            GNSConfig.getLogger().severe("Problem updating state: " + e.getMessage());
          }
        } else { // update the existing record
          try {
            NRState nrState = new NRState(state); // parse the new state
            nameRecord.updateState(nrState.valuesMap, nrState.ttl);
          } catch (JSONException | FieldNotFoundException e) {
            GNSConfig.getLogger().severe("Problem updating state: " + e.getMessage());
          }
        }
      }
      //DelayProfiler.updateDelay("restore", startTime);
      return true;
    } catch (FailedDBOperationException e) {
      GNSConfig.getLogger().severe("Failed update exception: " + e.getMessage());
      e.printStackTrace();
    }
    return false;
  }

  /**
   * Returns a stop request packet.
   *
   * @param name
   * @param epoch
   * @return the stop request packet
   */
  @Override
  public ReconfigurableRequest getStopRequest(String name, int epoch) {
    return new StopPacket(name, epoch);
  }

  @Override
  public String getFinalState(String name, int epoch) {
    throw new RuntimeException("This method should not have been called");
  }

  @Override
  public void putInitialState(String name, int epoch, String state) {
    throw new RuntimeException("This method should not have been called");
  }

  @Override
  public boolean deleteFinalState(String name, int epoch) {
    throw new RuntimeException("This method should not have been called");
  }

  @Override
  public Integer getEpoch(String name) {
    throw new RuntimeException("This method should not have been called");
  }

  //
  // GnsApplicationInterface implementation
  //
  @Override
  public String getNodeID() {
    return nodeID;
  }

  @Override
  public BasicRecordMap getDB() {
    return nameRecordDB;
  }

  @Override
  public ReconfigurableNodeConfig<String> getGNSNodeConfig() {
    return nodeConfig;
  }

  /**
   * arun: FIXME: This mode of calling getClientMessenger is outdated and
   * poor. The better way is to either delegate client messaging to gigapaxos
   * or to determine the right messenger to use in the app based on the
   * listening socket address (clear or ssl) on which the request was received
   * by invoking {@link SSLMessenger#getClientMessenger(InetSocketAddress)}.
   * Doing it like below works but requires all client requests to use the
   * same mode (ssl or clear), otherwise JSONMessenger has no through which
   * socket the request came in.
   */
  @Override
  public void sendToClient(InetSocketAddress isa, Request response, JSONObject responseJSON, InetSocketAddress myListeningAddress) throws IOException {;

    /* arun: FIXED: You have just ignored the doNotReplyToClient flag
		 * here, which is against the spec of the implementation of the
		 * Replicable.execute(.) method. Alternatively, you could just delegate 
		 * client messaging to gigapaxos, but you are not doing that either.
		 * The current implementation will unnecessarily incur 3x client
		 * messaging overhead and can potentially cause bugs if clients rapidly 
		 * open and close connections.
		 * 
		 * 
		 * arun: FIXME: use myListeningAddress and invoke just
		 * getClientMessenger().sendToAddress(isa,
		 * responseJSON,myListeningAddress), otherwise messenger does not know
		 * which socket the request came in and is forced to either always use
		 * SSL or always not use SSL. SSL should be a per-request option like
		 * HTTPS, i.e., if SSL is enabled, a client should be able to send
		 * requests to either the SSL port or the CLEAR port; the latter is
		 * always available by default.
		 * 
		 * Alternatively, delegate client messaging to gigapaxos and you don't
		 * have to worry about keeping track of sender or listening addresses.
		 * For that we need the corresponding request here so that we can invoke
		 * request.setResponse(response) here. */
    if (!disableSSL) {
      GNSConfig.getLogger().log(Level.INFO,
              "{0} sending back response to client {1} -> {2}",
              new Object[]{this, response.getSummary(), isa});
      messenger.getSSLClientMessenger().sendToAddress(isa, responseJSON);
    } else {
      messenger.getClientMessenger().sendToAddress(isa, responseJSON);
    }
  }

  public String toString() {
    return this.getClass().getSimpleName() + ":" + this.nodeID;
  }

  @Override
  // Currently only used by Select
  public void sendToID(String id, JSONObject msg) throws IOException {
    messenger.sendToID(id, msg);
  }

  @Override
  public ActiveCodeHandler getActiveCodeHandler() {
    return activeCodeHandler;
  }

  @Override
  public ClientRequestHandlerInterface getRequestHandler() {
    return requestHandler;
  }
}
