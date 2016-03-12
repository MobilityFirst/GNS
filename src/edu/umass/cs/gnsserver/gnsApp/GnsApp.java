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
package edu.umass.cs.gnsserver.gnsApp;

import edu.umass.cs.gigapaxos.interfaces.ClientMessenger;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gnscommon.exceptions.client.GnsClientException;
import edu.umass.cs.gnsserver.activecode.ActiveCodeHandler;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.CommandHandler;
import edu.umass.cs.gnsserver.database.ColumnField;
import edu.umass.cs.gnsserver.database.MongoRecords;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.FieldNotFoundException;
import edu.umass.cs.gnscommon.exceptions.server.RecordExistsException;
import edu.umass.cs.gnscommon.exceptions.server.RecordNotFoundException;
import edu.umass.cs.gnsserver.main.GNS;
import static edu.umass.cs.gnsserver.gnsApp.AppReconfigurableNodeOptions.disableSSL;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.ClientRequestHandler;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.RequestHandlerParameters;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.Admintercessor;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.ClientRequestHandlerInterface;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnsserver.nodeconfig.GNSConsistentReconfigurableNodeConfig;
import edu.umass.cs.gnsserver.nodeconfig.GNSNodeConfig;
//import edu.umass.cs.gnsserver.gnsApp.clientSupport.LNSQueryHandler;
import edu.umass.cs.gnsserver.gnsApp.packet.NoopPacket;
import edu.umass.cs.gnsserver.gnsApp.packet.Packet;
import edu.umass.cs.gnsserver.gnsApp.packet.Packet.PacketType;
import edu.umass.cs.gnsserver.gnsApp.packet.StopPacket;
import edu.umass.cs.gnsserver.gnsApp.recordmap.BasicRecordMap;
import edu.umass.cs.gnsserver.gnsApp.recordmap.GNSRecordMap;
import edu.umass.cs.gnsserver.gnsApp.recordmap.NameRecord;
import edu.umass.cs.gnsserver.ping.PingManager;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.SSLMessenger;
import edu.umass.cs.reconfiguration.examples.AbstractReconfigurablePaxosApp;
import edu.umass.cs.reconfiguration.interfaces.Reconfigurable;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableNodeConfig;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.DelayProfiler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

/**
 * @author Westy
 */
public class GnsApp extends AbstractReconfigurablePaxosApp<String>
        implements GnsApplicationInterface<String>, Replicable, Reconfigurable,
        ClientMessenger {

  private final static int INITIAL_RECORD_VERSION = 0;
  private String nodeID;
  private GNSConsistentReconfigurableNodeConfig<String> nodeConfig;
  private PingManager<String> pingManager;
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
  //private ClientCommandProcessor clientCommandProcessor;

  // Keep track of commands that are coming in
  /**
   *
   */
  public final ConcurrentMap<Long, CommandHandler.CommandRequestInfo> outStandingQueries
          = new ConcurrentHashMap<>(10, 0.75f, 3);

  /**
   * Active code handler
   */
  private ActiveCodeHandler activeCodeHandler;

  public GnsApp(String[] args) throws IOException {
    AppReconfigurableNode.initOptions(args);
  }

  /**
   * Creates the application.
   *
   * @param messenger
   * @throws java.io.IOException
   */
  public void GnsAppConstructor(JSONMessenger<String> messenger) throws IOException {
    this.nodeID = messenger.getMyID();
    this.nodeConfig = new GNSConsistentReconfigurableNodeConfig<>((GNSNodeConfig<String>) messenger.getNodeConfig());
    // Start a ping server, but not a client.
    this.pingManager = new PingManager<String>(nodeID, this.nodeConfig, true);
    GNS.getLogger().info("Node " + nodeID + " started Ping server on port "
            + nodeConfig.getCcpPingPort(nodeID));
    MongoRecords<String> mongoRecords = new MongoRecords<>(nodeID, AppReconfigurableNodeOptions.mongoPort);
    this.nameRecordDB = new GNSRecordMap<>(mongoRecords, MongoRecords.DBNAMERECORD);
    GNS.getLogger().info("App " + nodeID + " created " + nameRecordDB);
    this.messenger = messenger;
    RequestHandlerParameters parameters = new RequestHandlerParameters();
    parameters.setDebugMode(AppReconfigurableNodeOptions.debuggingEnabled);
    this.requestHandler = new ClientRequestHandler(
            new Admintercessor(),
            new InetSocketAddress(nodeConfig.getBindAddress(this.nodeID), this.nodeConfig.getCcpPort(this.nodeID)),
            nodeID, this,
            ((GNSNodeConfig<String>) messenger.getNodeConfig()),
            messenger, parameters);

//    this.clientCommandProcessor = new ClientCommandProcessor(messenger,
//            new InetSocketAddress(nodeConfig.getBindAddress(this.nodeID), this.nodeConfig.getCcpPort(this.nodeID)),
//            ((GNSNodeConfig<String>)messenger.getNodeConfig()),
//            AppReconfigurableNodeOptions.debuggingEnabled,
//            this,
//            this.nodeID,
//            AppReconfigurableNodeOptions.dnsGnsOnly,
//            AppReconfigurableNodeOptions.dnsOnly,
//            AppReconfigurableNodeOptions.gnsServerIP);
    // start the NSListenerAdmin thread
    new AppAdmin(this, (GNSNodeConfig<String>) messenger.getNodeConfig()).start();
    GNS.getLogger().info(nodeID.toString() + " Admin thread initialized");
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
  public GnsApp(String id, GNSNodeConfig<String> nodeConfig, JSONMessenger<String> messenger) throws IOException {
    this.nodeID = id;
    this.nodeConfig = new GNSConsistentReconfigurableNodeConfig<>(nodeConfig);
    // Start a ping server, but not a client.
    this.pingManager = new PingManager<String>(nodeID, this.nodeConfig, true);
    GNS.getLogger().info("Node " + nodeID + " started Ping server on port "
            + nodeConfig.getCcpPingPort(nodeID));
    MongoRecords<String> mongoRecords = new MongoRecords<>(nodeID, AppReconfigurableNodeOptions.mongoPort);
    this.nameRecordDB = new GNSRecordMap<>(mongoRecords, MongoRecords.DBNAMERECORD);
    GNS.getLogger().info("App " + nodeID + " created " + nameRecordDB);
    this.messenger = messenger;
    RequestHandlerParameters parameters = new RequestHandlerParameters();
    parameters.setDebugMode(AppReconfigurableNodeOptions.debuggingEnabled);
    this.requestHandler = new ClientRequestHandler(
            new Admintercessor(),
            new InetSocketAddress(nodeConfig.getBindAddress(this.nodeID), this.nodeConfig.getCcpPort(this.nodeID)),
            nodeID, this,
            ((GNSNodeConfig<String>) messenger.getNodeConfig()),
            messenger, parameters);
//    this.clientCommandProcessor = new ClientCommandProcessor(messenger,
//            new InetSocketAddress(nodeConfig.getBindAddress(id), nodeConfig.getCcpPort(id)),
//            (GNSNodeConfig<String>) nodeConfig,
//            AppReconfigurableNodeOptions.debuggingEnabled,
//            this,
//            (String) id,
//            AppReconfigurableNodeOptions.dnsGnsOnly,
//            AppReconfigurableNodeOptions.dnsOnly,
//            AppReconfigurableNodeOptions.gnsServerIP);
    // start the NSListenerAdmin thread
    new AppAdmin(this, (GNSNodeConfig<String>) nodeConfig).start();
    GNS.getLogger().info(nodeID.toString() + " Admin thread initialized");
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
      GNS.getLogger().severe("Unable to create app: exiting");
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

  private int execCount=0;
  private int incrExecCount() {
	 return this.execCount++;
  }
  private int decrExecCount() {
	  return --this.execCount;
  }
  
  
  @Override
  public boolean execute(Request request, boolean doNotReplyToClient) {
	  this.incrExecCount();
    boolean executed = false;
    try {
			// FIXME: arun: this is terrible. Why go back to json when you have
			// the packet you wnat already????
      JSONObject json = new JSONObject(request.toString());
      Packet.PacketType packetType = Packet.getPacketType(json);
      if (AppReconfigurableNodeOptions.debuggingEnabled) {
        GNS.getLogger().info("&&&&&&& APP " + nodeID + "&&&&&&& Handling " + this.execCount + " " + packetType.name()
                + " packet: " + json.toString());
        //+ " packet: " + json.toReasonableString());
      }
      switch (packetType) {
        case SELECT_REQUEST:
          Select.handleSelectRequest(json, this);
          break;
        case SELECT_RESPONSE:
          Select.handleSelectResponse(json, this);
          break;
        case STOP:
          break;
        case NOOP:
          break;
        case COMMAND:
          CommandHandler.handleCommandPacketForApp(json, this);
          break;
        case COMMAND_RETURN_VALUE:
          CommandHandler.handleCommandReturnValuePacketForApp(json, this);
          break;
        default:
          GNS.getLogger().severe(" Packet type not found: " + json.toString());
          //GNS.getLogger().severe(" Packet type not found: " + json.toReasonableString());
          return false;
      }
      executed = true;
    } catch (JSONException | IOException | GnsClientException e) {
      e.printStackTrace();
    } catch (FailedDBOperationException e) {
      // all database operations throw this exception, therefore we keep throwing this exception upwards and catch this
      // here.
      // A database operation error would imply that the application hasn't been able to successfully execute
      // the request. therefore, this method returns 'false', hoping that whoever calls handleDecision would retry
      // the request.
      GNS.getLogger().severe("Error handling request: " + request.toString());
      e.printStackTrace();
    }
    this.decrExecCount();
    return executed;
  }

  // For InterfaceApplication
  @Override
  public Request getRequest(String string)
          throws RequestParseException {
    //GNS.getLogger().info(">>>>>>>>>>>>>>> GET REQUEST: " + string);
    if (AppReconfigurableNodeOptions.debuggingEnabled) {
      GNS.getLogger().fine(">>>>>>>>>>>>>>> GET REQUEST: " + string);
    }
    // Special case handling of NoopPacket packets
    if (Request.NO_OP.toString().equals(string)) {
      return new NoopPacket();
    }
    try {
      JSONObject json = new JSONObject(string);
      Request request = (Request) Packet.createInstance(json, nodeConfig);
//      if (request instanceof InterfaceReplicableRequest) {
//        GNS.getLogger().info(">>>>>>>>>>>>>>>UPDATE PACKET********* needsCoordination is "
//                + ((InterfaceReplicableRequest) request).needsCoordination());
//      }
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
        GNS.getLogger().info("&&&&&&& APP " + nodeID + "&&&&&&& Getting state: " + state.toString());
      }
      return state.toString();
    } catch (RecordNotFoundException e) {
      // normal result
    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field not found exception: " + e.getMessage());
      e.printStackTrace();
    } catch (FailedDBOperationException e) {
      GNS.getLogger().severe("State not read from DB: " + e.getMessage());
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
      GNS.getLogger().info("&&&&&&& APP " + nodeID + "&&&&&&& Updating " + name + " state: " + state);
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
            GNS.getLogger().severe("Problem updating state, record already exists: " + e.getMessage());
          } catch (JSONException e) {
            GNS.getLogger().severe("Problem updating state: " + e.getMessage());
          }
        } else { // update the existing record
          try {
            NRState nrState = new NRState(state); // parse the new state
            nameRecord.updateState(nrState.valuesMap, nrState.ttl);
          } catch (JSONException | FieldNotFoundException e) {
            GNS.getLogger().severe("Problem updating state: " + e.getMessage());
          }
        }
      }
      DelayProfiler.updateDelay("restore", startTime);
      return true;
    } catch (FailedDBOperationException e) {
      GNS.getLogger().severe("Failed update exception: " + e.getMessage());
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
	 * Doing it like below works but requires all client requests to use the same mode
	 * (ssl or clear).
	 */
  @Override
  public void sendToClient(InetSocketAddress isa, JSONObject msg) throws IOException {;
    if (!disableSSL) {
    	GNS.getLogger().log(Level.INFO, "{0} sending response back to {1}: {2}", new Object[]{this,isa, msg});
      messenger.getSSLClientMessenger().sendToAddress(isa, msg);
    } else {
      messenger.getClientMessenger().sendToAddress(isa, msg);
    }
  }
  
  public String toString() {
	  return this.getClass().getSimpleName() + ":"+this.nodeID;
  }

  @Override
  public void sendToID(String id, JSONObject msg) throws IOException {
    messenger.sendToID(id, msg);
  }

  @Override
  public PingManager<String> getPingManager() {
    return pingManager;
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
