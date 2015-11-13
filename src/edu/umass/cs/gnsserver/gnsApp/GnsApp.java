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

import edu.umass.cs.gigapaxos.interfaces.ClientMessenger;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;

import edu.umass.cs.gnsserver.activecode.ActiveCodeHandler;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.CommandHandler;
import edu.umass.cs.gnsserver.database.ColumnField;
import edu.umass.cs.gnsserver.database.MongoRecords;
import edu.umass.cs.gnsserver.exceptions.FailedDBOperationException;
import edu.umass.cs.gnsserver.exceptions.FieldNotFoundException;
import edu.umass.cs.gnsserver.exceptions.RecordExistsException;
import edu.umass.cs.gnsserver.exceptions.RecordNotFoundException;
import edu.umass.cs.gnsserver.main.GNS;
import static edu.umass.cs.gnsserver.gnsApp.AppReconfigurableNodeOptions.disableSSL;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.ClientCommandProcessor;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnsserver.nodeconfig.GNSConsistentReconfigurableNodeConfig;
import edu.umass.cs.gnsserver.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gnsserver.gnsApp.clientSupport.LNSQueryHandler;
import edu.umass.cs.gnsserver.gnsApp.clientSupport.LNSUpdateHandler;
import edu.umass.cs.gnsserver.gnsApp.packet.ConfirmUpdatePacket;
import edu.umass.cs.gnsserver.gnsApp.packet.DNSPacket;
import edu.umass.cs.gnsserver.gnsApp.packet.NoopPacket;
import edu.umass.cs.gnsserver.gnsApp.packet.Packet;
import edu.umass.cs.gnsserver.gnsApp.packet.Packet.PacketType;
import edu.umass.cs.gnsserver.gnsApp.packet.StopPacket;
import edu.umass.cs.gnsserver.gnsApp.packet.UpdatePacket;
import edu.umass.cs.gnsserver.gnsApp.recordmap.BasicRecordMap;
import edu.umass.cs.gnsserver.gnsApp.recordmap.MongoRecordMap;
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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Westy
 */
public class GnsApp extends AbstractReconfigurablePaxosApp<String>
        implements GnsApplicationInterface<String>, Replicable, Reconfigurable,
        ClientMessenger {

  private final static int INITIAL_RECORD_VERSION = 0;
  private final String nodeID;
  private final GNSConsistentReconfigurableNodeConfig<String> nodeConfig;
  private final PingManager<String> pingManager;
  /**
   * Object provides interface to the database table storing name records
   */
  private final BasicRecordMap nameRecordDB;
  /**
   * The Nio server
   */
  private final SSLMessenger<String, JSONObject> messenger;
  private final ClientCommandProcessor clientCommandProcessor;

  // Keep track of commands that are coming in
  /**
   *
   */
  public final ConcurrentMap<Integer, CommandHandler.CommandRequestInfo> outStandingQueries
          = new ConcurrentHashMap<>(10, 0.75f, 3);

  /**
   * Active code handler
   */
  private ActiveCodeHandler activeCodeHandler;

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
    this.nameRecordDB = new MongoRecordMap<>(mongoRecords, MongoRecords.DBNAMERECORD);
    GNS.getLogger().info("App " + nodeID + " created " + nameRecordDB);
    this.messenger = messenger;
    this.clientCommandProcessor = new ClientCommandProcessor(messenger,
            new InetSocketAddress(nodeConfig.getBindAddress(id), nodeConfig.getCcpPort(id)),
            (GNSNodeConfig<String>) nodeConfig,
            AppReconfigurableNodeOptions.debuggingEnabled,
            this,
            (String) id,
            AppReconfigurableNodeOptions.dnsGnsOnly,
            AppReconfigurableNodeOptions.dnsOnly,
            AppReconfigurableNodeOptions.gnsServerIP);
    // start the NSListenerAdmin thread
    new AppAdmin(this, (GNSNodeConfig<String>) nodeConfig).start();
    GNS.getLogger().info(nodeID.toString() + " Admin thread initialized");
    this.activeCodeHandler = new ActiveCodeHandler(this,
            AppReconfigurableNodeOptions.activeCodeWorkerCount,
            AppReconfigurableNodeOptions.activeCodeBlacklistSeconds);

  }

  private static PacketType[] types = {
    PacketType.DNS,
    PacketType.UPDATE,
    PacketType.SELECT_REQUEST,
    PacketType.SELECT_RESPONSE,
    PacketType.UPDATE_CONFIRM,
    PacketType.ADD_CONFIRM,
    PacketType.REMOVE_CONFIRM,
    PacketType.STOP,
    PacketType.NOOP,
    PacketType.COMMAND,
    PacketType.COMMAND_RETURN_VALUE};

  @Override
  public boolean execute(Request request, boolean doNotReplyToClient) {
    boolean executed = false;
    try {
      //IntegerPacketType intPacket = request.getRequestType();
      JSONObject json = new JSONObject(request.toString());
      Packet.PacketType packetType = Packet.getPacketType(json);
      if (AppReconfigurableNodeOptions.debuggingEnabled) {
        GNS.getLogger().info("&&&&&&& APP " + nodeID + "&&&&&&& Handling " + packetType.name()
                //+ " packet: " + json.toString());
                + " packet: " + json.toReasonableString());
      }
      switch (packetType) {
        case DNS:
          // the only dns response we should see are coming in response to LNSQueryHandler requests
          DNSPacket<String> dnsPacket = new DNSPacket<String>(json, nodeConfig);
          if (!dnsPacket.isQuery()) {
            LNSQueryHandler.handleDNSResponsePacket(dnsPacket, this);
          } else {
            // otherwise it's a query
            AppLookup.executeLookupLocal(dnsPacket, this, doNotReplyToClient, activeCodeHandler);
          }
          break;
        case UPDATE:
          AppUpdate.executeUpdateLocal(new UpdatePacket<String>(json, nodeConfig), this, doNotReplyToClient, activeCodeHandler);
          break;
        case SELECT_REQUEST:
          Select.handleSelectRequest(json, this);
          break;
        case SELECT_RESPONSE:
          Select.handleSelectResponse(json, this);
          break;
        // HANDLE CONFIRMATIONS COMING BACK FROM AN LNS (SIDE-TO-SIDE)
        case UPDATE_CONFIRM:
        case ADD_CONFIRM:
        case REMOVE_CONFIRM:
          LNSUpdateHandler.handleConfirmUpdatePacket(new ConfirmUpdatePacket<String>(json, nodeConfig), this);
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
          //GNS.getLogger().severe(" Packet type not found: " + json.toString());
          GNS.getLogger().severe(" Packet type not found: " + json.toReasonableString());
          return false;
      }
      executed = true;
    } catch (JSONException e) {
      e.printStackTrace();
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    } catch (SignatureException e) {
      e.printStackTrace();
    } catch (InvalidKeySpecException e) {
      e.printStackTrace();
    } catch (InvalidKeyException e) {
      e.printStackTrace();
    } catch (IOException e) {
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
    return executed;
  }

  @Override
  public void setClientMessenger(SSLMessenger<?, JSONObject> messenger) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  class CommandQuery {

    private String host;
    private int port;

    public CommandQuery(String host, int port) {
      this.host = host;
      this.port = port;
    }

    public String getHost() {
      return host;
    }

    public int getPort() {
      return port;
    }
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

  @Override
  public void sendToClient(InetSocketAddress isa, JSONObject msg) throws IOException {
//    InetSocketAddress clientAddress = new InetSocketAddress(isa.getAddress(),
//            ActiveReplica.getClientFacingPort(isa.getPort()));
    //GNS.getLogger().info("&&&&&&& APP " + nodeID + "&&&&&&& Sending to: " + isa + " " + msg);
    if (!disableSSL) {
      messenger.getClientMessenger().sendToAddress(isa, msg);
    } else {
      messenger.sendToAddress(isa, msg);
    }
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
  public ClientCommandProcessor getClientCommandProcessor() {
    return clientCommandProcessor;
  }

}
