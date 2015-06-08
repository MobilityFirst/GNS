/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.newApp;

import edu.umass.cs.gigapaxos.InterfaceReplicable;
import edu.umass.cs.gigapaxos.InterfaceRequest;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.CommandHandler;
import edu.umass.cs.gns.database.ColumnField;
import edu.umass.cs.gns.database.MongoRecords;
import edu.umass.cs.gns.exceptions.FailedDBOperationException;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.newApp.clientCommandProcessor.ClientCommandProcessor;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nodeconfig.GNSConsistentReconfigurableNodeConfig;
import edu.umass.cs.gns.nodeconfig.GNSInterfaceNodeConfig;
import edu.umass.cs.gns.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.newApp.clientSupport.LNSQueryHandler;
import edu.umass.cs.gns.newApp.clientSupport.LNSUpdateHandler;
import edu.umass.cs.gns.newApp.packet.ConfirmUpdatePacket;
import edu.umass.cs.gns.newApp.packet.DNSPacket;
import edu.umass.cs.gns.newApp.packet.NoopPacket;
import edu.umass.cs.gns.newApp.packet.Packet;
import edu.umass.cs.gns.newApp.packet.Packet.PacketType;
import edu.umass.cs.gns.newApp.packet.StopPacket;
import edu.umass.cs.gns.newApp.packet.UpdatePacket;
import edu.umass.cs.gns.newApp.recordmap.BasicRecordMap;
import edu.umass.cs.gns.newApp.recordmap.MongoRecordMap;
import edu.umass.cs.gns.newApp.recordmap.NameRecord;
import edu.umass.cs.gns.ping.PingManager;
import edu.umass.cs.nio.IntegerPacketType;
import edu.umass.cs.nio.InterfaceJSONNIOTransport;
import edu.umass.cs.reconfiguration.InterfaceReconfigurable;
import edu.umass.cs.reconfiguration.InterfaceReconfigurableNodeConfig;
import edu.umass.cs.reconfiguration.InterfaceReconfigurableRequest;
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
public class NewApp implements GnsApplicationInterface, InterfaceReplicable, InterfaceReconfigurable {

  private final static int INITIAL_RECORD_VERSION = 0;
  private final String nodeID;
  private final GNSConsistentReconfigurableNodeConfig<String> nodeConfig;
  private final PingManager pingManager;
  /**
   * Object provides interface to the database table storing name records
   */
  private final BasicRecordMap nameRecordDB;
  /**
   * The Nio server
   */
  private final InterfaceJSONNIOTransport<String> nioServer;

  private boolean useLocalCCP = true;
  private ClientCommandProcessor<String> localCCP = null;

  public NewApp(String id, GNSInterfaceNodeConfig<String> nodeConfig, InterfaceJSONNIOTransport<String> nioServer,
          MongoRecords<String> mongoRecords) {
    this.nodeID = id;
    this.nodeConfig = new GNSConsistentReconfigurableNodeConfig<>(nodeConfig);
    this.pingManager = new PingManager<>(nodeID, this.nodeConfig);
    this.pingManager.startPinging();
    this.nameRecordDB = new MongoRecordMap<>(mongoRecords, MongoRecords.DBNAMERECORD);
    GNS.getLogger().info("App " + nodeID + " created " + nameRecordDB);
    this.nioServer = nioServer;
    try {
      if (useLocalCCP) {
        this.localCCP = new ClientCommandProcessor<>(
                new InetSocketAddress(nodeConfig.getNodeAddress(id), GNS.DEFAULT_CCP_TCP_PORT),
                (GNSNodeConfig) nodeConfig,
                AppReconfigurableNodeOptions.debuggingEnabled,
                (String) id,
                false,
                false,
                null);
      }
    } catch (IOException e) {
      GNS.getLogger().info("App could not create CCP:" + e);
    }
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
  public boolean handleRequest(InterfaceRequest request, boolean doNotReplyToClient) {
    boolean executed = false;
    try {
      //IntegerPacketType intPacket = request.getRequestType();
      JSONObject json = new JSONObject(request.toString());
      Packet.PacketType packetType = Packet.getPacketType(json);
      if (AppReconfigurableNodeOptions.debuggingEnabled) {
        GNS.getLogger().info("&&&&&&& APP " + nodeID + "&&&&&&& Handling " + packetType.name()
                + " packet: " + json.toString());
      }
      switch (packetType) {
        case DNS:
          // the only dns response we should see are coming in response to LNSQueryHandler requests
          DNSPacket<String> dnsPacket = new DNSPacket<String>(json, nodeConfig);
          if (!dnsPacket.isQuery()) {
            LNSQueryHandler.handleDNSResponsePacket(dnsPacket, this);
          } else {
            // otherwise it's a query
            AppLookup.executeLookupLocal(dnsPacket, this, doNotReplyToClient);
          }
          break;
        case UPDATE:
          AppUpdate.executeUpdateLocal(new UpdatePacket<String>(json, nodeConfig), this, doNotReplyToClient);
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
          GNS.getLogger().severe(" Packet type not found: " + json);
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

  //a map between request ids and host and port that the command request needs to be sent back to
  private final ConcurrentMap<Integer, CommandQuery> outStandingQueries = new ConcurrentHashMap<Integer, CommandQuery>(10, 0.75f, 3);

  // For InterfaceApplication
  @Override
  public InterfaceRequest getRequest(String string)
          throws RequestParseException {
    if (AppReconfigurableNodeOptions.debuggingEnabled) {
      GNS.getLogger().fine(">>>>>>>>>>>>>>> GET REQUEST: " + string);
    }
    // Special case handling of NoopPacket packets
    if (InterfaceRequest.NO_OP.toString().equals(string)) {
      return new NoopPacket();
    }
    try {
      JSONObject json = new JSONObject(string);
      InterfaceRequest request = (InterfaceRequest) Packet.createInstance(json, nodeConfig);
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
  public boolean handleRequest(InterfaceRequest request) {
    return this.handleRequest(request, false);
  }

  private final static ArrayList<ColumnField> curValueRequestFields = new ArrayList<>();

  static {
    curValueRequestFields.add(NameRecord.VALUES_MAP);
    curValueRequestFields.add(NameRecord.TIME_TO_LIVE);
  }

  @Override
  public String getState(String name) {
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
   *
   * @param name
   * @param state
   * @return
   */
  @Override
  public boolean updateState(String name, String state) {
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
   *
   * @param name
   * @param epoch
   * @return
   */
  @Override
  public InterfaceReconfigurableRequest getStopRequest(String name, int epoch) {
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

//  private final static ArrayList<ColumnField> valueRequestFields = new ArrayList<>();
//
//  static {
//    valueRequestFields.add(NameRecord.ACTIVE_VERSION);
//    valueRequestFields.add(NameRecord.VALUES_MAP);
//    valueRequestFields.add(NameRecord.TIME_TO_LIVE);
//  }
//
//  @Override
//  public String getFinalState(String name, int epoch) {
//    ValuesMap value = null;
//    int ttl = -1;
//    try {
//      NameRecord nameRecord = NameRecord.getNameRecordMultiField(nameRecordDB, name, valueRequestFields);
//      value = nameRecord.getValuesMap();
//      int recordVersion = nameRecord.getActiveVersion();
//      if (recordVersion != epoch) {
//        if (AppReconfigurableNode.debuggingEnabled) {
//          GNS.getLogger().warning("&&&&&&& APP " + nodeID + " for " + name + " ignoring epoch mismatch: epoch "
//                  + epoch + " record version " + recordVersion);
//        }
//      }
//      //NameRecord nameRecord = NameRecord.getNameRecordMultiField(nameRecordDB, name, prevValueRequestFields);
//      //value = nameRecord.getOldValuesOnVersionMatch(epoch);
//      ttl = nameRecord.getTimeToLive();
//    } catch (FieldNotFoundException e) {
//      GNS.getLogger().severe("Field not found exception.");
//    } catch (RecordNotFoundException e) {
//      GNS.getLogger().severe("Record not found exception. name = " + name + " version = " + epoch);
//    } catch (FailedDBOperationException e) {
//      GNS.getLogger().severe("Failed DB Operation. Final state not read: name " + name + " version " + epoch);
//      e.printStackTrace();
//      return null;
//    }
//    if (value == null) {
//      if (AppReconfigurableNode.debuggingEnabled) {
//        GNS.getLogger().warning("&&&&&&& APP " + nodeID + " final state for " + name + " not found!");
//      }
//      return null;
//    } else {
//      if (AppReconfigurableNode.debuggingEnabled) {
//        GNS.getLogger().warning("&&&&&&& APP " + nodeID + " final state for " + name + ": " + new NRState(value, ttl).toString());
//      }
//      return new NRState(value, ttl).toString();
//    }
//  }
//
//  @Override
//  public void putInitialState(String name, int epoch, String state) {
//    if (AppReconfigurableNode.debuggingEnabled) {
//      GNS.getLogger().info("&&&&&&& APP " + nodeID + " &&&&&&& Initial state: name " + name + " version " + epoch + " state " + state);
//    }
//    NRState weirdState;
//    try {
//      weirdState = new NRState(state);
//    } catch (JSONException e) {
//      GNS.getLogger().severe("JSON Exception in transferred state: " + state + "name " + name + " version " + epoch);
//      e.printStackTrace();
//      return;
//    }
//    // Keep retrying until we can store the initial state for a name in DB. 
//    // Unless this step completes, future operations
//    // e.g., lookupMultipleSystemFields, update, cannot succeed anyway.
//    while (true) {
//      try {
//        try {
//          NameRecord nameRecord = new NameRecord(nameRecordDB, name, epoch, weirdState.valuesMap, weirdState.ttl,
//                  nodeConfig.getReplicatedReconfigurators(name));
//          NameRecord.addNameRecord(nameRecordDB, nameRecord);
//          if (AppReconfigurableNode.debuggingEnabled) {
//            GNS.getLogger().info("&&&&&&& APP " + nodeID + " &&&&&&& NAME RECORD ADDED AT ACTIVE NODE: " + "name record = " + name);
//          }
//        } catch (RecordExistsException e) {
//          NameRecord nameRecord;
//          try {
//            nameRecord = NameRecord.getNameRecord(nameRecordDB, name);
//            nameRecord.handleNewActiveStart(epoch, weirdState.valuesMap, weirdState.ttl);
//
//          } catch (FieldNotFoundException e1) {
//            GNS.getLogger().severe("Field not found exception: " + e.getMessage());
//            e1.printStackTrace();
//          } catch (RecordNotFoundException e1) {
//            GNS.getLogger().severe("Not possible because record just existed.");
//            e1.printStackTrace();
//          }
//        }
//      } catch (FailedDBOperationException e) {
//        try {
//          Thread.sleep(100);
//        } catch (InterruptedException e1) {
//          e1.printStackTrace();
//        }
//        GNS.getLogger().severe("Failed DB exception. Retry: " + e.getMessage());
//        e.printStackTrace();
//        continue;
//      }
//      break;
//    }
//  }
//
//  @Override
//  public boolean deleteFinalState(String name, int epoch) {
////    if (AppReconfigurableNode.debuggingEnabled) {
////      GNS.getLogger().info("&&&&&&& APP " + nodeID + "&&&&&&& Deleting name " + name + " version " + epoch);
////    }
//    Integer recordEpoch = getEpoch(name);
//    //try {
////      if (recordEpoch != null && recordEpoch == epoch) {
////        NameRecord.removeNameRecord(nameRecordDB, name);
////      } else {
//    if (AppReconfigurableNode.debuggingEnabled) {
//      GNS.getLogger().info("&&&&&&& APP " + nodeID + " for " + name + " ignoring delete. Epoch is "
//              + epoch + " and record version is " + recordEpoch);
//    }
//    //}
////    } catch (FailedDBOperationException e) {
////      GNS.getLogger().severe("Failed to delete record for " + name + " :" + e.getMessage());
////      return false;
////    }
//    return true;
//  }
//
//  private final static ArrayList<ColumnField> readVersion = new ArrayList<>();
//
//  static {
//    readVersion.add(NameRecord.ACTIVE_VERSION);
//  }
//
//  @Override
//  public Integer getEpoch(String name) {
//    try {
//      NameRecord nameRecord = NameRecord.getNameRecordMultiField(nameRecordDB, name, readVersion);
//      return nameRecord.getActiveVersion();
//    } catch (RecordNotFoundException e) {
//      // normal result
//    } catch (FieldNotFoundException e) {
//      // normal result
//    } catch (FailedDBOperationException e) {
//      GNS.getLogger().severe("Database operation failed: " + e.getMessage());
//      e.printStackTrace();
//    }
//    return null;
//  }
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
  public InterfaceReconfigurableNodeConfig<String> getGNSNodeConfig() {
    return nodeConfig;
  }

  @Override
  public InterfaceJSONNIOTransport getNioServer() {
    return nioServer;
  }

  @Override
  public PingManager getPingManager() {
    return pingManager;
  }

  public ClientCommandProcessor<String> getLocalCCP() {
    return localCCP;
  }

}
