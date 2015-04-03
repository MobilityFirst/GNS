package edu.umass.cs.gns.newApp;

import edu.umass.cs.gns.activecode.ActiveCodeHandler;
import edu.umass.cs.gns.database.ColumnField;
import edu.umass.cs.gns.database.MongoRecords;
import edu.umass.cs.gns.exceptions.FailedDBOperationException;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nio.IntegerPacketType;
import edu.umass.cs.gns.nio.InterfaceJSONNIOTransport;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.GnsApplicationInterface;
import edu.umass.cs.gns.nsdesign.clientsupport.LNSQueryHandler;
import edu.umass.cs.gns.nsdesign.clientsupport.LNSUpdateHandler;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.Add;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconLookup;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconUpdate;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.Remove;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.Select;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.TransferableNameRecordState;
import edu.umass.cs.gns.nsdesign.packet.AddRecordPacket;
import edu.umass.cs.gns.nsdesign.packet.ConfirmUpdatePacket;
import edu.umass.cs.gns.nsdesign.packet.DNSPacket;
import edu.umass.cs.gns.nsdesign.packet.NoopPacket;
import edu.umass.cs.gns.nsdesign.packet.OldActiveSetStopPacket;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.nsdesign.packet.Packet.PacketType;
import edu.umass.cs.gns.nsdesign.packet.StopPacket;
import edu.umass.cs.gns.nsdesign.packet.UpdatePacket;
import edu.umass.cs.gns.nsdesign.recordmap.BasicRecordMap;
import edu.umass.cs.gns.nsdesign.recordmap.MongoRecordMap;
import edu.umass.cs.gns.nsdesign.recordmap.NameRecord;
import edu.umass.cs.gns.reconfiguration.InterfaceReconfigurable;
import edu.umass.cs.gns.reconfiguration.InterfaceReconfigurableNodeConfig;
import edu.umass.cs.gns.reconfiguration.InterfaceReplicable;
import edu.umass.cs.gns.reconfiguration.InterfaceRequest;
import edu.umass.cs.gns.reconfiguration.InterfaceReconfigurableRequest;
import edu.umass.cs.gns.reconfiguration.RequestParseException;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;
import edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket.RequestPacket;
import edu.umass.cs.gns.util.ValuesMap;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;

/**
 * @author Westy
 * @param <NodeIDType>
 */
public class NewApp<NodeIDType> implements GnsApplicationInterface, InterfaceReplicable, InterfaceReconfigurable {

  private final static int INITIAL_RECORD_VERSION = 0;
  private final NodeIDType nodeID;
  private final ConsistentReconfigurableNodeConfig nodeConfig;
  
  
  /**
   * Object provides interface to the database table storing name records
   */
  private final BasicRecordMap nameRecordDB;
  /**
   * The Nio server
   */
  private final InterfaceJSONNIOTransport<NodeIDType> nioServer;
  /**
   * Active code handler
   */
  private ActiveCodeHandler activeCodeHandler;

  public NewApp(NodeIDType id, InterfaceReconfigurableNodeConfig nodeConfig, InterfaceJSONNIOTransport<NodeIDType> nioServer,
          MongoRecords<NodeIDType> mongoRecords) {
    this.nodeID = id;
    this.nodeConfig = new ConsistentReconfigurableNodeConfig(nodeConfig);
    this.nameRecordDB = new MongoRecordMap<>(mongoRecords, MongoRecords.DBNAMERECORD);
    this.activeCodeHandler = new ActiveCodeHandler(this, Config.activeCodeWorkerCount);
    if (Config.debuggingEnabled) {
      GNS.getLogger().info("&&&&&&& APP " + nodeID + " &&&&&&& Created " + nameRecordDB);
    }
    this.nioServer = nioServer;
  }

  private static PacketType[] types = {
    PacketType.DNS,
    PacketType.UPDATE,
    PacketType.SELECT_REQUEST,
    PacketType.SELECT_RESPONSE,
    PacketType.ACTIVE_ADD,
    PacketType.ACTIVE_REMOVE,
    PacketType.UPDATE_CONFIRM,
    PacketType.ADD_CONFIRM,
    PacketType.REMOVE_CONFIRM,
    PacketType.STOP,
    PacketType.NOOP};

  @Override
  public boolean handleRequest(InterfaceRequest request, boolean doNotReplyToClient) {
    //
    boolean executed = false;
    try {
      JSONObject json = new JSONObject(request.toString());
      // CHECK THIS!!!!!!
      boolean noCoordinationState = json.has(Config.NO_COORDINATOR_STATE_MARKER);
      if (Config.debuggingEnabled && noCoordinationState) {
        GNS.getLogger().info("*********** APP " + nodeID + " packet has NO_COORDINATOR_STATE_MARKER: " + json.toString());
      }
      Packet.PacketType packetType = Packet.getPacketType(json);
      if (Config.debuggingEnabled) {
        GNS.getLogger().info("&&&&&&& APP " + nodeID + "&&&&&&& Handling " + packetType.name() + " packet: " + json.toString());
      }
      // SOME OF THE CODE BELOW IS NOT APPLICABLE IN THE NEW APP AND IS INCLUDED JUST FOR DOC PURPOSES
      // UNTIL THE TRANSITION IS FINISHED
      switch (packetType) {
        case DNS:
          // the only dns response we should see are coming in response to LNSQueryHandler requests
          DNSPacket<NodeIDType> dnsPacket = new DNSPacket<NodeIDType>(json, nodeConfig);
          if (!dnsPacket.isQuery()) {
            LNSQueryHandler.handleDNSResponsePacket(dnsPacket, this);
          } else {
            // otherwise it's a query
            GnsReconLookup.executeLookupLocal(dnsPacket, this, noCoordinationState, doNotReplyToClient, activeCodeHandler);
          }
          break;
        case UPDATE:
          GnsReconUpdate.executeUpdateLocal(new UpdatePacket<NodeIDType>(json, nodeConfig), this,
                  noCoordinationState, doNotReplyToClient, activeCodeHandler);
          break;
        case SELECT_REQUEST:
          Select.handleSelectRequest(json, this);
          break;
        case SELECT_RESPONSE:
          Select.handleSelectResponse(json, this);
          break;
        /**
         * Packets sent from replica controller *
         */
        case ACTIVE_ADD: // sent when new name is added to GNS
          AddRecordPacket<NodeIDType> addRecordPacket = new AddRecordPacket<NodeIDType>(json, nodeConfig);
          Add.handleActiveAdd(addRecordPacket, this);
          break;
        case ACTIVE_REMOVE: // sent when a name is to be removed from GNS
          Remove.executeActiveRemove(new OldActiveSetStopPacket<NodeIDType>(json, nodeConfig), this,
                  noCoordinationState, doNotReplyToClient);
          break;
        // HANDLE CONFIRMATIONS COMING BACK FROM AN LNS (SIDE-TO-SIDE)
        case UPDATE_CONFIRM:
        case ADD_CONFIRM:
        case REMOVE_CONFIRM:
          LNSUpdateHandler.handleConfirmUpdatePacket(new ConfirmUpdatePacket<NodeIDType>(json, nodeConfig), this);
          break;
        case STOP:
          break;
        case NOOP:
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
      e.printStackTrace();
    }
    return executed;
  }

  @Override
  public InterfaceRequest getRequest(String string)
          throws RequestParseException {
    if (Config.debuggingEnabled) {
      GNS.getLogger().fine(">>>>>>>>>>>>>>> GET REQUEST: " + string);
    }
    // Hack
    if (RequestPacket.NO_OP.toString().equals(string)) {
      return new NoopPacket();
    }
    try {
      return (InterfaceRequest) Packet.createInstance(new JSONObject(string), nodeConfig);
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
      if (Config.debuggingEnabled) {
        GNS.getLogger().info(nameRecord.toString());
      }
      TransferableNameRecordState state = new TransferableNameRecordState(nameRecord.getValuesMap(), nameRecord.getTimeToLive());
      if (Config.debuggingEnabled) {
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
    if (Config.debuggingEnabled) {
      GNS.getLogger().info("&&&&&&& APP " + nodeID + "&&&&&&& Updating " + name + " state: " + state);
    }
    boolean stateUpdated = false;
    try {
      if (true) {
        TransferableNameRecordState state1 = new TransferableNameRecordState(state);
        NameRecord nameRecord = new NameRecord(nameRecordDB, name, INITIAL_RECORD_VERSION, 
                state1.valuesMap, state1.ttl,
                nodeConfig.getReplicatedReconfigurators(name));
        NameRecord.addNameRecord(nameRecordDB, nameRecord);
      } else {
        TransferableNameRecordState state1 = new TransferableNameRecordState(state);
        NameRecord nameRecord = new NameRecord(nameRecordDB, name);
        nameRecord.updateState(state1.valuesMap, state1.ttl);
      }
      stateUpdated = true;
      // todo handle the case if record does not exist. for this update state should return record not found exception.
    } catch (JSONException e) {
      e.printStackTrace();
    } catch (RecordExistsException e) {
      GNS.getLogger().severe("Record already exists: " + e.getMessage());
      e.printStackTrace();
    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field not found exception: " + e.getMessage());
      e.printStackTrace();
    } catch (FailedDBOperationException e) {
      GNS.getLogger().severe("Failed update exception: " + e.getMessage());
      e.printStackTrace();
    }
    return stateUpdated;
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
    // Making these the sender and receiver -1 means that the old confirm code
    // in Remove.executeActiveRemove won't execute which is what we want here... I think.
    // It seems to work anyway.
    //return new OldActiveSetStopPacket(name, -1, -1, -1, (short) epoch, PacketType.ACTIVE_REMOVE);
//    return new NoopAppRequest(name, epoch, (int) (Math.random() * Integer.MAX_VALUE),
//            "", AppRequest.PacketType.DEFAULT_APP_REQUEST, true);
  }

//  private final static ArrayList<ColumnField> prevValueRequestFields = new ArrayList<>();
//
//  static {
//    prevValueRequestFields.add(NameRecord.OLD_ACTIVE_VERSION);
//    prevValueRequestFields.add(NameRecord.OLD_VALUES_MAP);
//    prevValueRequestFields.add(NameRecord.TIME_TO_LIVE);
//  }
  private final static ArrayList<ColumnField> valueRequestFields = new ArrayList<>();

  static {
    valueRequestFields.add(NameRecord.ACTIVE_VERSION);
    valueRequestFields.add(NameRecord.VALUES_MAP);
    valueRequestFields.add(NameRecord.TIME_TO_LIVE);
  }

  @Override
  public String getFinalState(String name, int epoch) {
    ValuesMap value = null;
    int ttl = -1;
    try {
      NameRecord nameRecord = NameRecord.getNameRecordMultiField(nameRecordDB, name, valueRequestFields);
      value = nameRecord.getValuesMap();
      int recordVersion = nameRecord.getActiveVersion();
      if (recordVersion != epoch) {
        if (Config.debuggingEnabled) {
          GNS.getLogger().warning("&&&&&&& APP " + nodeID + " for " + name + " ignoring epoch mismatch: epoch "
                  + epoch + " record version " + recordVersion);
        }
      }
      //NameRecord nameRecord = NameRecord.getNameRecordMultiField(nameRecordDB, name, prevValueRequestFields);
      //value = nameRecord.getOldValuesOnVersionMatch(epoch);
      ttl = nameRecord.getTimeToLive();
    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field not found exception.");
    } catch (RecordNotFoundException e) {
      GNS.getLogger().severe("Record not found exception. name = " + name + " version = " + epoch);
    } catch (FailedDBOperationException e) {
      GNS.getLogger().severe("Failed DB Operation. Final state not read: name " + name + " version " + epoch);
      e.printStackTrace();
      return null;
    }
    if (value == null) {
      if (Config.debuggingEnabled) {
        GNS.getLogger().warning("&&&&&&& APP " + nodeID + " final state for " + name + " not found!");
      }
      return null;
    } else {
      if (Config.debuggingEnabled) {
        GNS.getLogger().warning("&&&&&&& APP " + nodeID + " final state for " + name + ": " + new TransferableNameRecordState(value, ttl).toString());
      }
      return new TransferableNameRecordState(value, ttl).toString();
    }
  }

  @Override
  public void putInitialState(String name, int epoch, String state) {
    if (Config.debuggingEnabled) {
      GNS.getLogger().info("&&&&&&& APP " + nodeID + " &&&&&&& Initial state: name " + name + " version " + epoch + " state " + state);
    }
    TransferableNameRecordState weirdState;
    try {
      weirdState = new TransferableNameRecordState(state);
    } catch (JSONException e) {
      GNS.getLogger().severe("JSON Exception in transferred state: " + state + "name " + name + " version " + epoch);
      e.printStackTrace();
      return;
    }
    // Keep retrying until we can store the initial state for a name in DB. 
    // Unless this step completes, future operations
    // e.g., lookupMultipleSystemFields, update, cannot succeed anyway.
    while (true) {
      try {
        try {
          NameRecord nameRecord = new NameRecord(nameRecordDB, name, epoch, weirdState.valuesMap, weirdState.ttl,
                  nodeConfig.getReplicatedReconfigurators(name));
          NameRecord.addNameRecord(nameRecordDB, nameRecord);
          if (Config.debuggingEnabled) {
            GNS.getLogger().info("&&&&&&& APP " + nodeID + " &&&&&&& NAME RECORD ADDED AT ACTIVE NODE: " + "name record = " + name);
          }
        } catch (RecordExistsException e) {
          NameRecord nameRecord;
          try {
            nameRecord = NameRecord.getNameRecord(nameRecordDB, name);
            nameRecord.handleNewActiveStart(epoch, weirdState.valuesMap, weirdState.ttl);

          } catch (FieldNotFoundException e1) {
            GNS.getLogger().severe("Field not found exception: " + e.getMessage());
            e1.printStackTrace();
          } catch (RecordNotFoundException e1) {
            GNS.getLogger().severe("Not possible because record just existed.");
            e1.printStackTrace();
          }
        }
      } catch (FailedDBOperationException e) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e1) {
          e1.printStackTrace();
        }
        GNS.getLogger().severe("Failed DB exception. Retry: " + e.getMessage());
        e.printStackTrace();
        continue;
      }
      break;
    }
  }

  @Override
  public boolean deleteFinalState(String name, int epoch) {
//    if (Config.debuggingEnabled) {
//      GNS.getLogger().info("&&&&&&& APP " + nodeID + "&&&&&&& Deleting name " + name + " version " + epoch);
//    }
    Integer recordEpoch = getEpoch(name);
    //try {
//      if (recordEpoch != null && recordEpoch == epoch) {
//        NameRecord.removeNameRecord(nameRecordDB, name);
//      } else {
        if (Config.debuggingEnabled) {
          GNS.getLogger().info("&&&&&&& APP " + nodeID + " for " + name + " ignoring delete. Epoch is "
                  + epoch + " and record version is " + recordEpoch);
        }
      //}
//    } catch (FailedDBOperationException e) {
//      GNS.getLogger().severe("Failed to delete record for " + name + " :" + e.getMessage());
//      return false;
//    }
    return true;
  }

  private final static ArrayList<ColumnField> readVersion = new ArrayList<>();

  static {
    readVersion.add(NameRecord.ACTIVE_VERSION);
  }

  @Override
  public Integer getEpoch(String name) {
    try {
      NameRecord nameRecord = NameRecord.getNameRecordMultiField(nameRecordDB, name, readVersion);
      return nameRecord.getActiveVersion();
    } catch (RecordNotFoundException e) {
      // normal result
    } catch (FieldNotFoundException e) {
      // normal result
    } catch (FailedDBOperationException e) {
      GNS.getLogger().severe("Database operation failed: " + e.getMessage());
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public NodeIDType getNodeID() {
    return nodeID;
  }

  @Override
  public BasicRecordMap getDB() {
    return nameRecordDB;
  }

  @Override
  public InterfaceReconfigurableNodeConfig<NodeIDType> getGNSNodeConfig() {
    return nodeConfig;
  }

  @Override
  public InterfaceJSONNIOTransport getNioServer() {
    return nioServer;
  }
}
