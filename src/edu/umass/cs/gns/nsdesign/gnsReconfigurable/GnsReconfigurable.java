package edu.umass.cs.gns.nsdesign.gnsReconfigurable;

import edu.umass.cs.gns.database.ColumnField;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.GNSNIOTransport;
import edu.umass.cs.gns.nsdesign.*;
import edu.umass.cs.gns.nsdesign.activeReconfiguration.ActiveReplica;
import edu.umass.cs.gns.nsdesign.activeReconfiguration.ActiveReplicaCoordinatorPaxos;
import edu.umass.cs.gns.nsdesign.packet.*;
import edu.umass.cs.gns.nsdesign.recordmap.BasicRecordMap;
import edu.umass.cs.gns.nsdesign.recordmap.MongoRecordMap;
import edu.umass.cs.gns.nsdesign.recordmap.MongoRecords;
import edu.umass.cs.gns.nsdesign.recordmap.NameRecord;
import edu.umass.cs.gns.paxos.PaxosConfig;
import edu.umass.cs.gns.paxos.PaxosInterface;
import edu.umass.cs.gns.ping.PingManager;
import edu.umass.cs.gns.ping.PingServer;
import edu.umass.cs.gns.replicaCoordination.ActiveReplicaCoordinator;
import edu.umass.cs.gns.util.ValuesMap;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/*** DONT not use any class in package edu.umass.cs.gns.nsdesign ***/

/**
 * Work in progress. Inactive code.
 *
 * Implements functionality of an active replica of a name.
 * We keep a single instance of this class for all names for whom this name server is an active replica.
 * Created by abhigyan on 2/26/14.
 */
public class GnsReconfigurable implements PaxosInterface, Reconfigurable {

  /** object handles coordination among replicas on a request, if necessary */
  private ActiveReplicaCoordinator activeCoordinator = null;

  private ActiveReplica activeReplica;


  /**ID of this node*/
  private int nodeID;

  /** nio server*/
  private GNSNIOTransport nioServer;

  /** executor service for handling tasks */
  private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

  /** Object provides interface to the database table storing name records */
  private BasicRecordMap nameRecordDB;

  /** Configuration for all nodes in GNS **/
  private GNSNodeConfig gnsNodeConfig;

  private PingManager pingManager;

  /**
   * constructor object
   */
  public GnsReconfigurable(int nodeID, HashMap<String, String> configParameters, GNSNodeConfig gnsNodeConfig,
                           GNSNIOTransport nioServer, ScheduledThreadPoolExecutor scheduledThreadPoolExecutor,
                           MongoRecords mongoRecords) {
    this.nodeID = nodeID;

    this.gnsNodeConfig = gnsNodeConfig;

    this.nioServer = nioServer;

    if (!Config.emulatePingLatencies) {
      // when emulating ping latencies we do not
      PingServer.startServerThread(nodeID, gnsNodeConfig);
      this.pingManager = new PingManager(nodeID, gnsNodeConfig);
      this.pingManager.startPinging();
    }

    this.scheduledThreadPoolExecutor = scheduledThreadPoolExecutor;

    this.nameRecordDB = new MongoRecordMap(mongoRecords, MongoRecords.DBNAMERECORD);

    if (!Config.singleNS) {
      PaxosConfig paxosConfig = new PaxosConfig();
      paxosConfig.setPaxosLogFolder(Config.paxosLogFolder + "/gnsReconfigurable");
      this.activeCoordinator = new ActiveReplicaCoordinatorPaxos(nodeID, nioServer, new NSNodeConfig(gnsNodeConfig), this, paxosConfig);
    }
    this.activeReplica = new ActiveReplica(nodeID, configParameters, gnsNodeConfig, nioServer, this.scheduledThreadPoolExecutor, this);

  }

  /**
   * Entry point for all packets sent to active replica.
   *
   * Currently, we are implementing a single unreplicated active replica and replica controller.
   * So, we do not take any action on some packet types.
   * @param json json object received at name server
   */
  public void handleIncomingPacket(JSONObject json){
    // Types of packets:
    // (1) Lookup (from LNS)
    // (2) Update (from LNS)
    // (3) Add (from ReplicaController)  -- after completing add, sent reply to ReplicaController
    // (4) Remove (from ReplicaController) -- after completing remove, send reply to ReplicaController
    // (5) Group change (from ReplicaController) -- after completing group change, send reply to ReplicaController

    // and finally
    //  (6) ActiveReplicaCoordinator packets (from other ActiveReplicaCoordinator)
    try {
      Packet.PacketType type = Packet.getPacketType(json);

      GNSMessagingTask msgTask = null;
      switch (type) {
        /** Packets sent from LNS **/
        case DNS:     // lookup sent by lns
          if (activeCoordinator == null) {
            msgTask = Lookup.executeLookupLocal(new DNSPacket(json), this);
          } else {
            activeCoordinator.coordinateRequest(json);
          }
          break;
        case UPDATE_ADDRESS_LNS: // update sent by lns.
          msgTask = Update.handleUpdate(json, this);
          break;
        case SELECT_REQUEST:
          Select.handleSelectRequest(json, this);
          break;
        case SELECT_RESPONSE:
          Select.handleSelectResponse(json, this);
          break;

        /** Packets sent from replica controller **/
        case ACTIVE_ADD: // sent when new name is added to GNS
          AddRecordPacket addRecordPacket = new AddRecordPacket(json);
          msgTask = Add.handleActiveAdd(addRecordPacket, this);

          break;
        case ACTIVE_REMOVE: // sent when a name is to be removed from GNS
          msgTask = Remove.handleActiveRemovePacket(new OldActiveSetStopPacket(json), this);
          break;

        /** Packets from coordination modules at active replica **/
        case ACTIVE_COORDINATION:
          activeCoordinator.coordinateRequest(json);
          break;
        default:
          GNS.getLogger().warning("No handler for packet type: " + type.toString());
          break;
      }
      if (msgTask != null) {
        GNSMessagingTask.send(msgTask, nioServer);
      }
    } catch (JSONException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    } catch (InvalidKeySpecException e) {
      e.printStackTrace();
    } catch (InvalidKeyException e) {
      e.printStackTrace();
    } catch (SignatureException e) {
      // todo what to do in these cases?
      e.printStackTrace();
    }
  }

  public int getNodeID(){
    return nodeID;
  }

  public BasicRecordMap getDB(){
    return nameRecordDB;
  }

  public GNSNodeConfig getGNSNodeConfig(){
    return gnsNodeConfig;
  }

  public GNSNIOTransport getNioServer(){
    return nioServer;
  }

  public ActiveReplicaCoordinator getActiveCoordinator() {
    return activeCoordinator;
  }

  public ActiveReplica getActiveReplica() {
    return activeReplica;
  }

  /**
   * ActiveReplicaCoordinator calls this method to locally execute a decision.
   * Depending on request type, this method will call a private method to execute request.
   */
  @Override
  public void handleDecision(String name, String value, boolean recovery) {
    GNSMessagingTask msgTask = null;
    try {
      JSONObject json = new JSONObject(value);
      Packet.PacketType packetType = Packet.getPacketType(json);
      switch (packetType) {
        case DNS:
          msgTask = Lookup.executeLookupLocal(new DNSPacket(json), this);
          break;
        case UPDATE_ADDRESS_LNS:
          msgTask = Update.executeUpdateLocal(new UpdateAddressPacket(json), this);
          break;
        case SELECT_REQUEST:
          Select.handleSelectRequest(json, this);
          break;
        case SELECT_RESPONSE:
          Select.handleSelectResponse(json, this);
          break;

        /** Packets sent from replica controller **/
        case ACTIVE_ADD: // sent when new name is added to GNS
          AddRecordPacket addRecordPacket = new AddRecordPacket(json);
          msgTask = Add.executeSendConfirmation(addRecordPacket, this);
          break;

        default:
          GNS.getLogger().severe(" Packet type not found: " + json);
          break;

      }
      if (msgTask != null && !recovery) {
        GNSMessagingTask.send(msgTask, nioServer);
      }
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
    }
  }

  private static ArrayList<ColumnField> activeStopFields = new ArrayList<ColumnField>();

  static {
      activeStopFields.add(NameRecord.ACTIVE_VERSION);
      activeStopFields.add(NameRecord.VALUES_MAP);
  }

  @Override
  public void stop(String name, String value) {
    GNSMessagingTask msgTask = null;
    try {
      JSONObject json = new JSONObject(value);
      Packet.PacketType packetType = Packet.getPacketType(json);
      switch (packetType) {
        case ACTIVE_REMOVE: // sent when a name is to be removed from GNS
          msgTask = Remove.executeActiveRemove(new OldActiveSetStopPacket(json), this);
          break;
        case OLD_ACTIVE_STOP:
          handleActiveStop(name, value);
          break;
        default:
          GNS.getLogger().severe(" Packet type not found: " + json);
          break;

      }
      if (msgTask != null) {
        GNSMessagingTask.send(msgTask, nioServer);
      }
    } catch (JSONException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void handleActiveStop(String name, String value) {
    NameRecord nameRecord;
    try {
      // we copy the active version field to old active version field,
      // and values map field to old values map field
      nameRecord = NameRecord.getNameRecordMultiField(nameRecordDB, name, activeStopFields);
      int activeVersion = nameRecord.getActiveVersion();
      nameRecord.handleCurrentActiveStop();
      // also inform
      activeReplica.stopProcessed(name, activeVersion, true);
    } catch (RecordNotFoundException e) {
      GNS.getLogger().info("Record not found exception. Message = " + e.getMessage());
    }catch (FieldNotFoundException e) {
      GNS.getLogger().info("FieldNotFoundException. " + e.getMessage());
      e.printStackTrace();
    }
  }


  private static ArrayList<ColumnField> prevValueRequestFields = new ArrayList<ColumnField>();

  static {
      prevValueRequestFields.add(NameRecord.OLD_ACTIVE_VERSION);
      prevValueRequestFields.add(NameRecord.OLD_VALUES_MAP);
      prevValueRequestFields.add(NameRecord.TIME_TO_LIVE);
  }

  @Override
  public void stopVersion(String name, int version) {

    // there are three outcomes of this method.
    // 1. propose stop to coordinator
    // 2. confirm to activeReplica that version stopped successfully
    // 3. confirm to activeReplica that there is an error in handling stop message.

    Boolean success = null; // is this is non-null at the end of method we will send response to active replica.
    int[] versions = getCurrentOldVersions(name);
    if (versions != null) {
      boolean printWarning = false;
      int curVersion = versions[0];
      int oldVersion = versions[1];
      if (curVersion != NameRecord.NULL_VALUE_ACTIVE_VERSION) {
        if (curVersion == version) {
          if (getActiveCoordinator() != null) {
            OldActiveSetStopPacket stopPacket = new OldActiveSetStopPacket(name, 0, -1, nodeID, version,
                    Packet.PacketType.OLD_ACTIVE_STOP);
            try {
              getActiveCoordinator().coordinateRequest(stopPacket.toJSONObject());
            } catch (JSONException e) {
              e.printStackTrace();
            }
          } else {
            handleActiveStop(name, null);
            // "handleActiveStop" will send confirmation to active replica, so we do not need to send it.
          }
        } else if (curVersion > version) {
          success = true;
          if (curVersion != version + 1) printWarning = true;

        } else {
          success = false;
          printWarning = true;
        }

      } else if (oldVersion != NameRecord.NULL_VALUE_ACTIVE_VERSION) {
        if (oldVersion >= version) {
          success = true;
          if (oldVersion != version) printWarning = true;
        } else {
          success = false;
          printWarning = true;
        }
      }
      if (printWarning) {
        GNS.getLogger().warning("Unexpected version found. Expected version " + version +
                " Found current version = " + curVersion + " Old version = " + oldVersion + " Name: " + name);
      }

    }  else {
      GNS.getLogger().severe("Neither current nor old version found. Name =  " + name + " Version = " + version);
      success = false;

    }

    if (success != null) {
        activeReplica.stopProcessed(name, version, success);
    }
  }

  @Override
  public String getFinalState(String name, int version) {
    ValuesMap value = null;
    int ttl = -1;
    try {
      NameRecord nameRecord = NameRecord.getNameRecordMultiField(nameRecordDB, name, prevValueRequestFields);

        value = nameRecord.getOldValuesOnVersionMatch(version);
        ttl = nameRecord.getTimeToLive();
      } catch (FieldNotFoundException e) {
        GNS.getLogger().severe("Field not found exception.");
      } catch (RecordNotFoundException e) {
        GNS.getLogger().severe("Record not found exception. name = " + name + " version = " + version);
      }
      if (value == null) return null;
      else {
        return new TransferableNameRecordState(value, ttl).toString();
      }
  }

  @Override
  public void putInitialState(String name, int version, String state, Set<Integer> activeNameServers) {
    TransferableNameRecordState state1;
    try {
      state1 = new TransferableNameRecordState(state);
    } catch (JSONException e) {
      GNS.getLogger().severe("JSON Exception in transferred state: " + state   + "name " + name + " version " + version);
      e.printStackTrace();
      return;
    }

    // todo clear state for old
    try {

      NameRecord nameRecord = new NameRecord(nameRecordDB, name, activeNameServers, version, state1.valuesMap, state1.ttl);
      NameRecord.addNameRecord(nameRecordDB, nameRecord);
      if (Config.debugMode) GNS.getLogger().info(" NAME RECORD ADDED AT ACTIVE NODE: " + "name record = " + name);

    } catch (RecordExistsException e) {
      NameRecord nameRecord = null;
      try {
        nameRecord = NameRecord.getNameRecord(nameRecordDB, name);
        nameRecord.handleNewActiveStart(activeNameServers, version, state1.valuesMap, state1.ttl);
      } catch (FieldNotFoundException e1) {
        GNS.getLogger().severe("Field not found exception: " + e.getMessage());
        e1.printStackTrace();
      }
      catch (RecordNotFoundException e1) {
        GNS.getLogger().severe("Not possible because record just existed.");
        e1.printStackTrace();
      }
      if (Config.debugMode) GNS.getLogger().info(" NAME RECORD UPDATED AT ACTIVE  NODE. Name record = " + nameRecord);
    }
    if (getActiveCoordinator() != null) {
      try {
        getActiveCoordinator().coordinateRequest(new JSONObject(new NewActiveSetStartupPacket(name, 0, nodeID, activeNameServers, null, 0,version, Packet.PacketType.NEW_ACTIVE_START, state, true).toJSONObject()));
      } catch (JSONException e) {
        e.printStackTrace();
      }
    }

  }

  @Override
  public int deleteFinalState(String name, int version) {

    int [] versions = getCurrentOldVersions(name);
    if (versions != null) {
      int curVersion = versions[0];
      int oldVersion = versions[1];
      if (oldVersion == version) {
        if (curVersion == NameRecord.NULL_VALUE_ACTIVE_VERSION) {
          // todo test and remove record
          NameRecord.removeNameRecord(nameRecordDB, name);
        } else {
          try {
            NameRecord nameRecord = new NameRecord(nameRecordDB, name);
            nameRecord.deleteOldState(version);
          } catch (FieldNotFoundException e) {
            GNS.getLogger().severe("FieldNotFoundException: " + name + "\t " + version  + "\t " + e.getMessage());
            e.printStackTrace();
          }
        }
      }
    }
    return 0;
  }

  private static ArrayList<ColumnField> curValueRequestFields = new ArrayList<ColumnField>();

  static {
    curValueRequestFields.add(NameRecord.VALUES_MAP);
    curValueRequestFields.add(NameRecord.TIME_TO_LIVE);
  }

  @Override
  public String getState(String name) {

    try {
      NameRecord nameRecord = NameRecord.getNameRecordMultiField(nameRecordDB, name, curValueRequestFields);
      GNS.getLogger().fine(nameRecord.toString());
      return new TransferableNameRecordState(nameRecord.getValuesMap(), nameRecord.getTimeToLive()).toString();
    } catch (RecordNotFoundException e) {
      GNS.getLogger().severe("Record not found for name: " + name);
      e.printStackTrace();
    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field not found exception: " + e.getMessage());
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public void updateState(String name, String state) {
    try {
      TransferableNameRecordState state1  = new TransferableNameRecordState(state);
      NameRecord nameRecord = new NameRecord(nameRecordDB, name);
      nameRecord.updateState(state1.valuesMap, state1.ttl);
      // todo handle the case if record does not exist. for this update state should return record not found exception.
    } catch (JSONException e) {
      e.printStackTrace();
    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field not found exception: " + e.getMessage());
      e.printStackTrace();
    }

  }

  @Override
  public void deleteStateBeforeRecovery() {
    // we are doing nothing here. because we will not be adding records, but updating them.
  }

  public void resetGNS() {
    nameRecordDB.reset();
    activeCoordinator.reset();
  }

  private static ArrayList<ColumnField> readVersions = new ArrayList<ColumnField>();

  static {
    readVersions.add(NameRecord.ACTIVE_VERSION);
    readVersions.add(NameRecord.OLD_ACTIVE_VERSION);
  }


//  @Override
  private int[] getCurrentOldVersions(String name) {
    try {
      NameRecord nameRecord = NameRecord.getNameRecordMultiField(nameRecordDB, name, readVersions);
      int [] versions =  {nameRecord.getActiveVersion(), nameRecord.getOldActiveVersion()};
      return versions;
    } catch (RecordNotFoundException e) {
      GNS.getLogger().severe("Record not found for name: " + name);
      e.printStackTrace();
    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field not found exception: " + e.getMessage());
      e.printStackTrace();
    }
    return null;
  }

  public PingManager getPingManager() {
    return pingManager;
  }

  public void setPingManager(PingManager pingManager) {
    this.pingManager = pingManager;
  }
}
