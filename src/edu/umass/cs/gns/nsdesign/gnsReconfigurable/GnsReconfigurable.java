package edu.umass.cs.gns.nsdesign.gnsReconfigurable;

import edu.umass.cs.gns.database.ColumnField;
import edu.umass.cs.gns.database.MongoRecords;
import edu.umass.cs.gns.exceptions.FailedUpdateException;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.GNSNIOTransport;
import edu.umass.cs.gns.nsdesign.*;
import edu.umass.cs.gns.nsdesign.clientsupport.LNSUpdateHandler;
import edu.umass.cs.gns.nsdesign.packet.*;
import edu.umass.cs.gns.nsdesign.recordmap.BasicRecordMap;
import edu.umass.cs.gns.nsdesign.recordmap.MongoRecordMap;
import edu.umass.cs.gns.nsdesign.recordmap.NameRecord;
import edu.umass.cs.gns.ping.PingManager;
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
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Implements GNS module which stores the name records for all names that are replicated at this name server.
 * It contains code that is locally executed by an active replica of a name; any code which involves coordination
 * among multiple replicas of a name is included in the coordinator module.
 *
 * Created by abhigyan on 2/26/14.
 */
public class GnsReconfigurable implements Replicable, Reconfigurable {

  /**
   * ID of this node
   */
  private int nodeID;

  /**
   * nio server
   */
  private GNSNIOTransport nioServer;

  /**
   * executor service for handling tasks
   */
  private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

  /**
   * Object provides interface to the database table storing name records
   */
  private BasicRecordMap nameRecordDB;

  /**
   * Configuration for all nodes in GNS *
   */
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
      // TODO enable this after ping manager can handle a random set of IDs, instead of (0 to n)
//      // when emulating ping latencies we do not
//      PingServer.startServerThread(nodeID, gnsNodeConfig);
//      this.pingManager = new PingManager(nodeID, gnsNodeConfig);
//      this.pingManager.startPinging();
    }

    this.scheduledThreadPoolExecutor = scheduledThreadPoolExecutor;

    this.nameRecordDB = new MongoRecordMap(mongoRecords, MongoRecords.DBNAMERECORD);
  }

  public int getNodeID() {
    return nodeID;
  }

  public BasicRecordMap getDB() {
    return nameRecordDB;
  }

  public GNSNodeConfig getGNSNodeConfig() {
    return gnsNodeConfig;
  }

  public GNSNIOTransport getNioServer() {
    return nioServer;
  }

  /**
   * ActiveReplicaCoordinator calls this method to locally execute a decision.
   * Depending on request type, this method will call a private method to execute request.
   */
  @Override
  public boolean handleDecision(String name, String value, boolean recovery) {
    GNSMessagingTask msgTask = null;
    try {
      JSONObject json = new JSONObject(value);
      boolean noCoordinationState = json.has(Config.NO_COORDINATOR_STATE_MARKER);
      Packet.PacketType packetType = Packet.getPacketType(json);
      switch (packetType) {
        case DNS:
          msgTask = Lookup.executeLookupLocal(new DNSPacket(json), this);
          break;
        case UPDATE:
          msgTask = Update.executeUpdateLocal(new UpdatePacket(json), this, noCoordinationState);
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
          AddRecordPacket addRecordPacket = new AddRecordPacket(json);
          msgTask = Add.handleActiveAdd(addRecordPacket, this);
          break;
        case ACTIVE_REMOVE: // sent when a name is to be removed from GNS
          msgTask = Remove.executeActiveRemove(new OldActiveSetStopPacket(json), this, noCoordinationState);
          break;
        case CONFIRM_UPDATE:
        case CONFIRM_ADD:
        case CONFIRM_REMOVE:
          LNSUpdateHandler.handleConfirmUpdatePacket(new ConfirmUpdatePacket(json), this);
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
    return true;
  }

  private static ArrayList<ColumnField> activeStopFields = new ArrayList<ColumnField>();

  static {
    activeStopFields.add(NameRecord.ACTIVE_VERSION);
    activeStopFields.add(NameRecord.VALUES_MAP);
  }

  public boolean stopVersion(String name, short version) {
    NameRecord nameRecord;
    try {
      // we copy the active version field to old active version field,
      // and values map field to old values map field
      nameRecord = NameRecord.getNameRecordMultiField(nameRecordDB, name, activeStopFields);
      int activeVersion = nameRecord.getActiveVersion();
      nameRecord.handleCurrentActiveStop();
      // also inform
//      activeReplica.stopProcessed(name, activeVersion, true);
    } catch (FailedUpdateException e) {
      GNS.getLogger().warning("Field update exception. Message = " + e.getMessage());
    } catch (RecordNotFoundException e) {
      GNS.getLogger().warning("Record not found exception. Message = " + e.getMessage());
    } catch (FieldNotFoundException e) {
      GNS.getLogger().warning("FieldNotFoundException. " + e.getMessage());
      e.printStackTrace();
    }
    return true;
  }

  private static ArrayList<ColumnField> prevValueRequestFields = new ArrayList<ColumnField>();

  static {
    prevValueRequestFields.add(NameRecord.OLD_ACTIVE_VERSION);
    prevValueRequestFields.add(NameRecord.OLD_VALUES_MAP);
    prevValueRequestFields.add(NameRecord.TIME_TO_LIVE);
  }

  @Override
  public String getFinalState(String name, short version) {
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
    if (value == null) {
      return null;
    } else {
      return new TransferableNameRecordState(value, ttl).toString();
    }
  }

  @Override
  public void putInitialState(String name, short version, String state) {
    TransferableNameRecordState state1;
    try {
      state1 = new TransferableNameRecordState(state);
    } catch (JSONException e) {
      GNS.getLogger().severe("JSON Exception in transferred state: " + state + "name " + name + " version " + version);
      e.printStackTrace();
      return;
    }
    try {
      NameRecord nameRecord = new NameRecord(nameRecordDB, name, version, state1.valuesMap, state1.ttl);
      NameRecord.addNameRecord(nameRecordDB, nameRecord);
      if (Config.debugMode) {
        GNS.getLogger().fine(" NAME RECORD ADDED AT ACTIVE NODE: " + "name record = " + name);
      }
    } catch (RecordExistsException e) {
      NameRecord nameRecord = null;
      try {
        nameRecord = NameRecord.getNameRecord(nameRecordDB, name);
        nameRecord.handleNewActiveStart(version, state1.valuesMap, state1.ttl);
      } catch (FailedUpdateException e1) {
        GNS.getLogger().severe("Failed update execption: " + e.getMessage());
        e1.printStackTrace();
      } catch (FieldNotFoundException e1) {
        GNS.getLogger().severe("Field not found exception: " + e.getMessage());
        e1.printStackTrace();
      } catch (RecordNotFoundException e1) {
        GNS.getLogger().severe("Not possible because record just existed.");
        e1.printStackTrace();
      }
      if (Config.debugMode) {
        try {
          GNS.getLogger().fine(" NAME RECORD UPDATED AT ACTIVE  NODE. Name record = " + NameRecord.getNameRecord(nameRecordDB, name));
        } catch (RecordNotFoundException e1) {
          e1.printStackTrace();
        }
      }
    } catch (FailedUpdateException e) {
      GNS.getLogger().severe("Failed update execption: " + e.getMessage());
      e.printStackTrace();
    }
  }

  @Override
  public int deleteFinalState(String name, short version) {
    int[] versions = getCurrentOldVersions(name);
    if (versions != null) {
      int curVersion = versions[0];
      int oldVersion = versions[1];
      if (oldVersion == version) {
        if (curVersion == NameRecord.NULL_VALUE_ACTIVE_VERSION) {
          // todo test and remove record
          try {
            NameRecord.removeNameRecord(nameRecordDB, name);
          } catch (FailedUpdateException e) {
            GNS.getLogger().severe("FailedUpdateException: " + name + "\t " + version + "\t " + e.getMessage());
            e.printStackTrace();
          }
        } else {
          try {
            NameRecord nameRecord = new NameRecord(nameRecordDB, name);
            nameRecord.deleteOldState(version);
          } catch (FailedUpdateException e) {
            GNS.getLogger().severe("FailedUpdateException: " + name + "\t " + version + "\t " + e.getMessage());
            e.printStackTrace();
          } catch (FieldNotFoundException e) {
            GNS.getLogger().severe("FieldNotFoundException: " + name + "\t " + version + "\t " + e.getMessage());
            e.printStackTrace();
          }
        }
      }
    }
    return 0;
//    int[] versions = getCurrentOldVersions(name);
//    if (versions != null) {
//      int curVersion = versions[0];
//      int oldVersion = versions[1];
//      if (oldVersion == version) {
//        if (curVersion == NameRecord.NULL_VALUE_ACTIVE_VERSION) {
//          // todo test and remove record
//          NameRecord.removeNameRecord(nameRecordDB, name);
//        } else {
//          try {
//            NameRecord nameRecord = new NameRecord(nameRecordDB, name);
//            nameRecord.deleteOldState(version);
//          } catch (FieldNotFoundException e) {
//            GNS.getLogger().severe("FieldNotFoundException: " + name + "\t " + version + "\t " + e.getMessage());
//            e.printStackTrace();
//          }
//        }
//      }
//    }
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
  public boolean updateState(String name, String state) {
    try {
      TransferableNameRecordState state1 = new TransferableNameRecordState(state);
      NameRecord nameRecord = new NameRecord(nameRecordDB, name);
      nameRecord.updateState(state1.valuesMap, state1.ttl);
      // todo handle the case if record does not exist. for this update state should return record not found exception.
    } catch (JSONException e) {
      e.printStackTrace();
    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field not found exception: " + e.getMessage());
      e.printStackTrace();
    } catch (FailedUpdateException e) {
      GNS.getLogger().severe("Failed update exception: " + e.getMessage());
      e.printStackTrace();
    }
    return true;
  }

  /**
   * Nuclear option for clearing out all state at GNS.
   */
  public void reset() {
    nameRecordDB.reset();
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
      int[] versions = {nameRecord.getActiveVersion(), nameRecord.getOldActiveVersion()};
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

}
