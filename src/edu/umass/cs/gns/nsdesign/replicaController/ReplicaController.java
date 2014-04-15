package edu.umass.cs.gns.nsdesign.replicaController;

import edu.umass.cs.gns.database.BasicRecordCursor;
import edu.umass.cs.gns.database.MongoRecords;
import edu.umass.cs.gns.exceptions.FailedUpdateException;
import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.GNSNIOTransportInterface;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.GNSMessagingTask;
import edu.umass.cs.gns.nsdesign.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.Replicable;
import edu.umass.cs.gns.nsdesign.packet.*;
import edu.umass.cs.gns.nsdesign.recordmap.BasicRecordMap;
import edu.umass.cs.gns.nsdesign.recordmap.MongoRecordMap;
import edu.umass.cs.gns.nsdesign.recordmap.ReplicaControllerRecord;
import edu.umass.cs.gns.nsdesign.replicationframework.ReplicationFrameworkInterface;
import edu.umass.cs.gns.util.UniqueIDHashMap;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * * DONT not use any class in package edu.umass.cs.gns.nsdesign **
 */
/**
 * Work in progress. Inactive code.
 *
 * Class implements all functionality of a replica controller.
 * We keep a single instance of this class for all names for whom this name server is a replica controller.
 * Created by abhigyan on 2/26/14.
 */
public class ReplicaController implements Replicable {

  public static final int RC_TIMEOUT_MILLIS = 3000;

//  /**
//   * object handles coordination among replicas on a request, if necessary
//   */
//  private ReplicaControllerCoordinator rcCoordinator = null;
  /**
   * ID of this node
   */
  private final int nodeID;

  /**
   * nio server
   */
  private final GNSNIOTransportInterface nioServer;

  /**
   * executor service for handling timer tasks
   */
  private final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

  /**
   * Object provides interface to the database table storing replica controller records
   */
  private final BasicRecordMap replicaControllerDB;

  private final GNSNodeConfig gnsNodeConfig;

  private final UniqueIDHashMap ongoingStopActiveRequests = new UniqueIDHashMap();

  private final UniqueIDHashMap ongoingStartActiveRequests = new UniqueIDHashMap();

  private ReplicationFrameworkInterface replicationFramework;

  /**
   * constructor object
   */
  public ReplicaController(int nodeID, HashMap<String, String> configParameters, GNSNodeConfig gnsNodeConfig,
          GNSNIOTransportInterface nioServer, ScheduledThreadPoolExecutor scheduledThreadPoolExecutor,
          MongoRecords mongoRecords) {
    Config.initialize(configParameters);
    this.nodeID = nodeID;
    this.gnsNodeConfig = gnsNodeConfig;
    this.nioServer = nioServer;
    this.scheduledThreadPoolExecutor = scheduledThreadPoolExecutor;

    this.replicaControllerDB = new MongoRecordMap(mongoRecords, MongoRecords.DBREPLICACONTROLLER);

    // todo disabling group change functionality as it is not tested at all
//		scheduledThreadPoolExecutor.scheduleAtFixedRate(new ComputeNewActivesTask(this),
//				Config.analysisIntervalMillis, Config.analysisIntervalMillis, TimeUnit.MILLISECONDS);
  }

  /**
   * BEGIN: getter methods for ReplicaController elements ***
   */
  public int getNodeID() {
    return nodeID;
  }

  public BasicRecordMap getDB() {
    return replicaControllerDB;
  }

  public GNSNIOTransportInterface getNioServer() {
    return nioServer;
  }

  public UniqueIDHashMap getOngoingStopActiveRequests() {
    return ongoingStopActiveRequests;
  }

  public UniqueIDHashMap getOngoingStartActiveRequests() {
    return ongoingStartActiveRequests;
  }

  public GNSNodeConfig getGnsNodeConfig() {
    return gnsNodeConfig;
  }

  public ScheduledThreadPoolExecutor getScheduledThreadPoolExecutor() {
    return scheduledThreadPoolExecutor;
  }

  public ReplicationFrameworkInterface getReplicationFramework() {
    return replicationFramework;
  }

  /**
   * ****END: getter methods for ReplicaController elements ***
   */
  /**
   * ****BEGIN: miscellaneous methods needed by replica controller module ***
   */
  public boolean isSmallestNodeRunning(String name, Set<Integer> nameServers) {
    Random r = new Random(name.hashCode());
    ArrayList<Integer> x1 = new ArrayList<Integer>(nameServers);
    Collections.sort(x1);
    Collections.shuffle(x1, r);
    for (int x : x1) {
      // todo do failure detection check here
      return x == nodeID;
    }
    return false;

  }

  /**
   * Returns the version number of next group of active replicas, whose current version number is 'activeVersion'.
   */
  public int getNewActiveVersion(int activeVersion) {
    return activeVersion + 1;
  }

  @Override
  public String getState(String name) {
    BasicRecordCursor iterator = replicaControllerDB.getAllRowsIterator();
    StringBuilder sb = new StringBuilder();
    int recordCount = 0;
    while (iterator.hasNext()) {
      try {
        JSONObject jsonObject = iterator.next();
        sb.append(jsonObject.toString());
        sb.append("\n");
        recordCount += 1;
      } catch (Exception e) {
        GNS.getLogger().severe("Problem creating ReplicaControllerRecord from JSON" + e);
      }
    }
    GNS.getLogger().info("Number of records whose state is read from DB: " + recordCount);
    return sb.toString();
  }

  /**
   * ReplicaControllerCoordinator calls this method to locally execute a decision.
   * Depending on packet type, it call other methods in ReplicaController package to execute request.
   */
  @Override
  public boolean updateState(String paxosID, String state) {
    if (state.length() == 0) {
      return true;
    }
    GNS.getLogger().info("Here: " + paxosID);
    int recordCount = 0;
    int startIndex = 0;
    GNS.getLogger().info("Update state: " + paxosID);
    try {
      while (true) {
        int endIndex = state.indexOf('\n', startIndex);
        if (endIndex == -1) {
          break;
        }
        String x = state.substring(startIndex, endIndex);
        if (x.length() > 0) {
          recordCount += 1;
          JSONObject json = new JSONObject(x);
          ReplicaControllerRecord rcr = new ReplicaControllerRecord(replicaControllerDB, json);

          GNS.getLogger().fine("Inserting rcr into DB ....: " + rcr + "\tjson = " + json);
          try {
            ReplicaControllerRecord.addNameRecordPrimary(replicaControllerDB, rcr);
          } catch (FailedUpdateException e) {
            ReplicaControllerRecord.updateNameRecordPrimary(replicaControllerDB, rcr);
          }
          startIndex = endIndex;
        } else {
          startIndex += 1;
        }
      }
    } catch (JSONException e) {
      e.printStackTrace();
    } catch (FailedUpdateException e) {
      GNS.getLogger().severe("Failed update exception: " + e.getMessage());
      e.printStackTrace();
    }
    GNS.getLogger().info("Number of rc records updated in DB: " + recordCount);
    return true;
  }

  @Override
  public boolean handleDecision(String name, String value, boolean recovery) {
    try {
      GNSMessagingTask msgTask = null;
      try {
        JSONObject json = new JSONObject(value);
        Packet.PacketType packetType = Packet.getPacketType(json);
        switch (packetType) {

          // add name to GNS
          case ADD_RECORD:
            msgTask = Add.executeAddRecord(new AddRecordPacket(json), this, recovery);
            break;
          case ACTIVE_ADD_CONFIRM:
            msgTask = Add.executeAddActiveConfirm(new AddRecordPacket(json), this);
            break;
          case UPDATE: // this is a special update which adds a name if that does not exist.
            msgTask = Upsert.handleUpsert(new UpdatePacket(json), this);
            break;

          // lookup actives for name
          case REQUEST_ACTIVES:
            msgTask = LookupActives.executeLookupActives(new RequestActivesPacket(json), this);
            break;

          // remove
          case REMOVE_RECORD:
            msgTask = Remove.executeMarkRecordForRemoval(new RemoveRecordPacket(json), this, recovery);
            break;
          case ACTIVE_REMOVE_CONFIRM:  // confirmation received from active replica that name is removed
            msgTask = Remove.handleActiveRemoveRecord(new OldActiveSetStopPacket(json), this, recovery);
            break;
          case RC_REMOVE:
            msgTask = Remove.executeRemoveRecord(new RemoveRecordPacket(json), this, recovery);
            break;

          // group change
          case NEW_ACTIVE_PROPOSE:
            GroupChange.executeNewActivesProposed(new NewActiveProposalPacket(json), this, recovery);
            break;
          case OLD_ACTIVE_STOP_CONFIRM_TO_PRIMARY: // confirmation from active replica that old actives have stopped
            GroupChange.handleOldActiveStop(new OldActiveSetStopPacket(json), this);
            break;
          case NEW_ACTIVE_START_CONFIRM_TO_PRIMARY:  // confirmation from active replica that new actives have started
            GroupChange.handleNewActiveStartConfirmMessage(new NewActiveSetStartupPacket(json), this);
            break;
          case GROUP_CHANGE_COMPLETE:
            GroupChange.executeActiveNameServersRunning(new GroupChangeCompletePacket(json), this, recovery);
            break;
          case NAMESERVER_SELECTION:
          case NAME_RECORD_STATS_RESPONSE:
            // todo these packets related to stats reporting are not implemented yet.
            throw new UnsupportedOperationException();
          default:
            break;
        }
        // todo after enabling group change, ensure that messages are not send on GROUP_CHANGE_COMPLETE and NEW_ACTIVE_PROPOSE.
        if (msgTask != null && !recovery) {
          GNSMessagingTask.send(msgTask, nioServer);
        }
      } catch (JSONException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
    } catch (Exception e) {
      GNS.getLogger().severe("Exception in handling decisions: " + e.getMessage());
      e.printStackTrace();
    }
    return true;
  }

  /**
   * Nuclear option for clearing out all state at GNS.
   */
  public void reset() {
    replicaControllerDB.reset();
  }

  /**
   * ****END: miscellaneous methods needed by replica controller module ***
   */
}
