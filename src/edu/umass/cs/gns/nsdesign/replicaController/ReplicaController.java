package edu.umass.cs.gns.nsdesign.replicaController;

import edu.umass.cs.gns.database.AbstractRecordCursor;
import edu.umass.cs.gns.database.MongoRecords;
import edu.umass.cs.gns.exceptions.FailedDBOperationException;
import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.IntegerPacketType;
import edu.umass.cs.gns.nio.InterfaceJSONNIOTransport;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.Replicable;
import edu.umass.cs.gns.nsdesign.Shutdownable;
import edu.umass.cs.gns.nsdesign.packet.*;
import edu.umass.cs.gns.nsdesign.recordmap.BasicRecordMap;
import edu.umass.cs.gns.nsdesign.recordmap.MongoRecordMap;
import edu.umass.cs.gns.nsdesign.recordmap.ReplicaControllerRecord;
import edu.umass.cs.gns.nsdesign.replicationframework.ReplicationFrameworkInterface;
import edu.umass.cs.gns.nsdesign.replicationframework.ReplicationFrameworkType;
import edu.umass.cs.gns.reconfiguration.InterfaceReplicable;
import edu.umass.cs.gns.reconfiguration.InterfaceRequest;
import edu.umass.cs.gns.reconfiguration.RequestParseException;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;
import edu.umass.cs.gns.util.UniqueIDHashMap;
import org.json.JSONException;
import org.json.JSONObject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Class implements all functionality of a replica controller.
 * We keep a single instance of this class for all names for whom this name server is a replica controller.
 * Created by abhigyan on 2/26/14.
 *
 * @param <NodeIDType>
 */
public class ReplicaController<NodeIDType> implements Replicable, InterfaceReplicable, ReconfiguratorInterface, Shutdownable {

  public static final int RC_TIMEOUT_MILLIS = 3000;

  /**
   * ID of this node
   */
  private final NodeIDType nodeID;

  /**
   * nio server
   */
  private final InterfaceJSONNIOTransport<NodeIDType> nioServer;

  /**
   * executor service for handling timer tasks
   */
  private final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

  /**
   * Object provides interface to the database table storing replica controller records
   */
  private final BasicRecordMap replicaControllerDB;

  private final GNSNodeConfig<NodeIDType> gnsNodeConfig;
  
  private final ConsistentReconfigurableNodeConfig<NodeIDType> nodeConfig;

  private final UniqueIDHashMap ongoingStopActiveRequests = new UniqueIDHashMap();

  private final UniqueIDHashMap ongoingStartActiveRequests = new UniqueIDHashMap();

  private final ConcurrentHashMap<NodeIDType, Double> nsRequestRates = new ConcurrentHashMap<>();

  /**
   * Algorithm for replicating name records.
   */
  private ReplicationFrameworkInterface<NodeIDType> replicationFrameworkInterface;

  /**
   * Creates a ReplicaController.
   *
   * @param nodeID
   * @param gnsNodeConfig
   * @param nioServer
   * @param scheduledThreadPoolExecutor
   * @param mongoRecords
   */
  public ReplicaController(NodeIDType nodeID, GNSNodeConfig<NodeIDType> gnsNodeConfig, InterfaceJSONNIOTransport<NodeIDType> nioServer,
          ScheduledThreadPoolExecutor scheduledThreadPoolExecutor,
          MongoRecords<NodeIDType> mongoRecords) {
    this.nodeID = nodeID;
    this.gnsNodeConfig = gnsNodeConfig;
    this.nodeConfig = new ConsistentReconfigurableNodeConfig(gnsNodeConfig);
    this.nioServer = nioServer;
    this.scheduledThreadPoolExecutor = scheduledThreadPoolExecutor;

    this.replicaControllerDB = new MongoRecordMap<NodeIDType>(mongoRecords, MongoRecords.DBREPLICACONTROLLER);

    this.replicationFrameworkInterface = ReplicationFrameworkType.instantiateReplicationFramework(Config.replicationFrameworkType, gnsNodeConfig);

    if (replicationFrameworkInterface != null) {
      int initialDelay = new Random(1000).nextInt(Config.analysisIntervalSec);
      GNS.getLogger().info("Starting task to compute new actives ... initial delay: " + initialDelay);
      scheduledThreadPoolExecutor.scheduleAtFixedRate(new ComputeNewActivesTask<NodeIDType>(this), initialDelay,
              Config.analysisIntervalSec, TimeUnit.SECONDS);
    }
  }

  /**
   * BEGIN: getter methods for ReplicaController elements
   *
   ***
   * @return
   */
  public NodeIDType getNodeID() {
    return nodeID;
  }

  public BasicRecordMap getDB() {
    return replicaControllerDB;
  }

  public InterfaceJSONNIOTransport<NodeIDType> getNioServer() {
    return nioServer;
  }

  public UniqueIDHashMap getOngoingStopActiveRequests() {
    return ongoingStopActiveRequests;
  }

  public UniqueIDHashMap getOngoingStartActiveRequests() {
    return ongoingStartActiveRequests;
  }

  @Override
  public GNSNodeConfig<NodeIDType> getGnsNodeConfig() {
    return gnsNodeConfig;
  }

  public ConsistentReconfigurableNodeConfig<NodeIDType> getNodeConfig() {
    return nodeConfig;
  }

  public ScheduledThreadPoolExecutor getScheduledThreadPoolExecutor() {
    return scheduledThreadPoolExecutor;
  }

  public ReplicationFrameworkInterface<NodeIDType> getReplicationFrameworkInterface() {
    return replicationFrameworkInterface;
  }

  /**
   * ****END: getter methods for ReplicaController elements ***
   */
  /**
   * ****BEGIN: miscellaneous methods needed by replica controller module
   *
   ***
   * @param name
   * @param nameServers
   * @return
   */
  // FIXME: Code like this really needs an explanation!!
  public boolean isSmallestNodeRunning(String name, Set<NodeIDType> nameServers) {
    Random r = new Random(name.hashCode());
    ArrayList<NodeIDType> x1 = new ArrayList<NodeIDType>(nameServers);
    Collections.sort(x1, NodeIDComparator);
    Collections.shuffle(x1, r);
    for (NodeIDType x : x1) {
      if (gnsNodeConfig.getPingLatency(x) < 9000L) {
        return x.equals(nodeID);
      }
    }
    return false;
  }

  public static Comparator<Object> NodeIDComparator = new Comparator<Object>() {

    @Override
    public int compare(Object object1, Object object2) {

      String objectName1 = object1.toString();
      String objectName2 = object2.toString();

      //ascending order
      return objectName1.compareTo(objectName2);

    }

  };

  /**
   * Returns the version number of next group of active replicas, whose current version number is 'activeVersion'.
   */
  public int getNewActiveVersion(int activeVersion) {
    return activeVersion + 1;
  }

  @Override
  public String getState(String name) {
    try {
      AbstractRecordCursor iterator = replicaControllerDB.getAllRowsIterator();
      StringBuilder sb = new StringBuilder();
      int recordCount = 0;
      while (iterator.hasNext()) {
        try {
          JSONObject jsonObject = iterator.nextJSONObject();
          sb.append(jsonObject.toString());
          sb.append("\n");
          recordCount += 1;
        } catch (Exception e) {
          GNS.getLogger().severe("Problem creating ReplicaControllerRecord from JSON" + e);
        }
      }
      GNS.getLogger().info("Number of records whose state is read from DB: " + recordCount);
      return sb.toString();
    } catch (FailedDBOperationException e) {
      GNS.getLogger().severe("Failed DB Operation. State could not be read from DB. Name: " + name);
      e.printStackTrace();
      return null;
    }
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
          if (Config.debuggingEnabled) {
            GNS.getLogger().fine("Inserting rcr into DB ....: " + rcr + "\tjson = " + json);
          }
          try {
            ReplicaControllerRecord.addNameRecordPrimary(replicaControllerDB, rcr);
          } catch (RecordExistsException e) {
            ReplicaControllerRecord.updateNameRecordPrimary(replicaControllerDB, rcr);
          }
          startIndex = endIndex;
        } else {
          startIndex += 1;
        }
      }
    } catch (JSONException e) {
      e.printStackTrace();
    } catch (FailedDBOperationException e) {
      GNS.getLogger().severe("Failed update exception: " + e.getMessage());
      e.printStackTrace();
    }
    GNS.getLogger().info("Number of rc records updated in DB: " + recordCount);
    return true;
  }

  @Override
  public boolean handleDecision(String name, String value, boolean doNotReplyToClient) {
    // if the request isn't executed
    boolean executed = false;
//    try {
    try {
      JSONObject json = new JSONObject(value);
      Packet.PacketType packetType = Packet.getPacketType(json);
      if (Config.debuggingEnabled) {
        GNS.getLogger().info("************ " + packetType + " RECOVERY IS " + doNotReplyToClient);
      }
      switch (packetType) {

        // add name to GNS
        case ADD_RECORD:
          Add.executeAddRecord(new AddRecordPacket<NodeIDType>(json, gnsNodeConfig), this, doNotReplyToClient);
          break;
        case ACTIVE_ADD_CONFIRM:
          Add.executeAddActiveConfirm(new AddRecordPacket<NodeIDType>(json, gnsNodeConfig), this);
          break;

        // lookupMultipleSystemFields actives for name
        case REQUEST_ACTIVES:
          LookupActives.executeLookupActives(new RequestActivesPacket<NodeIDType>(json, gnsNodeConfig), this, doNotReplyToClient);
          break;

        // remove
        case REMOVE_RECORD:
          Remove.executeMarkRecordForRemoval(new RemoveRecordPacket<NodeIDType>(json, gnsNodeConfig), this, doNotReplyToClient);
          break;
        case ACTIVE_REMOVE_CONFIRM:  // confirmation received from active replica that name is removed
          Remove.handleActiveRemoveRecord(new OldActiveSetStopPacket<NodeIDType>(json, gnsNodeConfig), this, doNotReplyToClient);
          break;
        case RC_REMOVE:
          Remove.executeRemoveRecord(new RemoveRecordPacket<NodeIDType>(json, gnsNodeConfig), this, doNotReplyToClient);
          break;

        // group change
        case NEW_ACTIVE_PROPOSE:
          GroupChange.executeNewActivesProposed(new NewActiveProposalPacket<NodeIDType>(json, gnsNodeConfig), this, doNotReplyToClient);
          break;
        case OLD_ACTIVE_STOP_CONFIRM_TO_PRIMARY: // confirmation from active replica that old actives have stopped
          GroupChange.handleOldActiveStop(new OldActiveSetStopPacket<NodeIDType>(json, gnsNodeConfig), this);
          break;
        case NEW_ACTIVE_START_CONFIRM_TO_PRIMARY:  // confirmation from active replica that new actives have started
          GroupChange.handleNewActiveStartConfirmMessage(new NewActiveSetStartupPacket<NodeIDType>(json, gnsNodeConfig), this);
          break;
        case GROUP_CHANGE_COMPLETE:
          GroupChange.executeActiveNameServersRunning(new GroupChangeCompletePacket(json), this, doNotReplyToClient);
          break;
        case NAMESERVER_SELECTION:
          NameStats.handleLNSVotesPacket(json, this);
          break;
        //case NAME_RECORD_STATS_RESPONSE:
        // todo this packets related to stats reporting are not implemented yet.
        //throw new UnsupportedOperationException();
        case NAME_SERVER_LOAD:
          updateNSLoad(json);
        default:
          GNS.getLogger().severe("Unexpected packet received " + packetType);
          return false;
      }
      executed = true;
      // todo after enabling group change, ensure that messages are not send on GROUP_CHANGE_COMPLETE and NEW_ACTIVE_PROPOSE.
    } catch (JSONException | IOException | FailedDBOperationException e) {
      GNS.getLogger().severe(" Hello ... exception " + value);
      e.printStackTrace();
      // all database operations throw this exception, therefore we keep throwing this exception upwards and catch this
      // here.
      // A database operation error would imply that the application hasn't been able to successfully execute
      // the request. therefore, this method returns 'false', hoping that whoever calls handleDecision would retry
      // the request.
    }
    return executed;
  }

  private void updateNSLoad(JSONObject json) throws JSONException {
    NameServerLoadPacket<NodeIDType> nsLoad = new NameServerLoadPacket<>(json, gnsNodeConfig);
    if (Config.debuggingEnabled) {
      GNS.getLogger().fine("Updated NS Load. Node: " + nsLoad.getReportingNodeID()
              + "\tPrevLoad: " + nsRequestRates.get(nsLoad.getReportingNodeID())
              + "\tNewNoad: " + nsLoad.getLoadValue() + "\t");
    }
    nsRequestRates.put(nsLoad.getReportingNodeID(), nsLoad.getLoadValue());
  }

  @Override
  public ConcurrentHashMap<NodeIDType, Double> getNsRequestRates() {
    return nsRequestRates;
  }

  /**
   * Nuclear option for clearing out all state at GNS.
   * @throws edu.umass.cs.gns.exceptions.FailedDBOperationException
   */
  public void reset() throws FailedDBOperationException {
    replicaControllerDB.reset();
  }

  @Override
  public void shutdown() {

  }

  // Methods for InterfaceReplicable
  @Override
  public boolean handleRequest(InterfaceRequest request) {
    return handleRequest(request, false); // false is the default
  }

  @Override
  public boolean handleRequest(InterfaceRequest request, boolean doNotReplyToClient) {
    return this.handleDecision(request.getServiceName(), request.toString(), doNotReplyToClient);
  }

  @Override
  public InterfaceRequest getRequest(String stringified) throws RequestParseException {
    try {
      return (InterfaceRequest) Packet.createInstance(new JSONObject(stringified), gnsNodeConfig);
    } catch (JSONException e) {
      throw new RequestParseException(e);
    }
  }

  @Override
  public Set<IntegerPacketType> getRequestTypes() {
    throw new UnsupportedOperationException("Not supported yet."); 
  }

}
