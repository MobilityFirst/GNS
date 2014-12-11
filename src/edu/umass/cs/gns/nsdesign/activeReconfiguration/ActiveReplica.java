package edu.umass.cs.gns.nsdesign.activeReconfiguration;

import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.nio.InterfaceJSONNIOTransport;
import edu.umass.cs.gns.nsdesign.*;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.DefaultGnsCoordinator;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.DummyGnsCoordinatorUnreplicated;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsCoordinatorEventual;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsCoordinatorPaxos;
import edu.umass.cs.gns.nsdesign.packet.NewActiveSetStartupPacket;
import edu.umass.cs.gns.nsdesign.packet.OldActiveSetStopPacket;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.paxos.PaxosConfig;
import edu.umass.cs.gns.replicaCoordination.ActiveReplicaCoordinator;
import edu.umass.cs.gns.util.UniqueIDHashMap;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Implements group reconfiguration functionality.
 *
 * Created by abhigyan on 3/27/14.
 */
public class ActiveReplica<NodeIDType, AppType extends Reconfigurable & Replicable> implements Shutdownable {

  private ActiveReplicaApp activeReplicaApp;

  private ActiveReplicaCoordinator coordinator;

  /**ID of this node*/
  private NodeIDType nodeID;

  /** nio server*/
  private InterfaceJSONNIOTransport nioServer;

  /** executor service for handling tasks */
  private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

  /** Configuration for all nodes in GNS **/
  private GNSNodeConfig<NodeIDType> gnsNodeConfig;

  /* FIXME: Arun: There needs to be detailed documentation of why you 
   * need UniqueIDHashMap.
   */
  private UniqueIDHashMap ongoingStateTransferRequests = new UniqueIDHashMap();

  private UniqueIDHashMap activeStartupInProgress = new UniqueIDHashMap();


  public ActiveReplicaCoordinator getCoordinator() {
    return coordinator;
  }

  /* FIXME: Arun: Really bizarre to pass a ScheduledThreadPoolExecutor in the constructor. Why
   * can't this class use an internal executor of its own? 
   */
  public ActiveReplica(NodeIDType nodeID, GNSNodeConfig<NodeIDType> gnsNodeConfig,
                       InterfaceJSONNIOTransport nioServer, ScheduledThreadPoolExecutor scheduledThreadPoolExecutor,
                       AppType reconfigurableApp) {
    this.nodeID = nodeID;
    this.gnsNodeConfig = gnsNodeConfig;
    this.nioServer = nioServer;
    this.scheduledThreadPoolExecutor = scheduledThreadPoolExecutor;

    PaxosConfig paxosConfig = new PaxosConfig();
    paxosConfig.setDebugMode(Config.debuggingEnabled);
    paxosConfig.setFailureDetectionPingMillis(Config.failureDetectionPingSec * 1000);
    paxosConfig.setFailureDetectionTimeoutMillis(Config.failureDetectionTimeoutSec * 1000);
    paxosConfig.setPaxosLogFolder(Config.paxosLogFolder + "/gnsReconfigurable");
    this.activeReplicaApp = new ActiveReplicaApp(reconfigurableApp, this);

    if (Config.singleNS && Config.dummyGNS) {  // coordinator for testing only
      this.coordinator = new DummyGnsCoordinatorUnreplicated(nodeID, gnsNodeConfig, this.activeReplicaApp);
    } else if (Config.singleNS) {  // coordinator for testing only
      this.coordinator = new DefaultGnsCoordinator(nodeID, this.activeReplicaApp);
    } else if(Config.eventualConsistency) {  // coordinator for testing only
      this.coordinator = new GnsCoordinatorEventual(nodeID, nioServer, gnsNodeConfig,
              this.activeReplicaApp, paxosConfig, Config.readCoordination);
    } else { // this is the actual coordinator
      this.coordinator = new GnsCoordinatorPaxos(nodeID, nioServer, gnsNodeConfig,
              this.activeReplicaApp, paxosConfig, Config.readCoordination);
    }
//    SendRequestLoadTask requestLoadTask = new SendRequestLoadTask(activeReplicaApp, this);
//    scheduledThreadPoolExecutor.submit(requestLoadTask);
  }

  public void handleIncomingPacket(JSONObject json) {
    try {
      Packet.PacketType type = Packet.getPacketType(json);
      switch (type) {
        // replica controller to active replica
        case NEW_ACTIVE_START:
          GroupChange.handleNewActiveStart(new NewActiveSetStartupPacket(json, gnsNodeConfig), this);
          break;
        case NEW_ACTIVE_START_FORWARD:
          GroupChange.handleNewActiveStartForward(new NewActiveSetStartupPacket(json, gnsNodeConfig), this);
          break;
        case NEW_ACTIVE_START_RESPONSE:
          GroupChange.handleNewActiveStartResponse(new NewActiveSetStartupPacket(json, gnsNodeConfig), this);
          break;
        case NEW_ACTIVE_START_PREV_VALUE_REQUEST:
          GroupChange.handlePrevValueRequest(new NewActiveSetStartupPacket(json, gnsNodeConfig), this);
          break;
        case NEW_ACTIVE_START_PREV_VALUE_RESPONSE:
          GroupChange.handlePrevValueResponse(new NewActiveSetStartupPacket(json, gnsNodeConfig), this);
          break;
        case OLD_ACTIVE_STOP:
          GroupChange.handleOldActiveStopFromReplicaController(new OldActiveSetStopPacket(json), this);
          break;
        case DELETE_OLD_ACTIVE_STATE:
          GroupChange.deleteOldActiveState(new OldActiveSetStopPacket(json), this);
          break;
      }
    } catch (JSONException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * The app will call this method after it has executed stop decision
   */
  public void stopProcessed(OldActiveSetStopPacket stopPacket) {
    GroupChange.handleStopProcessed(stopPacket, this);
  }


  public Reconfigurable getActiveReplicaApp() {
    return activeReplicaApp;
  }

  public NodeIDType getNodeID() {
    return nodeID;
  }

  public InterfaceJSONNIOTransport getNioServer() {
    return nioServer;
  }

  public ScheduledThreadPoolExecutor getScheduledThreadPoolExecutor() {
    return scheduledThreadPoolExecutor;
  }

  public GNSNodeConfig<NodeIDType> getGnsNodeConfig() {
    return gnsNodeConfig;
  }

  public UniqueIDHashMap getOngoingStateTransferRequests() {
    return ongoingStateTransferRequests;
  }

  public UniqueIDHashMap getActiveStartupInProgress() {
    return activeStartupInProgress;
  }


  @Override
  public void shutdown() {
    // nothing to do here currently, may need updating if we create a thread inside this module
  }
}
