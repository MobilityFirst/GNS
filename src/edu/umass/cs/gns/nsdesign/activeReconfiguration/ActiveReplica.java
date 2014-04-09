package edu.umass.cs.gns.nsdesign.activeReconfiguration;

import edu.umass.cs.gns.nio.GNSNIOTransport;
import edu.umass.cs.gns.nsdesign.*;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.DefaultGnsCoordinator;
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
import java.util.HashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Implements group reconfiguration functionality.
 *
 * Created by abhigyan on 3/27/14.
 */
public class ActiveReplica<AppType extends Reconfigurable & Replicable> {

  private ActiveReplicaApp activeReplicaApp;

  private ActiveReplicaCoordinator coordinator;

  /**ID of this node*/
  private int nodeID;

  /** nio server*/
  private GNSNIOTransport nioServer;

  /** executor service for handling tasks */
  private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

  /** Configuration for all nodes in GNS **/
  private GNSNodeConfig gnsNodeConfig;


//  /** Ongoing stop requests proposed by this active replica. */
//  private ConcurrentHashMap<String, OldActiveSetStopPacket> ongoingStops =
//          new ConcurrentHashMap<String, OldActiveSetStopPacket>();

  private UniqueIDHashMap ongoingStateTransferRequests = new UniqueIDHashMap();

  private UniqueIDHashMap activeStartupInProgress = new UniqueIDHashMap();


  public ActiveReplicaCoordinator getCoordinator() {
    return coordinator;
  }

  public ActiveReplica(int nodeID, HashMap<String, String> configParameters, GNSNodeConfig gnsNodeConfig,
                       GNSNIOTransport nioServer, ScheduledThreadPoolExecutor scheduledThreadPoolExecutor,
                       AppType reconfigurableApp) {
    this.nodeID = nodeID;
    this.gnsNodeConfig = gnsNodeConfig;
    this.nioServer = nioServer;
    this.scheduledThreadPoolExecutor = scheduledThreadPoolExecutor;

    PaxosConfig paxosConfig = new PaxosConfig();
    paxosConfig.setPaxosLogFolder(Config.paxosLogFolder + "/gnsReconfigurable");
    this.activeReplicaApp = new ActiveReplicaApp(reconfigurableApp, this);

    if (Config.singleNS) {
      this.coordinator = new DefaultGnsCoordinator(nodeID, this.activeReplicaApp);
    } else {
      this.coordinator = new GnsCoordinatorPaxos(nodeID, nioServer, new NSNodeConfig(gnsNodeConfig),
              this.activeReplicaApp, paxosConfig);
    }
  }



  public void handleIncomingPacket(JSONObject json) {
    try {
      Packet.PacketType type = Packet.getPacketType(json);
      switch (type) {
        // replica controller to active replica
        case NEW_ACTIVE_START:
          GroupChange.handleNewActiveStart(new NewActiveSetStartupPacket(json), this);
          break;
        case NEW_ACTIVE_START_FORWARD:
          GroupChange.handleNewActiveStartForward(new NewActiveSetStartupPacket(json), this);
          break;
        case NEW_ACTIVE_START_RESPONSE:
          GroupChange.handleNewActiveStartResponse(new NewActiveSetStartupPacket(json), this);
          break;
        case NEW_ACTIVE_START_PREV_VALUE_REQUEST:
          GroupChange.handlePrevValueRequest(new NewActiveSetStartupPacket(json), this);
          break;
        case NEW_ACTIVE_START_PREV_VALUE_RESPONSE:
          GroupChange.handlePrevValueResponse(new NewActiveSetStartupPacket(json), this);
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
  void stopProcessed(OldActiveSetStopPacket stopPacket) {
    GroupChange.handleStopProcessed(stopPacket, this);
  }


  Reconfigurable getActiveReplicaApp() {
    return activeReplicaApp;
  }

  int getNodeID() {
    return nodeID;
  }

  GNSNIOTransport getNioServer() {
    return nioServer;
  }

  ScheduledThreadPoolExecutor getScheduledThreadPoolExecutor() {
    return scheduledThreadPoolExecutor;
  }

  GNSNodeConfig getGnsNodeConfig() {
    return gnsNodeConfig;
  }

  UniqueIDHashMap getOngoingStateTransferRequests() {
    return ongoingStateTransferRequests;
  }

  UniqueIDHashMap getActiveStartupInProgress() {
    return activeStartupInProgress;
  }


}
