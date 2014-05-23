package edu.umass.cs.gns.nsdesign.activeReconfiguration;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.packet.NewActiveSetStartupPacket;
import edu.umass.cs.gns.nsdesign.packet.OldActiveSetStopPacket;
import org.json.JSONException;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Helper class for ActiveReplica.
 * 
 * Implements functionality at an active replica for a changing the set of active replicas for a name. This class
 * contains two types of methods: those executed at old active replicas, and those executed at new active replicas.
 *
 * The old active replica takes following actions during group change:
 * (1) When replica controller contacts an active replica receives a message to stop current active set, it proposes
 * this stop request to all active replicas.
 * (2) After active replicas agree to stop, the app for each active replica backs up its state at the time of executing
 * the stop request. The active replica contacted by replica controller send back a confirmation to replica controller
 * that actives have stopped.
 * (3) When a new active replica contacts an old active replicas for copying state for a name, the old active replica
 * sends the backed up state, if available, to the new active replica.
 * (4) When replica controller asks old active replicas to clear any state related to this name, they do so. In case an
 * old active replica is also a member of new active replica set, it only deletes the backed up state from the time of
 * executing the stop request.
 *
 * The new active replica takes following actions during group change:
 *
 * (1) When a replica controller informs one of the new active replicas of its membership in the new group, that
 * active replica to informs all new active replicas of their membership.
 * (2) When an active replica learns about its membership in the new group from another active replica, it requests a
 * state transfer from any of the old active replicas who have executed the stop request.
 * (3) Once an active replica obtains state from an old active replica, it becomes functional as a new active replica,
 * and informs the active replica who had informed it about its membership in new set.
 * (4) When the active replica first contact by replica controller receives confirmation from a majority of new active
 * replicas that they are functional, it confirms to the replica controller that the new active replica set is
 * functional.
 *
 * Created by abhigyan on 2/27/14.
 */
public class GroupChange {



  /********************   BEGIN: methods executed at old active replicas. *********************/

  public static void handleOldActiveStopFromReplicaController(OldActiveSetStopPacket stopPacket, ActiveReplica replica)
          throws JSONException{
    replica.getCoordinator().coordinateRequest(stopPacket.toJSONObject());
    // do the check and propose to replica controller.
  }

  /**
   * Send confirmation to replica controller that actives have stopped.
   */
  public static void handleStopProcessed(OldActiveSetStopPacket stopPacket, ActiveReplica activeReplica) {
    try {
      // confirm to primary name server that this set of actives has stopped
      if (stopPacket.getActiveReceiver() == activeReplica.getNodeID()) {
        // the active node who received this node, sends confirmation to primary
        // confirm to primary
        stopPacket.changePacketTypeToConfirm();
        activeReplica.getNioServer().sendToID(stopPacket.getPrimarySender(), stopPacket.toJSONObject());
        GNS.getLogger().info("Active removed: Name Record updated. Sent confirmation to replica controller. Packet = " +
                stopPacket);
      } else {
        // other nodes do nothing.
        GNS.getLogger().info("Active removed: Name Record updated. OldVersion = " + stopPacket.getVersion());
      }
    } catch (JSONException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Responds to a request from a new active replica regarding transferring state for a name.
   */
  public static void handlePrevValueRequest(NewActiveSetStartupPacket packet, ActiveReplica activeReplica)
          throws JSONException, IOException {
    if (Config.debugMode) GNS.getLogger().info(" Received NEW_ACTIVE_START_PREV_VALUE_REQUEST at node " +
            activeReplica.getNodeID());
    // obtain name record
    String value = activeReplica.getActiveReplicaApp().getFinalState(packet.getName(), (short) packet.getOldActiveVersion());
    if (value == null) {
      packet.changePreviousValueCorrect(false);
    } else {
      // update previous value
      packet.changePreviousValueCorrect(true);
      packet.changePreviousValue(value);
    }

    packet.changePacketTypeToPreviousValueResponse();

    if (Config.debugMode) GNS.getLogger().info(" NEW_ACTIVE_START_PREV_VALUE_REQUEST reply sent to: " + packet.getSendingActive());
    // reply to sending active
    activeReplica.getNioServer().sendToID(packet.getSendingActive(), packet.toJSONObject());
  }

  /**
   * On a request from replica controller after group change is completed, this method deletes state at
   * old active replicas.
   */
  public static void deleteOldActiveState(OldActiveSetStopPacket oldActiveSetStopPacket, ActiveReplica activeReplica) {
    activeReplica.getActiveReplicaApp().deleteFinalState(oldActiveSetStopPacket.getName(),
            (short) oldActiveSetStopPacket.getVersion());
  }

  /********************   END: methods executed at old active replicas. *********************/


  /********************   BEGIN: methods executed at new active replicas. *********************/

  /**
   *  Handle message from replica controller informing of this node's membership in the new active replica set; this
   *  node informs all new active replicas (including itself) of their membership in the new set.
   *  This method also creates book-keeping state at active replica to records response from active replicas.
   */
  public static void handleNewActiveStart(NewActiveSetStartupPacket packet, ActiveReplica activeReplica)
          throws JSONException, IOException{

    // sanity check: am I in set? otherwise quit.
    if (!packet.getNewActiveNameServers().contains(activeReplica.getNodeID())) {
      GNS.getLogger().severe("ERROR: NewActiveSetStartupPacket reached a non-active name server." + packet.toString());
      return;
    }
    // create name server
    NewActiveStartInfo activeStartInfo = new NewActiveStartInfo(new NewActiveSetStartupPacket(packet.toJSONObject()));
    int requestID = activeReplica.getActiveStartupInProgress().put(activeStartInfo);
    // send to all nodes, except self
    packet.changePacketTypeToForward();
    packet.setUniqueID(requestID); // this ID is set by active replica for identifying this packet.
    if (Config.debugMode) GNS.getLogger().info("NEW_ACTIVE_START: forwarded msg to nodes; "
            + packet.getNewActiveNameServers());
    for (int nodeID: packet.getNewActiveNameServers()) {
      if (activeReplica.getNodeID() != nodeID) { // exclude myself
        activeReplica.getNioServer().sendToID(nodeID, packet.toJSONObject());
      }
    }
    CopyStateFromOldActiveTask copyTask = new CopyStateFromOldActiveTask(packet, activeReplica);
    activeReplica.getScheduledThreadPoolExecutor().scheduleAtFixedRate(copyTask, 0, Config.NS_TIMEOUT_MILLIS,
            TimeUnit.MILLISECONDS);
  }


  /**
   * This active replica learns that it one of the new active replica, upon which it creates a task to copy
   * state from one of the old active replicas who have executed the stop request.
   */
  public static void handleNewActiveStartForward(NewActiveSetStartupPacket packet, ActiveReplica activeReplica)
          throws JSONException, IOException {

    CopyStateFromOldActiveTask copyTask = new CopyStateFromOldActiveTask(packet, activeReplica);
    activeReplica.getScheduledThreadPoolExecutor().scheduleAtFixedRate(copyTask, 0, Config.NS_TIMEOUT_MILLIS,
            TimeUnit.MILLISECONDS);
  }

  /**
   * One of the old active replica have responded to this node's request for transferring state for a name. If the
   * response is valid, this node becomes functional as a new active replica and confirms back to that active replica
   * who informed it of its membership in new group.
   */
  public static void handlePrevValueResponse(NewActiveSetStartupPacket packet, ActiveReplica activeReplica)
          throws JSONException, IOException{
    if (Config.debugMode) GNS.getLogger().info(" Received NEW_ACTIVE_START_PREV_VALUE_RESPONSE at node " + activeReplica.getNodeID());
    if (packet.getPreviousValueCorrect()) {
      activeReplica.getActiveReplicaApp().putInitialState(packet.getName(), (short) packet.getNewActiveVersion(),
              packet.getPreviousValue());
      //
      activeReplica.getCoordinator().coordinateRequest(packet.toJSONObject());
      NewActiveSetStartupPacket originalPacket = (NewActiveSetStartupPacket) activeReplica.getOngoingStateTransferRequests().remove(packet.getUniqueID());
      if (originalPacket != null) {
        // send back response to the active who forwarded this packet to this node.
        originalPacket.changePacketTypeToResponse();
        int sendingActive = originalPacket.getSendingActive();
        originalPacket.changeSendingActive(activeReplica.getNodeID());

        if (Config.debugMode) GNS.getLogger().info("NEW_ACTIVE_START: replied to active sending the startup packet from node: " + sendingActive);

        activeReplica.getNioServer().sendToID(sendingActive, originalPacket.toJSONObject());
      } else {
        if (Config.debugMode) GNS.getLogger().info(" NewActiveSetStartupPacket not found for response.");
      }
    } else {
      if (Config.debugMode) GNS.getLogger().info(" Old Active did not return previous value.");
    }
  }

  /**
   * This method is executed at the new active replica first contact by replica controller.
   * This message received confirms that one of the new active replica is functional now. This node checks if it has
   * received such messages from a majority of new replicas. If so, it confirms to the replica controller
   * that the new active replica set is functional.
   */
  public static void handleNewActiveStartResponse(NewActiveSetStartupPacket packet, ActiveReplica activeReplica)
          throws JSONException, IOException {
    NewActiveStartInfo info = (NewActiveStartInfo) activeReplica.getActiveStartupInProgress().get(packet.getUniqueID());
    if (Config.debugMode) GNS.getLogger().info("NEW_ACTIVE_START: received confirmation from node: " +
            packet.getSendingActive());
    if (info != null) {
      info.receivedResponseFromActive(packet.getSendingActive());
      if (info.haveMajorityActivesResponded()) {
        if (Config.debugMode) GNS.getLogger().info("NEW_ACTIVE_START: received confirmation from majority. name = " + packet.getName());
        info.originalPacket.changePacketTypeToConfirmation();
        activeReplica.getNioServer().sendToID(info.originalPacket.getSendingPrimary(), info.originalPacket.toJSONObject());
        activeReplica.getActiveStartupInProgress().remove(packet.getUniqueID());
      }
    }

  }


  /********************   END: methods executed at new active replicas. *********************/
}
