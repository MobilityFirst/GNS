package edu.umass.cs.gns.nsdesign.replicaController;


import edu.umass.cs.gns.database.ColumnField;
import edu.umass.cs.gns.exceptions.FailedUpdateException;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.recordmap.ReplicaControllerRecord;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.packet.*;
import org.json.JSONException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * Implements group changes in the active replica set of a name. A group change consists of these steps:
 * (1) A replica controller, called manager, proposing to change active replica set and all replica controllers agreeing to it.
 * (2) The manager replica controller sends request to a current active replicas to stop.
 * (3) That active replica confirming to the manager that current active replicas have stopped.
 * (4) The manager informs one of the new active replicas to start functioning, who in turn inform others.
 * (5) That active replica confirming to the manager that majority of new active replicas have started functioning.
 * (6) Manager proposes an update to replica controllers that group change is completed and all replica controllers agreeing to it.
 * (7) Last step is to clean up state at old active replicas, for which manage informs all old active replicas to clean
 * up state for this name record.
 *
 * <p>
 * We ensure that there is only one group change for a name at any time. To this end, when replica controllers
 * agree on a group change, they update a variable indicating that group change is progress; replica controllers
 * do not agree to any other group change unless this group change is marked as completed. After group change is
 * completed, they again update this variable to indicate that group change has completed. This second update is important
 * because the next group change cannot start unless this update is done.</p>
 *
 * <p>
 * If a manager does get a response from either old or new active replicas, it retries sending those messages.
 * We expect that the request are successful on a few retries. Otherwise, the manager gives up, and group change is left
 * incomplete at that stage.</p>
 *
 * <p>
 * Also refer to documentation in activeReconfiguration/GroupChange on how active replicas implement group change.</p>
 *
 *
 * Created by abhigyan on 2/27/14.
 */
public class GroupChange {

  /**
   * These are set of fields that different methods will read from database. We try to keep variable names
   * similar to method names. If method name changes, update variable names accordingly.
   */
  private static ArrayList<ColumnField> executeNewActivesProposedFields = new ArrayList<ColumnField>();

  static {
    executeNewActivesProposedFields.add(ReplicaControllerRecord.MARKED_FOR_REMOVAL);
    executeNewActivesProposedFields.add(ReplicaControllerRecord.ACTIVE_NAMESERVERS_RUNNING);
    executeNewActivesProposedFields.add(ReplicaControllerRecord.ACTIVE_NAMESERVERS);
    executeNewActivesProposedFields.add(ReplicaControllerRecord.ACTIVE_VERSION);
  }

  private static ArrayList<ColumnField> startActiveSetFields = new ArrayList<ColumnField>();

  static {
    startActiveSetFields.add(ReplicaControllerRecord.OLD_ACTIVE_VERSION);
    startActiveSetFields.add(ReplicaControllerRecord.ACTIVE_VERSION);
    startActiveSetFields.add(ReplicaControllerRecord.OLD_ACTIVE_NAMESERVERS);
    startActiveSetFields.add(ReplicaControllerRecord.ACTIVE_NAMESERVERS_RUNNING);
    startActiveSetFields.add(ReplicaControllerRecord.ACTIVE_NAMESERVERS);
    startActiveSetFields.add(ReplicaControllerRecord.PRIMARY_NAMESERVERS);
  }

  private static ArrayList<ColumnField> newActiveStartedFields = new ArrayList<ColumnField>();

  static {
    newActiveStartedFields.add(ReplicaControllerRecord.MARKED_FOR_REMOVAL);
    newActiveStartedFields.add(ReplicaControllerRecord.ACTIVE_VERSION);
    newActiveStartedFields.add(ReplicaControllerRecord.ACTIVE_NAMESERVERS);
    newActiveStartedFields.add(ReplicaControllerRecord.ACTIVE_NAMESERVERS_RUNNING);
  }

  /**
   * After replica controllers agree on changing the set of active replicas, this method updates the database to
   * indicate a group change for this name is in progress.
   *
   * @param activeProposalPacket Actives proposed to primary replicas
   */
  public static void executeNewActivesProposed(NewActiveProposalPacket activeProposalPacket,
                                               ReplicaController replicaController, boolean recovery) {

    try {
      ReplicaControllerRecord rcRecord = ReplicaControllerRecord.getNameRecordPrimaryMultiField(
              replicaController.getDB(), activeProposalPacket.getName(), executeNewActivesProposedFields);
      GNS.getLogger().fine("Record Read: " + rcRecord);
      if (rcRecord == null) {
        // this is protective NULL check as we don't expect this method to return a null value.
        // if record does not exist, it throws null pointer exception.
        return;
      }

      if (rcRecord.isMarkedForRemoval()) {
        // this could happen in rare cases when remove request for a name arrives at the same time as a group change
        GNS.getLogger().fine(" DECISION NOT APPLIED: actives not changed because rcRecord is markedForRemoval ");
        return;
      }
      if (!rcRecord.isActiveRunning()) {
        // this means another group change is already in progress. this can happen if another node proposed a group
        // change at same time OR  previous group change did not complete successfully.
        GNS.getLogger().warning(" DECISION NOT APPLIED. Because most recently "
                + "proposed active name servers is not yet running: " + rcRecord.getActiveNameservers());
        return;
      }
      if (rcRecord.getActiveVersion() == activeProposalPacket.getVersion()) {
        GNS.getLogger().info(" DECISION NOT APPLIED. Old and new versions are same" + rcRecord.getActiveVersion());
        return;
      }
      GNS.getLogger().fine(" DECISION: Name = " + activeProposalPacket.getName() + " Actives: "
              + activeProposalPacket.getProposedActiveNameServers());
      // All primaries will apply this decision.
      rcRecord.updateActiveNameServers(activeProposalPacket.getProposedActiveNameServers(),
              activeProposalPacket.getVersion());

      GNS.getLogger().fine("Name Record Now: = " + rcRecord.toString());
      if (!recovery) {
        // Next step: stop old actives
        if (activeProposalPacket.getProposingNode() == replicaController.getNodeID()) {// if I have proposed this change, I will start actives group change process
          // todo could use failure detector here NameServer.getManager().isNodeUp(activeProposalPacket.getProposingNode()) == false
          // todo else if proposing node has failed, then also I will start group change
          if (Config.debugMode) {
            GNS.getLogger().fine(" : Stop oldActiveSet now: Name = " + activeProposalPacket.getName());
          }
          StopActiveSetTask stopTask = new StopActiveSetTask(activeProposalPacket.getName(),
                  rcRecord.getOldActiveNameservers(), rcRecord.getOldActiveVersion(),
                  Packet.PacketType.OLD_ACTIVE_STOP, activeProposalPacket, replicaController);
          replicaController.getScheduledThreadPoolExecutor().scheduleAtFixedRate(stopTask, 0,
                  ReplicaController.RC_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        }
      }

    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Unexpected Error: New Actives Accepted. " + e.getMessage());
      e.printStackTrace();
    } catch (RecordNotFoundException e) {
      // this could happen in rare cases when remove request for a name arrives at the same time as a group change
      GNS.getLogger().warning("ERROR:  DECISION: BUT PRIMARY NAME RECORD DELETED Name = " + activeProposalPacket.getName());
    } catch (FailedUpdateException e) {
      GNS.getLogger().severe("Unexpected Error!" + e.getMessage());
      e.printStackTrace();
    }
  }

  /**
   * Old set of active replicas have stopped, and therefore new set of active replicas can be started.
   */
  public static void handleOldActiveStop(OldActiveSetStopPacket oldActiveSetStop, ReplicaController replicaController) {
    if (replicaController.getOngoingStopActiveRequests().remove(oldActiveSetStop.getRequestID()) != null) {
      createStartActiveSetTask(oldActiveSetStop, replicaController);
    } else {
      GNS.getLogger().info("Confirmation is received previously or is excessively delayed. " + oldActiveSetStop);
    }

  }

  /**
   * Old actives have stopped, create a task to start new actives for name.
   */
  private static void createStartActiveSetTask(OldActiveSetStopPacket packet, ReplicaController replicaController) {
    try {
      ReplicaControllerRecord rcRecord = ReplicaControllerRecord.getNameRecordPrimaryMultiField(
              replicaController.getDB(), packet.getName(), startActiveSetFields);
      if (Config.debugMode) {
        GNS.getLogger().info("Old active stopped. write to nameRecord: " + packet.getName());
      }
      if (!rcRecord.isActiveRunning()) {
        if (Config.debugMode) {
          GNS.getLogger().info("OLD Active  stopped. Name: " + rcRecord.getName() + " Old Version: "
                  + packet.getVersion());
        }
        StartActiveSetTask startupTask = new StartActiveSetTask(rcRecord.getName(),
                rcRecord.getOldActiveNameservers(), rcRecord.getActiveNameservers(), rcRecord.getActiveVersion(),
                rcRecord.getOldActiveVersion(), null, replicaController);
        // scheduled
        replicaController.getScheduledThreadPoolExecutor().scheduleAtFixedRate(startupTask, 0,
                ReplicaController.RC_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
//        }
      } else {
        GNS.getLogger().info("IGNORE MSG: GROUP CHANGE ALREADY COMPLETE. " + packet.getVersion());
      }
    } catch (RecordNotFoundException e) {
      GNS.getLogger().severe("Name record not found. This case should not happen. " + e.getMessage());
      e.printStackTrace();
    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field not found exception. " + e.getMessage());
      e.printStackTrace();
    }
  }

  /**
   * ReplicaController has received message from an active that a majority of new actives have been informed.
   * This method proposes a request to update replica controllers that new actives are running, and group change
   * is complete.
   */
  public static void handleNewActiveStartConfirmMessage(NewActiveSetStartupPacket packet, ReplicaController replicaController)
          throws JSONException, IOException {
    if (Config.debugMode) {
      GNS.getLogger().info("NEW_ACTIVE_START: Received confirmation at primary. " + packet.getName());
    }
    Object o = replicaController.getOngoingStartActiveRequests().remove(packet.getUniqueID());
    if (o != null) {
      // inform old actives to delete state
      OldActiveSetStopPacket oldActiveSetStopPacket = new OldActiveSetStopPacket(packet.getName(), 0,
              replicaController.getNodeID(), -1, packet.getOldActiveVersion(), Packet.PacketType.DELETE_OLD_ACTIVE_STATE);
      replicaController.getNioServer().sendToIDs(packet.getOldActiveNameServers(), oldActiveSetStopPacket.toJSONObject());
      GroupChangeCompletePacket proposePacket = new GroupChangeCompletePacket(packet.getNewActiveVersion(), packet.getName());
      replicaController.getNioServer().sendToID(replicaController.getNodeID(), proposePacket.toJSONObject());
      if (Config.debugMode) {
        GNS.getLogger().info(" Propose group change complete message for coordination: " + packet.getName()
                + " Version = " + packet.getNewActiveNameServers());
      }
      // this task is created to check if replica controllers have updated database,
      // if not, the request is re-proposed for coordination.
      WriteActiveNameServersRunningTask task = new WriteActiveNameServersRunningTask(packet.getName(),
              packet.getNewActiveVersion(), replicaController);
      replicaController.getScheduledThreadPoolExecutor().scheduleAtFixedRate(task, ReplicaController.RC_TIMEOUT_MILLIS,
              ReplicaController.RC_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    } else {
      GNS.getLogger().info("Confirmation is received previously or is excessively delayed. " + packet);
    }
  }

  /**
   * Updates <code>ReplicaControllerRecord</code> database to indicate group change has completed.
   * Executes the result of update proposed by <code>handleNewActiveStartConfirmMessage</code>.
   */
  public static void executeActiveNameServersRunning(GroupChangeCompletePacket packet,
                                                     ReplicaController replicaController, boolean recovery)
          throws JSONException {
    if (Config.debugMode) {
      GNS.getLogger().info("Execute: New active started. write to database: " + packet);
    }
    try {
      ReplicaControllerRecord rcRecord = ReplicaControllerRecord.getNameRecordPrimaryMultiField(
              replicaController.getDB(), packet.getName(), newActiveStartedFields);

      GNS.getLogger().info("Group change complete. name = " + rcRecord.getName() + " Version "
              + rcRecord.getActiveVersion());
      if (rcRecord.setNewActiveRunning(packet.getVersion())) {
        if (Config.debugMode) {
          GNS.getLogger().info("New active running. Name: " + packet.getName() + " Version: " + packet.getVersion());
        }
      } else {
        GNS.getLogger().info("IGNORE MSG: NEW Active Version NOT FOUND while setting "
                + "it to inactive. Already received msg before. Version = " + packet.getVersion());
      }

    } catch (RecordNotFoundException e) {
      GNS.getLogger().severe("Record does not exist !! Should not happen. " + packet.getName());
      e.printStackTrace();
    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field not found exception. " + e.getMessage() + "\tName\t" + packet.getName());
      e.printStackTrace();
    } catch (FailedUpdateException e) {
      GNS.getLogger().severe("Failed update exception. " + e.getMessage() + "\tName\t" + packet.getName());
      e.printStackTrace();
    }

  }

}
