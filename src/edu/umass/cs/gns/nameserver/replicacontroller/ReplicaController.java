package edu.umass.cs.gns.nameserver.replicacontroller;

import edu.umass.cs.gns.database.BasicRecordCursor;
import edu.umass.cs.gns.database.ColumnField;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.packet.*;
import edu.umass.cs.gns.packet.Packet.PacketType;
import edu.umass.cs.gns.packet.paxospacket.FailureDetectionPacket;
import edu.umass.cs.gns.packet.paxospacket.PaxosPacketType;
import edu.umass.cs.gns.packet.paxospacket.RequestPacket;
import edu.umass.cs.gns.test.FailureScenario;
import edu.umass.cs.gns.util.ConsistentHashing;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class implements three operations of a replica controller that are interlinked to one another:
 * changing the set of active replicas for a name, removing a name from GNS, and handling failure of a name server.
 *
 * TODO class needs lot more documentation.
 */
public class ReplicaController {

  public static ConcurrentHashMap<String, Long> groupChangeStartTimes = new ConcurrentHashMap<String, Long>();
  /**
   * Timeout after which replica controller retries sending request to actives for (1) starting new actives
   * (2) stopping old actives. (3) re-sends proposal to write new active running.
   */
  public static int RC_TIMEOUT_MILLIS = 5000;
  /**
   * Set of RemoveRecordPacket that this node has currently received and is removing records for.
   */
  private static ConcurrentHashMap<String, RemoveRecordPacket> removeRecordRequests = new ConcurrentHashMap<String, RemoveRecordPacket>();

  /******** START: Methods that return paxosID among actives and among primaries for a name ******************/
  /**
   * Returns true if the given paxosID belongs to that between primary name servers for a name.
   *
   * If the paxosID string ends with '-P' substring, it belongs to paxos between primaries.
   * @param paxosID
   * @return
   */
  public static boolean isPrimaryPaxosID(String paxosID) {
    if (paxosID == null) {
      GNS.getLogger().severe("Error Exception: PaxosID is null. String = " + paxosID);
      return false;
    }
    if (paxosID.endsWith("-P")) {
      return true;
    }
    return false;
  }

  /**
   * Return ID of the paxos instance among primary name servers for this record.
   * This method selects using consistent hashing the set of primary replicas for a name, and returns the paxos
   * instance corresponding to that set of primary replicas.
   *
   * @param rcRecord
   */
  public static String getPrimaryPaxosID(ReplicaControllerRecord rcRecord) throws FieldNotFoundException {
    return getPrimaryPaxosID(rcRecord.getName());
  }

  /**
   * Return ID of the paxos instance among primary name servers for this name.
   * This method selects using consistent hashing the set of primary replicas for a name, and returns the paxos
   * instance corresponding to that set of primary replicas.
   *
   * @param name
   */
  public static String getPrimaryPaxosID(String name) {
    return getPaxosIDForReplicaControllerGroup(ConsistentHashing.getReplicaControllerGroupID(name));
  }

  public static String getPaxosIDForReplicaControllerGroup(String groupID) {
    return groupID + "-P";
  }

  /**
   * Return a new randomly generated ID of the paxos instance among active name servers of this record.
   * PaxosID is of format: name-randomint
   *
   * @param nameRecord
   */
  public static String getActivePaxosID(ReplicaControllerRecord nameRecord) throws FieldNotFoundException {
    return getActivePaxosID(nameRecord.getName());
  }

  /**
   * Return a new randomly generated ID of the paxos instance among active name servers of this name.
   * PaxosID is of format: name-randomint
   * @param name
   * @return
   */
  public static String getActivePaxosID(String name) {
    Random r = new Random();
    return name + "-" + r.nextInt(100000000);
  }

  /**
   * Reverse lookup for name given the paxosID between actives.
   * @param activePaxosID
   * @return
   */
  public static String getNameFromActivePaxosID(String activePaxosID) {
    int index = activePaxosID.lastIndexOf("-");
    if (index == -1) {
      GNS.getLogger().severe("Exception ERROR: Improperly formatted active paxos ID: " + activePaxosID);
      return activePaxosID;
    }
    return activePaxosID.substring(0, index);
  }

  /*****END: Methods that return paxosID among actives and among primaries for a name*************/
  /************START: Public/private methods related to changes in set of active replicas*******************/
  public static void handleIncomingPacket(JSONObject json) {
    try {
      switch (Packet.getPacketType(json)) {
        case OLD_ACTIVE_STOP_CONFIRM_TO_PRIMARY:
          OldActiveSetStopPacket oldPacket = new OldActiveSetStopPacket(json);
          handleOldActivesStopConfirmMessage(oldPacket);
          break;
        case NEW_ACTIVE_START_CONFIRM_TO_PRIMARY:
          NewActiveSetStartupPacket packet = new NewActiveSetStartupPacket(json);
          handleNewActiveStartConfirmMessage(packet);
          break;
        default:
          break;
      }
    } catch (JSONException e) {
      if (StartNameServer.debugMode) {
        GNS.getLogger().info("JSON Exception here.");
      }
      e.printStackTrace();
    } catch (Exception e) {
      if (StartNameServer.debugMode) {
        GNS.getLogger().info("Exception in Replication controller .... ." + e.getMessage());
      }
      e.printStackTrace();
    }
  }
  private static ArrayList<ColumnField> oldActiveStopConfirmFields = new ArrayList<ColumnField>();

  private static ArrayList<ColumnField> getOldActiveStopConfirmFields() {
    synchronized (oldActiveStopConfirmFields) {
      if (oldActiveStopConfirmFields.size() > 0) {
        return oldActiveStopConfirmFields;
      }
      oldActiveStopConfirmFields.add(ReplicaControllerRecord.MARKED_FOR_REMOVAL);
      oldActiveStopConfirmFields.add(ReplicaControllerRecord.OLD_ACTIVE_PAXOS_ID);
      oldActiveStopConfirmFields.add(ReplicaControllerRecord.ACTIVE_PAXOS_ID);
      oldActiveStopConfirmFields.add(ReplicaControllerRecord.ACTIVE_NAMESERVERS_RUNNING);
      return oldActiveStopConfirmFields;
    }
  }

  /**
   * TODO update this doc
   * Primary has received confirmation from an active that the old set of actives have stopped.
   * This method proposes a request to primary-paxos to write this to the database. Once the write is complete,
   * the new set of actives can be started.
   *
   * In case the name is to be removed, this message indicates that paxos between actives is stopped, and that
   * primary-paxos can now be stopped.
   * @param packet
   */
  public static void handleOldActivesStopConfirmMessage(OldActiveSetStopPacket packet) {
    if (StartNameServer.experimentMode) {
      StartNameServer.checkFailure(FailureScenario.handleOldActivesStopConfirmMessage);
    }

    // write to name record object using Primary-paxos that oldActive is stopped
    //
    // schedule new active startup event: StartupReplicaSetTask

    if (StartNameServer.debugMode) {
      GNS.getLogger().info("OLD ACTIVE STOP: Primary recvd confirmation. Name  = "
              + packet.getName());
    }

    String paxosID = getPrimaryPaxosID(packet.getName());
    Long groupChangeStartTime = groupChangeStartTimes.get(packet.getName());
    if (groupChangeStartTime != null) {
      long groupChangeDuration = System.currentTimeMillis() - groupChangeStartTime;
      if (StartNameServer.experimentMode) {
        GNS.getLogger().severe("\tOldActiveStopDuration\t" + packet.getName()
                + "\t" + groupChangeDuration + "\t");
      }
    }

    ReplicaControllerRecord rcRecord;
    try {
      rcRecord = ReplicaControllerRecord.getNameRecordPrimaryMultiField(NameServer.replicaController, packet.getName(),
              getOldActiveStopConfirmFields());
      GNS.getLogger().info("Record read is " + rcRecord);
    } catch (RecordNotFoundException e) {
      GNS.getLogger().severe("Exception: name record should exist in DB. Error. " + e.getMessage());
      e.printStackTrace();
      return;
    }

    try {
      GNS.getLogger().info("Marked for removal: " + rcRecord.isMarkedForRemoval());
      GNS.getLogger().info("Active running: " + rcRecord.isActiveRunning());

      if (rcRecord.isMarkedForRemoval() && rcRecord.isActiveRunning()) {
        GroupChangeProgress.groupChangeComplete(rcRecord.getName()); // IMPORTANT : this ensures that
//        // StopPaxosID task will see the paxos instance as completed.

//        rcRecord.setOldActiveStopped(packet.getPaxosIDToBeStopped()); // IMPORTANT : this ensures that
//        // StopPaxosID task will see the paxos instance as completed.

        NameServer.paxosManager.propose(paxosID, new RequestPacket(PacketType.PRIMARY_PAXOS_STOP.getInt(), packet.toString(),
                PaxosPacketType.REQUEST, false));
        if (StartNameServer.debugMode) {
          GNS.getLogger().info("PAXOS PROPOSAL PROPOSED STOP COMMAND because name record is "
                  + "marked for removal: " + packet.toString());
        }
      } else {
        // Write locally that name record is stopped and send message to start new actives ...
        createStartActiveSetTask(packet);
      }

    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field not found exception. " + e.getMessage());
      e.printStackTrace();
    }
  }
  private static ArrayList<ColumnField> oldActiveStoppedFields = new ArrayList<ColumnField>();

  private static ArrayList<ColumnField> getGetOldActiveStoppedFields() {
    synchronized (oldActiveStoppedFields) {
      if (oldActiveStoppedFields.size() > 0) {
        return oldActiveStoppedFields;
      }
      oldActiveStoppedFields.add(ReplicaControllerRecord.OLD_ACTIVE_PAXOS_ID);
      oldActiveStoppedFields.add(ReplicaControllerRecord.ACTIVE_PAXOS_ID);
      oldActiveStoppedFields.add(ReplicaControllerRecord.OLD_ACTIVE_NAMESERVERS);
      oldActiveStoppedFields.add(ReplicaControllerRecord.ACTIVE_NAMESERVERS_RUNNING);
      oldActiveStoppedFields.add(ReplicaControllerRecord.ACTIVE_NAMESERVERS);
      oldActiveStoppedFields.add(ReplicaControllerRecord.PRIMARY_NAMESERVERS);
      return oldActiveStoppedFields;
    }
  }

  /**
   * Old actives have stopped, create a task to start new actives for name.
   * Also update, {@code groupChangeProgress} data structure.
   * @param packet
   */
  private static void createStartActiveSetTask(OldActiveSetStopPacket packet) {

    try {
      ReplicaControllerRecord rcRecord = ReplicaControllerRecord.getNameRecordPrimaryMultiField(
              NameServer.replicaController, packet.getName(), getGetOldActiveStoppedFields());

      if (StartNameServer.debugMode) {
        GNS.getLogger().info("Primary send: old active stopped. write to nameRecord: " + packet.getName());
      }

      if (rcRecord.isActiveRunning() == false) {
        boolean result = GroupChangeProgress.updateGroupChangeProgress(rcRecord.getName(), GroupChangeProgress.OLD_ACTIVE_STOP);
        if (result) {
          //      if (rcRecord.setOldActiveStopped(packet.getPaxosIDToBeStopped())) {
          if (StartNameServer.debugMode) {
            GNS.getLogger().info("OLD Active paxos stopped. Name: " + rcRecord.getName()
                    + " Old Paxos ID: " + packet.getPaxosIDToBeStopped());
          }
          //        if (isSmallestNodeRunning(rcRecord.getName(), rcRecord.getPrimaryNameservers())) {
          StartActiveSetTask startupTask = new StartActiveSetTask(rcRecord.getName(),
                  rcRecord.getOldActiveNameservers(), rcRecord.getActiveNameservers(), rcRecord.getActivePaxosID(),
                  rcRecord.getOldActivePaxosID(), null);
          // scheduled
          NameServer.timer.schedule(startupTask, 0, RC_TIMEOUT_MILLIS);
        } else {
          GNS.getLogger().info("IGNORE MSG: ALREADY RECEIVED OLD ACTIVE STOP FOR GROUP CHANGE. " + packet.getPaxosIDToBeStopped());
        }
//        }
      } else {
        GNS.getLogger().info("IGNORE MSG: GROUP CHANGE ALREADY COMPLETE. " + packet.getPaxosIDToBeStopped());
      }
    } catch (RecordNotFoundException e) {
      GNS.getLogger().severe("Name record not found. This case should not happen. " + e.getMessage());
      e.printStackTrace();
      return;
    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field not found exception. " + e.getMessage());
      e.printStackTrace();
    }

  }

  /**
   * Primary has received message from an active that the paxos instance between new actives has started.
   * This method proposes a request to paxos among replica controllers to update that new actives are running.
   * @param packet
   */
  private static void handleNewActiveStartConfirmMessage(NewActiveSetStartupPacket packet) {
    if (StartNameServer.experimentMode) {
      StartNameServer.checkFailure(FailureScenario.handleNewActiveStartConfirmMessage);
    }
    if (StartNameServer.debugMode) {
      GNS.getLogger().info("NEW_ACTIVE_START: Received confirmation at primary. " + packet.getName());
    }

    Long groupChangeStartTime = groupChangeStartTimes.remove(packet.getName());
    if (groupChangeStartTime != null) {
      long groupChangeDuration = System.currentTimeMillis() - groupChangeStartTime;
      if (StartNameServer.experimentMode) {
        GNS.getLogger().severe("\tGroupChangeDuration\t" + packet.getName() + "\t"
                + groupChangeDuration + "\t");
      }
    }
//    ReplicaControllerRecord nameRecordPrimary = NameServer.getNameRecordPrimaryLazy(packet.getName());
    String paxosID = getPrimaryPaxosID(packet.getName());
    boolean result = GroupChangeProgress.updateGroupChangeProgress(packet.getName(), GroupChangeProgress.NEW_ACTIVE_START);
    if (result) {
      ChangeActiveStatusPacket proposePacket = new ChangeActiveStatusPacket(packet.getNewActivePaxosID(),
              packet.getName(), PacketType.NEW_ACTIVE_START_CONFIRM_TO_PRIMARY);

      // write to replica controller record object using Primary-paxos that newActive is running
      NameServer.paxosManager.propose(paxosID, new RequestPacket(PacketType.NEW_ACTIVE_START_CONFIRM_TO_PRIMARY.getInt(),
              proposePacket.toString(), PaxosPacketType.REQUEST, false));
      if (StartNameServer.debugMode) {
        GNS.getLogger().info("PAXOS PROPOSAL: New Active Started for Name: " + packet.getName()
                + " Paxos ID = " + packet.getNewActiveNameServers());
      }
      WriteActiveNameServersRunningTask task = new WriteActiveNameServersRunningTask(packet.getName(),
              packet.getNewActivePaxosID());
      NameServer.timer.schedule(task, RC_TIMEOUT_MILLIS, RC_TIMEOUT_MILLIS);
    }

  }

  /**
   * Executes the result of paxos message proposed by <code>handleNewActiveStartConfirmMessage</code>.
   * Writes to <code>ReplicaControllerRecord</code> that new actives have started.
   * @param decision
   * @throws JSONException
   */
  public static void applyActiveNameServersRunning(String decision) throws JSONException {
    if (StartNameServer.experimentMode) {
      StartNameServer.checkFailure(FailureScenario.applyActiveNameServersRunning);
    }
    if (StartNameServer.debugMode) {
      GNS.getLogger().info("PAXOS DECISION: new active started. write to nameRecord: "
              + decision);
    }

    ChangeActiveStatusPacket packet = new ChangeActiveStatusPacket(new JSONObject(decision));

    try {
      ReplicaControllerRecord rcRecord = ReplicaControllerRecord.getNameRecordPrimaryMultiField(
              NameServer.replicaController, packet.getName(), getNewActiveStartedFields());
      GroupChangeProgress.groupChangeComplete(packet.getName());
      GNS.getLogger().info("Group change complete. name = " + rcRecord.getName() + " PaxosID " + rcRecord.getActivePaxosID());
      if (rcRecord.setNewActiveRunning(packet.getPaxosID())) {
        if (StartNameServer.debugMode) {
          GNS.getLogger().info("New Active paxos running for name : " + packet.getName()
                  + " Paxos ID: " + packet.getPaxosID());
        }
      } else {
        if (StartNameServer.debugMode) {
          GNS.getLogger().info("IGNORE MSG: NEW Active PAXOS ID NOT FOUND while setting "
                  + "it to inactive. Already received msg before. Paxos ID = " + packet.getPaxosID());
        }
      }
      if (rcRecord.isMarkedForRemoval() && removeRecordRequests.containsKey(packet.getName())) {
        GroupChangeProgress.updateGroupChangeProgress(rcRecord.getName(), GroupChangeProgress.GROUP_CHANGE_START);
        StopActiveSetTask stopTask = new StopActiveSetTask(packet.getName(), rcRecord.getActiveNameservers(),
                rcRecord.getActivePaxosID());
        NameServer.timer.schedule(stopTask, 0, RC_TIMEOUT_MILLIS);
      }

    } catch (RecordNotFoundException e) {
      GNS.getLogger().severe("Record does not exist !! Should not happen. " + packet.getName());
      e.printStackTrace();
    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field not found exception. " + e.getMessage() + "\tName\t" + packet.getName());
      e.printStackTrace();
    }

  }

  /************END: Public/private methods related to changes in set of active replicas*******************/
  /************START: Public/private methods related to removing a record from GNS*******************/
  /**
   * Handles a name record remove request from a client. if name exists, then we start deleting the name by
   * proposing to primaries that name record will be deleted.
   * @param json
   * @throws JSONException
   * @throws IOException
   */
  public static void handleNameRecordRemoveRequestAtPrimary(JSONObject json) throws JSONException, IOException {
    // 1. primaries agree to remove record (paxos operation among primaries)
    // 2. stop current actives  (paxos operation among actives)
    // 3. primaries remove record (paxos operation among primaries)
    // 4. send confirmation to client
    RemoveRecordPacket removeRecord = new RemoveRecordPacket(json);
    //ReplicaControllerRecord nameRecordPrimary = NameServer.getNameRecordPrimaryLazy(removeRecord.getName());

    ArrayList<ColumnField> readFields = new ArrayList<ColumnField>();
    readFields.add(ReplicaControllerRecord.MARKED_FOR_REMOVAL);

    try {
      ReplicaControllerRecord rcRecord = ReplicaControllerRecord.getNameRecordPrimaryMultiField(
              NameServer.replicaController, removeRecord.getName(), readFields);
      if (rcRecord.isRemoved()) { // if removed, send confirm to client
        ConfirmUpdateLNSPacket confirmPacket = new ConfirmUpdateLNSPacket(NSResponseCode.NO_ERROR, removeRecord);
        NameServer.tcpTransport.sendToID(removeRecord.getLocalNameServerID(), confirmPacket.toJSONObject());
        if (StartNameServer.debugMode) {
          GNS.getLogger().info("Record already remove. Sent confirmation to client. Name = " + removeRecord.getName());
        }
        return;
      }

      if (rcRecord.isMarkedForRemoval() == true) {
        if (StartNameServer.debugMode) {
          GNS.getLogger().info("Already marked for removal. Name record will be deleted soon. So request is dropped.");
        }
        return;
      }
      // propose this to primary paxos
      String primaryPaxosID = getPrimaryPaxosID(rcRecord);
      NameServer.paxosManager.propose(primaryPaxosID, new RequestPacket(removeRecord.getType().getInt(), removeRecord.toString(),
              PaxosPacketType.REQUEST, false));
      if (StartNameServer.debugMode) {
        GNS.getLogger().info("PAXOS PROPOSAL: Proposed mark for removal in primary paxos. Packet = " + removeRecord);
      }
      removeRecordRequests.put(rcRecord.getName(), removeRecord);

    } catch (RecordNotFoundException e) {
      // return failure, because record was not even found in deleted state
      ConfirmUpdateLNSPacket confirmPacket = new ConfirmUpdateLNSPacket(NSResponseCode.ERROR, removeRecord);
      NameServer.tcpTransport.sendToID(removeRecord.getLocalNameServerID(), confirmPacket.toJSONObject());

      if (StartNameServer.debugMode) {
        GNS.getLogger().info("Record not found. Sent failure confirmation to client. Name = " + removeRecord.getName());
      }
      GNS.getLogger().severe(" REMOVE RECORD ERROR!! Name: " + removeRecord.getName());

    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("ColumnField not found exception. " + e.getMessage());
    }

  }
  private static ArrayList<ColumnField> applyMarkedForRemovalFields = new ArrayList<ColumnField>();

  private static ArrayList<ColumnField> getApplyMarkedForRemovalFields() {
    synchronized (applyMarkedForRemovalFields) {
      if (applyMarkedForRemovalFields.size() > 0) {
        return applyMarkedForRemovalFields;
      }

      applyMarkedForRemovalFields.add(ReplicaControllerRecord.MARKED_FOR_REMOVAL);
//      applyMarkedForRemovalFields.add(ReplicaControllerRecord.OLD_ACTIVE_NAMESERVERS_RUNNING);
      applyMarkedForRemovalFields.add(ReplicaControllerRecord.ACTIVE_NAMESERVERS_RUNNING);
      applyMarkedForRemovalFields.add(ReplicaControllerRecord.ACTIVE_NAMESERVERS);
      applyMarkedForRemovalFields.add(ReplicaControllerRecord.ACTIVE_PAXOS_ID);
      return applyMarkedForRemovalFields;
    }
  }

  /**
   * Primary-paxos has commit the request: mark this name for removal.
   * This method updates the database and notifies current actives to stop their paxos instance.
   *
   * @param value
   * @throws JSONException
   */
  public static void applyMarkedForRemoval(String value) throws JSONException {
    // create a remove record object
    if (StartNameServer.debugMode) {
      GNS.getLogger().info("PAXOS DECISION remove record packet accepted by paxos: " + value);
    }

    RemoveRecordPacket removeRecord = new RemoveRecordPacket(new JSONObject(value));
    // 
    ReplicaControllerRecord rcRecord = null;
    try {
      rcRecord = ReplicaControllerRecord.getNameRecordPrimaryMultiField(NameServer.replicaController,
              removeRecord.getName(), getApplyMarkedForRemovalFields());

    } catch (RecordNotFoundException e) {

      e.printStackTrace();
    }

    try {
      rcRecord.setMarkedForRemoval(); // DB write
      // TODO: if update not applied do not proceed further

      if (StartNameServer.debugMode) {
        GNS.getLogger().info("PAXOS DECISION applied. Name Record marked for removal "
                + rcRecord.getName());
      }

      if (StartNameServer.debugMode) {
        GNS.getLogger().info("Remove record request has keys: "
                + removeRecordRequests.keySet());
      }
      if (removeRecordRequests.containsKey(rcRecord.getName()) == false) {
        if (StartNameServer.debugMode) {
          GNS.getLogger().info("SKIP: remove record request does not not contain "
                  + rcRecord.getName());
        }
        return;
      }

      if (rcRecord.isActiveRunning()) { // if active is running, stop current actives
        GroupChangeProgress.updateGroupChangeProgress(rcRecord.getName(), GroupChangeProgress.GROUP_CHANGE_START);
        StopActiveSetTask stopTask = new StopActiveSetTask(rcRecord.getName(), rcRecord.getActiveNameservers(),
                rcRecord.getActivePaxosID());
        NameServer.timer.schedule(stopTask, 0, RC_TIMEOUT_MILLIS);
      } else {
        // active name servers not running, means group change in progress, let new actives become active
        // then we will stop them.
      }
    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field Not Found Exception. " + e.getMessage());

      e.printStackTrace();

    }

  }
  private static ArrayList<ColumnField> newActiveStartedFields = new ArrayList<ColumnField>();

  private static ArrayList<ColumnField> getNewActiveStartedFields() {
    synchronized (newActiveStartedFields) {
      if (newActiveStartedFields.size() > 0) {
        return newActiveStartedFields;
      }
      newActiveStartedFields.add(ReplicaControllerRecord.MARKED_FOR_REMOVAL);
      newActiveStartedFields.add(ReplicaControllerRecord.ACTIVE_PAXOS_ID);
      newActiveStartedFields.add(ReplicaControllerRecord.ACTIVE_NAMESERVERS);
      newActiveStartedFields.add(ReplicaControllerRecord.ACTIVE_NAMESERVERS_RUNNING);
      return newActiveStartedFields;
    }
  }

  /**
   * Executes the result of paxos message proposed by <code>handleOldActivesStopConfirmMessage</code>.
   * Apply the decision to stop paxos between primaries. This is done to delete name record from GNS.
   * TODO update this doc
   * @param value
   * @throws JSONException
   * @throws IOException
   */
  public static void applyStopPrimaryPaxos(String value) throws JSONException, IOException {
    // TODO update documentation to say that we do not remove paxos among primaries anymore
    if (StartNameServer.debugMode) {
      GNS.getLogger().info("PAXOS DECISION stop primary paxos decision received.");
    }
    OldActiveSetStopPacket packet = new OldActiveSetStopPacket(new JSONObject(value));
    String paxosID = getPrimaryPaxosID(packet.getName());
//		String name = packet.getName();
    RemoveRecordPacket removeRecordPacket = removeRecordRequests.remove(packet.getName());
    NameServer.replicaController.removeNameRecord(packet.getName());

    if (StartNameServer.debugMode) {
      GNS.getLogger().info("RECORD MARKED AS REMOVED IN REPLICA CONTROLLER DB");
    }

    if (removeRecordPacket != null) {
      ConfirmUpdateLNSPacket confirmPacket = new ConfirmUpdateLNSPacket(NSResponseCode.NO_ERROR, removeRecordPacket);
      NameServer.returnToSender(confirmPacket.toJSONObject(), removeRecordPacket.getLocalNameServerID());
      if (StartNameServer.debugMode) {
        GNS.getLogger().info("REMOVE RECORD SENT RESPONSE TO LNS");
      }
    }
  }

  /************END: Public/private methods related to removing a record from GNS*******************/
  /************START: Public/private methods related to handling failure of a name server node*******************/
  public static void handleNodeFailure(FailureDetectionPacket fdPacket) {
    if (fdPacket.status == true) {
      return; // node was down and it came up, don't worry about that
    }
    int failedNode = fdPacket.responderNodeID;
    GNS.getLogger().info(" Failed Node Detected: replication controller working. " + failedNode);

//		if (node fails then what happens)
    BasicRecordCursor iterator = NameServer.replicaController.getAllRowsIterator();
//    if (StartNameServer.debugMode) GNS.getLogger().info("Got iterator : " + replicationRound);

    while (iterator.hasNext()) {
      ReplicaControllerRecord record;
      try {
        JSONObject jsonObject = iterator.next();
        record = new ReplicaControllerRecord(NameServer.replicaController, jsonObject);
      } catch (Exception e) {
        GNS.getLogger().severe("Problem creating ReplicaControllerRecord from JSON" + e);
        continue;
      }

      // if both this node & failed node are primaries.
      try {
        if (record.containsPrimaryNameserver(NameServer.nodeID)
                && record.containsPrimaryNameserver(failedNode)
                && record.isRemoved() == false) {
          if (StartNameServer.debugMode) {
            GNS.getLogger().info(" Handing Failure for Name: " + record.getName() + " NAME RECORD: " + record);
          }
          handlePrimaryFailureForNameRecord(record, failedNode);
        }
      } catch (FieldNotFoundException e) {
        GNS.getLogger().severe("Field not found exception. This should not happen because we read complete record. "
                + e.getMessage());
        e.printStackTrace();
      }
    }
  }

  private static void handlePrimaryFailureForNameRecord(ReplicaControllerRecord rcRecord, int failedNode) throws
          FieldNotFoundException {
    if (StartNameServer.debugMode) {
      GNS.getLogger().info("Handling node failure for name record: " + rcRecord.getName() + " Failed Node: "
              + failedNode + "\tActive running\t" + rcRecord.isActiveRunning() + " Remove\t"
              + rcRecord.isMarkedForRemoval());
    }

    if (isSmallestNodeRunning(rcRecord.getName(), rcRecord.getPrimaryNameservers()) == false) {
      return; // am I the smallest primary alive, otherwise don't do anything.
    }

    // 4 cases arise
    if (rcRecord.isActiveRunning() && rcRecord.isMarkedForRemoval() == false) { // nothing to do
      return;
    } else if (rcRecord.isActiveRunning() == false && rcRecord.isMarkedForRemoval() == false) { // group change ongoing
      if (GroupChangeProgress.groupChangeProgress.containsKey(rcRecord.getName()) == false) { // I am not doing it
        GroupChangeProgress.updateGroupChangeProgress(rcRecord.getName(), GroupChangeProgress.GROUP_CHANGE_START); // start to do
        StopActiveSetTask stopTask = new StopActiveSetTask(rcRecord.getName(), rcRecord.getOldActiveNameservers(),
                rcRecord.getOldActivePaxosID());
        NameServer.timer.schedule(stopTask, 0, RC_TIMEOUT_MILLIS);
      }
    } else if (rcRecord.isActiveRunning() && rcRecord.isMarkedForRemoval()) { // removal in progress
      if (removeRecordRequests.containsKey(rcRecord.getName()) == false) { // I am not doing it
        removeRecordRequests.put(rcRecord.getName(), new RemoveRecordPacket(new Random().nextInt(), rcRecord.getName(),
                -1));
        // complete removing record
        GroupChangeProgress.updateGroupChangeProgress(rcRecord.getName(), GroupChangeProgress.GROUP_CHANGE_START);
        StopActiveSetTask stopTask = new StopActiveSetTask(rcRecord.getName(), rcRecord.getActiveNameservers(),
                rcRecord.getActivePaxosID());
        NameServer.timer.schedule(stopTask, 0, RC_TIMEOUT_MILLIS);
      }
    } else if (rcRecord.isActiveRunning() == false && rcRecord.isMarkedForRemoval()) {
      // both group change and remove record in progress
      if (removeRecordRequests.containsKey(rcRecord.getName()) == false) {
        // not doing remove record? enter
        removeRecordRequests.put(rcRecord.getName(), new RemoveRecordPacket(new Random().nextInt(), rcRecord.getName(),
                -1));
      }
      if (GroupChangeProgress.groupChangeProgress.containsKey(rcRecord.getName()) == false) {
        // start to do group change?
        GroupChangeProgress.updateGroupChangeProgress(rcRecord.getName(), GroupChangeProgress.GROUP_CHANGE_START);
        StopActiveSetTask stopTask = new StopActiveSetTask(rcRecord.getName(), rcRecord.getOldActiveNameservers(),
                rcRecord.getOldActivePaxosID());
        NameServer.timer.schedule(stopTask, 0, RC_TIMEOUT_MILLIS);
      }
    }
  }

  /************END: Public/private methods related to handling failure of a name server node*******************/
  public static boolean isSmallestNodeRunning(String name, Set<Integer> nameServers) {
    Random r = new Random(name.hashCode());
    ArrayList<Integer> x1 = new ArrayList<Integer>(nameServers);
    Collections.sort(x1);
    Collections.shuffle(x1, r);
    for (int x : x1) {
      if (NameServer.paxosManager.isNodeUp(x)) {
        return x == NameServer.nodeID;
      }
    }
    return false;

  }
}
