package edu.umass.cs.gns.nameserver.replicacontroller;


import edu.umass.cs.gns.database.BasicRecordCursor;
import edu.umass.cs.gns.database.ColumnField;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.ListenerReplicationPaxos;
import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.nameserver.ValuesMap;
import edu.umass.cs.gns.packet.*;
import edu.umass.cs.gns.packet.Packet.PacketType;
import edu.umass.cs.gns.packet.paxospacket.FailureDetectionPacket;
import edu.umass.cs.gns.packet.paxospacket.PaxosPacketType;
import edu.umass.cs.gns.packet.paxospacket.RequestPacket;
import edu.umass.cs.gns.paxos.PaxosManager;
import edu.umass.cs.gns.test.FailureScenario;
import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.gns.util.HashFunction;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ReplicaController {

  public static ConcurrentHashMap<String,Long> groupChangeStartTimes = new ConcurrentHashMap<String, Long>();

  /**
   * Timeout after which replica controller retries sending request to actives for (1) starting new actives
   * (2) stopping old actives. (3) re-sends proposal to write new active running.
   */
  public static int RC_TIMEOUT_MILLIS = 5000;

  /**
   * Set of RemoveRecordPacket that this node has currently received and is removing records for.
   */
  private static ConcurrentHashMap<String, RemoveRecordPacket> removeRecordRequests = new ConcurrentHashMap<String, RemoveRecordPacket>();

  public static ConcurrentHashMap<String, Integer> groupChangeProgress = new ConcurrentHashMap<String, Integer>();

  public static final int STOP_SENT = 1;

  public static final int OLD_ACTIVE_STOP = 2;

  public static final int NEW_ACTIVE_START = 3;


  public static boolean updateGroupChangeProgress(String name, int status) {
    synchronized (groupChangeProgress) {
      if (groupChangeProgress.containsKey(name) == false && status > STOP_SENT) {
        return false;
      }
      if (groupChangeProgress.containsKey(name) == false) {
        groupChangeProgress.put(name, status);
        return true;
      }
      if (groupChangeProgress.get(name) >= status) {
        return false;
      }
      groupChangeProgress.put(name, status);
      return true;
    }
  }


  public static void groupChangeComplete(String name) {
    synchronized (groupChangeProgress) {
      groupChangeProgress.remove(name);
    }
  }


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
    if (paxosID.endsWith("-P")) return true;
    return  false;
  }

  /**
   * Return ID of the paxos instance among primary name servers for the record.
   *
   * @param nameRecord
   */
  public static String getPrimaryPaxosID(ReplicaControllerRecord nameRecord) throws FieldNotFoundException
  {
    return getPrimaryPaxosID(nameRecord.getName());
  }

  /**
   * Returns ID of the paxos instance between primaries for this name.
   * @param name
   * @return
   */
  public static String getPrimaryPaxosID(String name) {
    return  name + "-P";
  }

  /**
   * Reverse lookup for name given the ID of the paxos instance between primaries.
   * @param primaryPaxosID
   * @return
   */
  public static String getNameFromPrimaryPaxosID(String primaryPaxosID) {

    if (primaryPaxosID.endsWith("-P")) {
      return primaryPaxosID.substring(0, primaryPaxosID.length() - "-P".length());
    }

    GNS.getLogger().severe("Error Exception: String is not a valid primaryPaxosID. String = " + primaryPaxosID);
    return  null;
  }

  /**
   * Return ID of the paxos instance among active name servers of this record.
   *
   * @param nameRecord
   */
  public static String getActivePaxosID(ReplicaControllerRecord nameRecord) throws FieldNotFoundException
  {
    return getActivePaxosID(nameRecord.getName());
  }

  public static String getActivePaxosID(String name)
  {
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


  /**
   * When name record is added this method (1) creates paxos instance between primaries
   * (2) NameRecord in DB (3) paxos instance between actives.
   * @param recordEntry
   * @param valuesMap
   * @throws FieldNotFoundException
   */
  public static void handleNameRecordAddAtPrimary(ReplicaControllerRecord recordEntry, ValuesMap valuesMap,
                                                  long initScoutDelay, int ttl) throws FieldNotFoundException{
//        if (StartNameServer.debugMode) GNS.getLogger().info(recordEntry.getName() +
//                "\tBefore Paxos instance created for name: " + recordEntry.getName()
//                        + " Primaries: " + primaries);
//    long initScoutDelay = 0;
//    if (StartNameServer.paxosStartMinDelaySec > 0 && StartNameServer.paxosStartMaxDelaySec > 0) {
//      initScoutDelay = StartNameServer.paxosStartMaxDelaySec*1000 +
// new Random().nextInt(StartNameServer.paxosStartMaxDelaySec*1000 - StartNameServer.paxosStartMinDelaySec*1000);
//    }
    PaxosManager.createPaxosInstance(getPrimaryPaxosID(recordEntry), recordEntry.getPrimaryNameservers(),
            recordEntry.toString(), initScoutDelay);
    if (StartNameServer.debugMode) GNS.getLogger().info(" Primary-paxos created: Name = " + recordEntry.getName());

    //		if (StartNameServer.debugMode) GNS.getLogger().info(recordEntry.getName()  +

    ListenerReplicationPaxos.addNameRecordLocal(recordEntry.getName(), recordEntry.getActiveNameservers(),
            recordEntry.getActivePaxosID(), valuesMap, initScoutDelay, ttl);

    if (StartNameServer.debugMode) GNS.getLogger().info(" Active-paxos and name record created. Name = " +
            recordEntry.getName());
//				"\tPaxos instance created for name: " + recordEntry.getName()
//						+ " Primaries: " + primaries);
//    if (startActives) {
//      startupNewActives(recordEntry, valuesMap);
//      if (StartNameServer.debugMode) GNS.getLogger().info(recordEntry.getName() + "\t" +
//              "Startup new actives: " + recordEntry.getName());
//    }
//    else {
//      if (StartNameServer.debugMode) GNS.getLogger().info(recordEntry.getName() + "\t" +
//              "NOT starting new actives. I did not receive client request to add name record: " + recordEntry.getName());
//    }
  }


  /**
   * Handles a name record remove request from a client. if name exists, then we start deleting the name by
   * proposing to primaries that name record will be deleted.
   * @param json
   * @throws JSONException
   * @throws IOException
   */
  public static void handleNameRecordRemoveRequestAtPrimary(JSONObject json) throws JSONException, IOException {
    // 1. stop current actives
    // 2. stop current primaries
    // 3. send confirmation to client.
    RemoveRecordPacket removeRecord = new RemoveRecordPacket(json);
    //ReplicaControllerRecord nameRecordPrimary = NameServer.getNameRecordPrimaryLazy(removeRecord.getName());

    ArrayList<ColumnField> readFields = new ArrayList<ColumnField>();
    readFields.add(ReplicaControllerRecord.MARKED_FOR_REMOVAL);

    try {
      ReplicaControllerRecord rcRecord = NameServer.getNameRecordPrimaryMultiField(removeRecord.getName(),
              readFields);
      if (rcRecord.isRemoved()) { // if removed, send confirm to client
        ConfirmUpdateLNSPacket confirmPacket = new ConfirmUpdateLNSPacket(true, removeRecord);
        NameServer.tcpTransport.sendToID(removeRecord.getLocalNameServerID(),confirmPacket.toJSONObject());
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
      PaxosManager.propose(primaryPaxosID, new RequestPacket(removeRecord.getType().getInt(), removeRecord.toString(),
              PaxosPacketType.REQUEST, false));
      if (StartNameServer.debugMode) {
        GNS.getLogger().info("PAXOS PROPOSAL: Proposed mark for removal in primary paxos. Packet = " + removeRecord);
      }
      removeRecordRequests.put(getPrimaryPaxosID(rcRecord), removeRecord);

    } catch (RecordNotFoundException e) {
      // return failure, because record was not even found in deleted state
      ConfirmUpdateLNSPacket confirmPacket = new ConfirmUpdateLNSPacket(false, removeRecord);
      NameServer.tcpTransport.sendToID(removeRecord.getLocalNameServerID(),confirmPacket.toJSONObject());

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
      if (applyMarkedForRemovalFields.size() > 0) return applyMarkedForRemovalFields;

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
      rcRecord = NameServer.getNameRecordPrimaryMultiField(removeRecord.getName(), getApplyMarkedForRemovalFields());

    } catch (RecordNotFoundException e) {

      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

    try {
      rcRecord.setMarkedForRemoval(); // DB write
      // TODO: if update not applied do not proceed further

      if (StartNameServer.debugMode) GNS.getLogger().info("PAXOS DECISION applied. Name Record marked for removal " +
              rcRecord.getName());

      if (StartNameServer.debugMode) GNS.getLogger().info("Remove record request has keys: " +
              removeRecordRequests.keySet());
      if (removeRecordRequests.containsKey(getPrimaryPaxosID(rcRecord.getName())) == false) {
        if (StartNameServer.debugMode) GNS.getLogger().info("SKIP: remove record request does not not contain " +
                rcRecord.getName());
        return;
      }

      if (rcRecord.isActiveRunning()) { // if active is running, stop current actives
        updateGroupChangeProgress(rcRecord.getName(), STOP_SENT);
        StopActiveSetTask stopTask = new StopActiveSetTask(rcRecord.getName(), rcRecord.getActiveNameservers(),
                rcRecord.getActivePaxosID());
        NameServer.timer.schedule(stopTask, 0, RC_TIMEOUT_MILLIS);
      } else {
        // active name servers not running, means group change in progress, let new actives become active
        // then we will stop them.
      }
//      switch (stage) {
//        case ACTIVE_RUNNING:
////          ReplicaControllerRecord rcRecord = new ReplicaControllerRecord(removeRecord.getName());
//
//
//          break;
//        default:
//          break;
//      }
//		else if (stage == 2) { // old active stopped, new active yet to run
//			// when new active runs, send message to stop it.
//
//		}
//		else if (stage == 1) { // old active not yet stopped
//			// wait for old active to stop. that process must be in progress.
//		}
    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field Not Found Exception. " + e.getMessage());

      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.

    }

  }


//  public static void startupNewActives(ReplicaControllerRecord nameRecord, ValuesMap initialValue)
// throws FieldNotFoundException{
//    // this method will schedule a timer task to startup active replicas.
//    StartActiveSetTask startupTask = new StartActiveSetTask(
//            nameRecord.getName(),
//            nameRecord.getOldActiveNameservers(),
//            nameRecord.getActiveNameservers(),
//            nameRecord.getActivePaxosID(), nameRecord.getOldActivePaxosID(), initialValue);
//    // scheduled
//    NameServer.timer.schedule(startupTask, 0, RC_TIMEOUT_MILLIS);
//  }

//  /**
//   * Create a task to stop old actives from this name record.
//   *
//   * @param nameRecord
//   */
//  public static void stopOldActives(ReplicaControllerRecord nameRecord) throws FieldNotFoundException{
//    // this method will schedule a timer task to startup active replicas.
//    StopActiveSetTask task = new StopActiveSetTask(nameRecord.getName(),
//            // nameRecord.getRecordKey(),
//            nameRecord.getOldActiveNameservers(),
//            nameRecord.getOldActivePaxosID());
//    NameServer.timer.schedule(task, 0, RC_TIMEOUT_MILLIS);
//  }



  private static ArrayList<ColumnField> newActiveStartedFields = new ArrayList<ColumnField>();

  private static ArrayList<ColumnField> getNewActiveStartedFields() {
    synchronized (newActiveStartedFields) {
      if (newActiveStartedFields.size() > 0) return newActiveStartedFields;
      newActiveStartedFields.add(ReplicaControllerRecord.MARKED_FOR_REMOVAL);
      newActiveStartedFields.add(ReplicaControllerRecord.ACTIVE_PAXOS_ID);
      newActiveStartedFields.add(ReplicaControllerRecord.ACTIVE_NAMESERVERS);
      newActiveStartedFields.add(ReplicaControllerRecord.ACTIVE_NAMESERVERS_RUNNING);
      return newActiveStartedFields;
    }
  }


  private static ArrayList<ColumnField> oldActiveStopConfirmFields = new ArrayList<ColumnField>();

  private static ArrayList<ColumnField> getOldActiveStopConfirmFields() {
    synchronized (oldActiveStopConfirmFields) {
      if (oldActiveStopConfirmFields.size() > 0) return oldActiveStopConfirmFields;
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
    if (StartNameServer.experimentMode) StartNameServer.checkFailure(FailureScenario.handleOldActivesStopConfirmMessage);

    // write to name record object using Primary-paxos that oldActive is stopped
    //
    // schedule new active startup event: StartupReplicaSetTask

    if (StartNameServer.debugMode) GNS.getLogger().info("OLD ACTIVE STOP: Primary recvd confirmation. Name  = " +
            packet.getName());

    String paxosID = getPrimaryPaxosID(packet.getName());
    Long groupChangeStartTime = groupChangeStartTimes.get(packet.getName());
    if (groupChangeStartTime != null) {
      long groupChangeDuration = System.currentTimeMillis()  - groupChangeStartTime;
      if (StartNameServer.experimentMode) GNS.getLogger().severe("\tOldActiveStopDuration\t" + packet.getName() +
              "\t" + groupChangeDuration+ "\t");
    }

    ReplicaControllerRecord rcRecord;
    try {
      rcRecord = NameServer.getNameRecordPrimaryMultiField(packet.getName(), getOldActiveStopConfirmFields());
    } catch (RecordNotFoundException e) {
      GNS.getLogger().severe("Exception: name record should exist in DB. Error. " + e.getMessage());
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      return;
    }

    try {

      if (rcRecord.isMarkedForRemoval() && rcRecord.isActiveRunning()) {
        groupChangeComplete(rcRecord.getName()); // IMPORTANT : this ensures that
//        // StopPaxosID task will see the paxos instance as completed.

//        rcRecord.setOldActiveStopped(packet.getPaxosIDToBeStopped()); // IMPORTANT : this ensures that
//        // StopPaxosID task will see the paxos instance as completed.

        PaxosManager.propose(paxosID, new RequestPacket(PacketType.PRIMARY_PAXOS_STOP.getInt(), packet.toString(),
                PaxosPacketType.REQUEST, true));
        if (StartNameServer.debugMode) {
          GNS.getLogger().info("PAXOS PROPOSAL PROPOSED STOP COMMAND because name record is " +
                  "marked for removal: " + packet.toString());
        }
      }
      else {
        // Write locally that name record is stopped and send message to start new actives ...
        createStartActiveSetTask(packet);
      }

//      ChangeActiveStatusPacket proposePacket = new ChangeActiveStatusPacket(packet.getPaxosIDToBeStopped(),
//              packet.getName(), PacketType.OLD_ACTIVE_STOP_CONFIRM_TO_PRIMARY);
//
//      PaxosManager.propose(paxosID, new RequestPacket(PacketType.OLD_ACTIVE_STOP_CONFIRM_TO_PRIMARY.getInt(),
//                      proposePacket.toString(), PaxosPacketType.REQUEST, false));
//
//      if (StartNameServer.debugMode) {
//        GNS.getLogger().info("PAXOS PROPOSAL: Old Active Stopped for  Name: " + packet.getName()
//                + " Old Paxos ID = " + packet.getPaxosIDToBeStopped());
//      }
    }catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field not found exception. " + e.getMessage());
      e.printStackTrace();
    }
  }



  /**
   * Executes the result of paxos message proposed by <code>handleOldActivesStopConfirmMessage</code>.
   * Apply the decision to stop paxos between primaries. This is done to delete name record from GNS.
   * @param value
   * @throws JSONException
   * @throws IOException
   */
  public static void applyStopPrimaryPaxos(String value) throws JSONException, IOException {
    if (StartNameServer.debugMode)  GNS.getLogger().info("PAXOS DECISION stop primary paxos decision received.");
    OldActiveSetStopPacket packet = new OldActiveSetStopPacket(new JSONObject(value));
    String paxosID = getPrimaryPaxosID(packet.getName()//, packet.getRecordKey()
    );
//		String name = packet.getName();
    RemoveRecordPacket removeRecordPacket = removeRecordRequests.remove(paxosID);
    NameServer.replicaController.removeNameRecord(packet.getName());

    if (StartNameServer.debugMode) GNS.getLogger().info("RECORD MARKED AS REMOVED IN REPLICA CONTROLLER DB");

    if (removeRecordPacket != null) {
      ConfirmUpdateLNSPacket confirmPacket = new ConfirmUpdateLNSPacket(true, removeRecordPacket);
      NameServer.tcpTransport.sendToID(removeRecordPacket.getLocalNameServerID(),confirmPacket.toJSONObject());
//      NSListenerUDP.udpTransport.sendPacket(confirmPacket.toJSONObject(),
//              confirmPacket.getLocalNameServerId(), GNS.PortType.LNS_UDP_PORT);
      if (StartNameServer.debugMode) GNS.getLogger().info("REMOVE RECORD SENT RESPONSE TO LNS");
    }
  }



  private static ArrayList<ColumnField> oldActiveStoppedFields = new ArrayList<ColumnField>();

  private static ArrayList<ColumnField> getGetOldActiveStoppedFields() {
    synchronized (oldActiveStoppedFields) {
      if (oldActiveStoppedFields.size() > 0) return oldActiveStoppedFields;
      oldActiveStoppedFields.add(ReplicaControllerRecord.OLD_ACTIVE_PAXOS_ID);
      oldActiveStoppedFields.add(ReplicaControllerRecord.ACTIVE_PAXOS_ID);
      oldActiveStoppedFields.add(ReplicaControllerRecord.OLD_ACTIVE_NAMESERVERS);
      oldActiveStoppedFields.add(ReplicaControllerRecord.ACTIVE_NAMESERVERS_RUNNING);
//      oldActiveStoppedFields.add(ReplicaControllerRecord.OLD_ACTIVE_NAMESERVERS_RUNNING);
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
      ReplicaControllerRecord rcRecord = NameServer.getNameRecordPrimaryMultiField(packet.getName(), getGetOldActiveStoppedFields());

      if (StartNameServer.debugMode)
        GNS.getLogger().info("Primary send: old active stopped. write to nameRecord: "+ packet.getName());

      if (rcRecord.isActiveRunning() == false) {
        boolean result = updateGroupChangeProgress(rcRecord.getName(), OLD_ACTIVE_STOP);
        if (result) {
          //      if (rcRecord.setOldActiveStopped(packet.getPaxosIDToBeStopped())) {
          if (StartNameServer.debugMode) GNS.getLogger().info("OLD Active paxos stopped. Name: "+ rcRecord.getName()
                  + " Old Paxos ID: "+ packet.getPaxosIDToBeStopped());
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
  public static void handleNewActiveStartConfirmMessage(NewActiveSetStartupPacket packet) {
    if (StartNameServer.experimentMode) StartNameServer.checkFailure(FailureScenario.handleNewActiveStartConfirmMessage);
    if (StartNameServer.debugMode) {
      GNS.getLogger().info("NEW_ACTIVE_START: Received confirmation at primary. " + packet.getName());
    }

    Long groupChangeStartTime = groupChangeStartTimes.remove(packet.getName());
    if (groupChangeStartTime != null) {
      long groupChangeDuration = System.currentTimeMillis()  - groupChangeStartTime;
      if (StartNameServer.experimentMode) GNS.getLogger().severe("\tGroupChangeDuration\t" + packet.getName() + "\t" +
              groupChangeDuration+ "\t");
    }
//    ReplicaControllerRecord nameRecordPrimary = NameServer.getNameRecordPrimaryLazy(packet.getName());
    String paxosID = getPrimaryPaxosID(packet.getName());
    boolean result = updateGroupChangeProgress(packet.getName(), NEW_ACTIVE_START);
    if (result) {
      ChangeActiveStatusPacket proposePacket = new ChangeActiveStatusPacket(packet.getNewActivePaxosID(),
              packet.getName(), PacketType.NEW_ACTIVE_START_CONFIRM_TO_PRIMARY);

      // write to replica controller record object using Primary-paxos that newActive is running
      PaxosManager.propose(paxosID, new RequestPacket(PacketType.NEW_ACTIVE_START_CONFIRM_TO_PRIMARY.getInt(),
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
    if (StartNameServer.experimentMode) StartNameServer.checkFailure(FailureScenario.applyActiveNameServersRunning);
    if (StartNameServer.debugMode) GNS.getLogger().info("PAXOS DECISION: new active started. write to nameRecord: " +
            decision);

    ChangeActiveStatusPacket packet = new ChangeActiveStatusPacket( new JSONObject(decision));

    try {
      ReplicaControllerRecord rcRecord = NameServer.getNameRecordPrimaryMultiField(packet.getName(),
              getNewActiveStartedFields());
      groupChangeComplete(packet.getName());
      if (rcRecord.setNewActiveRunning(packet.getPaxosID())) {
        if (StartNameServer.debugMode)  GNS.getLogger().info("New Active paxos running for name : " + packet.getName()
                + " Paxos ID: " + packet.getPaxosID());
      } else {
        if (StartNameServer.debugMode) GNS.getLogger().info("IGNORE MSG: NEW Active PAXOS ID NOT FOUND while setting "
                + "it to inactive. Already received msg before. Paxos ID = " + packet.getPaxosID());
      }
      if (rcRecord.isMarkedForRemoval() == true && removeRecordRequests.containsKey(packet.getName())) {
        updateGroupChangeProgress(rcRecord.getName(), STOP_SENT);
        StopActiveSetTask stopTask = new StopActiveSetTask(packet.getName(), rcRecord.getActiveNameservers(),
                rcRecord.getActivePaxosID());
        NameServer.timer.schedule(stopTask, 0, RC_TIMEOUT_MILLIS);
      }

    } catch (RecordNotFoundException e) {
      GNS.getLogger().severe("Record does not exist !! Should not happen. " + packet.getName());
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field not found exception. " + e.getMessage() + "\tName\t" + packet.getName());
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

  }





//  /**
//   * /// **** we do not use this method any more **** ////
//   * Executes the result of paxos message proposed by <code>handleOldActivesStopConfirmMessage</code>.
//   * Writes to <code>ReplicaControllerRecord</code> that old actives have stopped; we notify new actives to start.
//   * @param decision
//   * @throws JSONException
//   */
//  public static void oldActiveStoppedWriteToNameRecord(String decision)
//          throws JSONException {
//    /// **** we do not use this method any more **** ////
//    ChangeActiveStatusPacket packet = new ChangeActiveStatusPacket(new JSONObject(decision));
//    ArrayList<Field> fields = new ArrayList<Field>();
//
//    ReplicaControllerRecord rcRecord;
//    try {
//      rcRecord = NameServer.getNameRecordPrimaryMultiField(packet.getName(), getGetOldActiveStoppedFields());
//
//      if (StartNameServer.debugMode) GNS.getLogger().info("PAXOS DECISION: old active stopped. write to nameRecord: "+ packet.getName());
//      //
//      if (rcRecord.setOldActiveStopped(packet.getPaxosID())) {
//        if (StartNameServer.debugMode) GNS.getLogger().info("OLD Active paxos stopped. Name: "+ rcRecord.getName()
//                + " Old Paxos ID: "+ packet.getPaxosID());
//        if (isSmallestNodeRunning(rcRecord.getName(), rcRecord.getPrimaryNameservers())) {
//          StartActiveSetTask startupTask = new StartActiveSetTask(
//                  rcRecord.getName(),
//                  rcRecord.getOldActiveNameservers(),
//                  rcRecord.getActiveNameservers(),
//                  rcRecord.getActivePaxosID(), rcRecord.getOldActivePaxosID(), null);
//          // scheduled
//          NameServer.timer.schedule(startupTask, 0, RC_TIMEOUT_MILLIS);
//        }
//      } else {
//        if (StartNameServer.debugMode) GNS.getLogger().info("INGORE MSG: OLD PAXOS ID NOT FOUND IN ReplicaControllerRecord" +
//                " while setting it to inactive: " + packet.getPaxosID());
//      }
//    } catch (RecordNotFoundException e) {
//      GNS.getLogger().severe("Name record not found. This case should not happen. " + e.getMessage());
//      e.printStackTrace();
//      return;
//    } catch (FieldNotFoundException e) {
//      GNS.getLogger().severe("Field not found exception. " + e.getMessage());
//      e.printStackTrace();
//    }
//
//  }


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
        record = new ReplicaControllerRecord(jsonObject);
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
        GNS.getLogger().severe("Field not found exception. This should not happen because we read complete record. " +
                e.getMessage());
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    }
  }


  private static void handlePrimaryFailureForNameRecord(ReplicaControllerRecord rcRecord, int failedNode) throws
          FieldNotFoundException{
    if (StartNameServer.debugMode) {
      GNS.getLogger().info("Handling node failure for name record: " + rcRecord.getName() + " Failed Node: " +
              failedNode + "\tActive running\t" + rcRecord.isActiveRunning()  + " Remove\t" +
              rcRecord.isMarkedForRemoval());
    }

    if (isSmallestNodeRunning(rcRecord.getName(),rcRecord.getPrimaryNameservers()) == false) {
      return; // am I the smallest primary alive, otherwise don't do anything.
    }

    // 4 cases arise
    if (rcRecord.isActiveRunning() && rcRecord.isMarkedForRemoval() == false) { // nothing to do
      return;
    }
    else if (rcRecord.isActiveRunning() == false && rcRecord.isMarkedForRemoval() == false) { // group change ongoing
      if (groupChangeProgress.containsKey(rcRecord.getName()) == false) { // I am not doing it
        updateGroupChangeProgress(rcRecord.getName(), STOP_SENT); // start to do
        StopActiveSetTask stopTask = new StopActiveSetTask(rcRecord.getName(), rcRecord.getOldActiveNameservers(),
                rcRecord.getOldActivePaxosID());
        NameServer.timer.schedule(stopTask, 0, RC_TIMEOUT_MILLIS);
      }
    }
    else if (rcRecord.isActiveRunning() && rcRecord.isMarkedForRemoval()) { // removal in progress
      if (removeRecordRequests.containsKey(rcRecord.getName()) == false) { // I am not doing it
        removeRecordRequests.put(rcRecord.getName(), new RemoveRecordPacket(new Random().nextInt(), rcRecord.getName(),
                -1));
        // complete removing record
        updateGroupChangeProgress(rcRecord.getName(), STOP_SENT);
        StopActiveSetTask stopTask = new StopActiveSetTask(rcRecord.getName(), rcRecord.getActiveNameservers(),
                rcRecord.getActivePaxosID());
        NameServer.timer.schedule(stopTask, 0, RC_TIMEOUT_MILLIS);
      }
    }
    else if (rcRecord.isActiveRunning() == false && rcRecord.isMarkedForRemoval()) {
      // both group change and remove record in progress
      if (removeRecordRequests.containsKey(rcRecord.getName()) == false) {
        // not doing remove record? enter
        removeRecordRequests.put(rcRecord.getName(), new RemoveRecordPacket(new Random().nextInt(),rcRecord.getName(),
                -1));
      }
      if (groupChangeProgress.containsKey(rcRecord.getName()) == false) {
        // start to do group change?
        updateGroupChangeProgress(rcRecord.getName(), STOP_SENT);
        StopActiveSetTask stopTask = new StopActiveSetTask(rcRecord.getName(), rcRecord.getOldActiveNameservers(),
                rcRecord.getOldActivePaxosID());
        NameServer.timer.schedule(stopTask, 0, RC_TIMEOUT_MILLIS);
      }
    }

  }

//  /**
//   *
//   * @param rcRecord
//   * @param failedNode
//   * @throws FieldNotFoundException
//   */
//  private static void handlePrimaryFailureForNameRecord(ReplicaControllerRecord rcRecord, int failedNode) throws
//          FieldNotFoundException{
//
////    if (isSmallestNodeRunningAfterFailedNode(failedNode, nameRecord.getName(), nameRecord.getPrimaryNameservers())
////          == false) return;
//    if (StartNameServer.debugMode) {
//      GNS.getLogger().info("Handling node failure for name record: " + rcRecord.getName()
//              + " Failed Node: " + failedNode + " STAGE = " + rcRecord.isActiveRunning());
//    }
//    if (rcRecord.isActiveRunning()) {
//      //
//      if (rcRecord.isMarkedForRemoval() && !rcRecord.isRemoved()   // removal in progress
//              && removeRecordRequests.containsKey(rcRecord.getName()) == false // i am not doing the removal
//              && isSmallestNodeRunning(rcRecord.getName(), rcRecord.getPrimaryNameservers())) { // but i am smallest
//                        primary running
//        // if in the process of deletion
//        rcRecord.updateActiveNameServers(rcRecord.getActiveNameservers(), getActivePaxosID(rcRecord.getName()));
//        StopActiveSetTask stopTask = new StopActiveSetTask(rcRecord.getName(),
//                rcRecord.getOldActiveNameservers(), rcRecord.getOldActivePaxosID());
//        NameServer.timer.schedule(stopTask, 0, RC_TIMEOUT_MILLIS);
//      }
//      else {
//        GNS.getLogger().info("No action for name: " + rcRecord.getName());
//      }
//    }
//    else {
//      if (groupChangeProgress.containsKey(rcRecord.getName()) == false && // i am not doing the group change
//          isSmallestNodeRunning(rcRecord.getName(), rcRecord.getPrimaryNameservers())) { // but i am smallest primary running
//
//        StopActiveSetTask task = new StopActiveSetTask(rcRecord.getName(), rcRecord.getOldActiveNameservers(),
//                rcRecord.getOldActivePaxosID());
//        NameServer.timer.schedule(task, 0, RC_TIMEOUT_MILLIS);
//        if (StartNameServer.debugMode) {
//          GNS.getLogger().info(" Started the old actives task. upon failure of node");
//        }
//      }
//    }
//
//}

//  private static void handlePrimaryFailureForNameRecord(ReplicaControllerRecord nameRecord, int failedNode)
//          throws FieldNotFoundException{
//
//    ReplicaControllerRecord.ACTIVE_STATE stage = nameRecord.getNewActiveTransitionStage();
//    if (StartNameServer.debugMode) {
//      GNS.getLogger().info("Handling node failure for name record: " + nameRecord.getName()
//              + " Failed Node: " + failedNode + " STAGE = " + stage);
//    }
//    // worry only if I am smallest primary
//    if (isSmallestNodeRunningAfterFailedNode(failedNode, nameRecord.getName(), nameRecord.getPrimaryNameservers()) == false) return;
//    GNS.getLogger().info(" Smallest node for name = " + nameRecord.getName());
//    switch (stage) {
//      case ACTIVE_RUNNING:
//        if (nameRecord.isMarkedForRemoval() && !nameRecord.isRemoved()) {
//          // if in the process of deletion
//          nameRecord.updateActiveNameServers(nameRecord.getActiveNameservers(), getActivePaxosID(nameRecord.getName()));
//          StopActiveSetTask stopTask = new StopActiveSetTask(nameRecord.getName(),
//                  nameRecord.getOldActiveNameservers(), nameRecord.getOldActivePaxosID());
//          NameServer.timer.schedule(stopTask, 0, RC_TIMEOUT_MILLIS);
//        }
//        else {
//          GNS.getLogger().info("No action for name: " + nameRecord.getName());
//        }
//        break;
//      case OLD_ACTIVE_RUNNING:
//        // stop old actives, since we do not know whether old active is stopped or not.
//        // if isMarkedForRemoval() == true: then
//        // 			(1) we will make sure that old active has stopped
//        //			(2) then remove name record
//        StopActiveSetTask task = new StopActiveSetTask(nameRecord.getName(),
//                nameRecord.getOldActiveNameservers(),
//                nameRecord.getOldActivePaxosID());
//        NameServer.timer.schedule(task, 0, RC_TIMEOUT_MILLIS);
//        if (StartNameServer.debugMode) {
//          GNS.getLogger().info(" Started the old actives task. upon failure of node");
//        }
//        break;
//      case NO_ACTIVE_RUNNING: // Abhigyan: this case wont arise because
//        // start to run new active replicas, since we do not know whether new active is running or not.
//        // if isMarkedForRemoval() == true: then
//        //			(1) make sure new active has started
//        //			(2) next we will stop new active
//        //			(3) then remove name record
//
//        if (nameRecord.getActivePaxosID().endsWith("-2")) return; // TODO MAGIC NUMBER used here
//        // actives failed to start at the time request was added,
//        // since I do not have the initial value for name record the client sent, I will not try to start the actives.
//
//        StartActiveSetTask startupTask = new StartActiveSetTask(nameRecord.getName(),
//                nameRecord.getOldActiveNameservers(),
//                nameRecord.getActiveNameservers(),
//                nameRecord.getActivePaxosID(), nameRecord.getOldActivePaxosID(), null);
//        // scheduled
//        NameServer.timer.schedule(startupTask, 0, RC_TIMEOUT_MILLIS);
//        break;
//      default:
//        break;
//    }
//  }

  public static boolean isSmallestNodeRunning(String name, Set<Integer> nameServers) {
    Random r = new Random(name.hashCode());
    ArrayList<Integer> x1  = new ArrayList<Integer>(nameServers);
    Collections.sort(x1);
    Collections.shuffle(x1, r);
    for (int x: x1) {
      if (PaxosManager.isNodeUp(x) ) {
        return  x == NameServer.nodeID;
      }
    }
    return false;

//    Random r = new Random(name.hashCode());
//    int smallestNSUp = -1;
//    for (Integer ns : nameServers) {
//      if (PaxosManager.isNodeUp(ns)) {
//        smallestNSUp = ns;
//        break;
////        if (smallestNSUp == -1 || primaryNS < smallestNSUp) {
////          smallestNSUp = primaryNS;
////        }
//      }
//    }
//    if (smallestNSUp == NameServer.nodeID) {
//      return true;
//    } else {
//      return false;
//    }
  }

  public static boolean isSmallestNodeRunningAfterFailedNode(int failedNodeID, String name, Set<Integer> nameServers) {
    Random r = new Random(name.hashCode());
    ArrayList<Integer> x1  = new ArrayList<Integer>(nameServers);
    Collections.sort(x1);
    Collections.shuffle(x1, r);
    boolean failedSeen = false;
    for (int x: x1) {
      if (x == failedNodeID) failedSeen = true;
      if (PaxosManager.isNodeUp(x)) {
        return failedSeen && x == NameServer.nodeID;
      }
    }
    return false;
  }



  public static void main(String[] args) {

    HashFunction.initializeHashFunction();
    ConfigFileInfo.setNumberOfNameServers(100);
    NameServer.nodeID = 0;
    int numNames = 10000;
    ConcurrentHashMap<Integer, Integer> smallestCounts = new ConcurrentHashMap<Integer, Integer>();
    for (int i = 0; i < numNames; i++) {
      Set<Integer> nodes = HashFunction.getPrimaryReplicas(Integer.toString(i));
      int smallestPrimary = getSmallestPrimaryRunning(nodes);
      if (smallestCounts.containsKey(smallestPrimary)) {
        smallestCounts.put(smallestPrimary, smallestCounts.get(smallestPrimary) + 1);
      }else {
        smallestCounts.put(smallestPrimary, 1);
      }
    }

    for (int x: smallestCounts.keySet()) {
      System.out.println(x + "\t" + smallestCounts.get(x));
    }
  }

  private static int getSmallestPrimaryRunning(Set<Integer> primaryNameServer) {
    int smallestNSUp = -1;
    for (Integer primaryNS : primaryNameServer) {
      return primaryNS;
//      if (PaxosManager.isNodeUp(primaryNS)) {
//
//      }
//      if (smallestNSUp == -1 || primaryNS < smallestNSUp) {
//        smallestNSUp = primaryNS;
//      }
    }
    return  smallestNSUp;
//    if (smallestNSUp == NameServer.nodeID) {
//      return true;
//    } else {
//      return false;
//    }
  }
}
