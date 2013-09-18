package edu.umass.cs.gns.nameserver.replicacontroller;


import edu.umass.cs.gns.database.BasicRecordCursor;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.ListenerReplicationPaxos;
import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.nameserver.ValuesMap;
import edu.umass.cs.gns.database.Field;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.packet.*;
import edu.umass.cs.gns.packet.Packet.PacketType;
import edu.umass.cs.gns.packet.paxospacket.FailureDetectionPacket;
import edu.umass.cs.gns.packet.paxospacket.PaxosPacketType;
import edu.umass.cs.gns.packet.paxospacket.RequestPacket;
import edu.umass.cs.gns.paxos.PaxosManager;
import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.gns.util.HashFunction;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ReplicaController {

  /**
   * Timeout after which replica controller retries sending request to actives for (1) starting new actives
   * (2) stopping old actives.
   */
  public static int TIMEOUT_INTERVAL = 5000;

  /**
   * Set of RemoveRecordPacket that this node has currently received and is removing records for.
   */
  private static ConcurrentHashMap<String, RemoveRecordPacket> removeRecordRequests = new ConcurrentHashMap<String, RemoveRecordPacket>();


  /**
   * returns true if the given paxosID belongs to that between primary name servers for a name
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
    String[] tokens = primaryPaxosID.split("-");
    if (tokens.length == 2 && tokens[1].equals("P")) {
      return tokens[0];
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
    Random r = new Random();
    return nameRecord.getName() + "-" + r.nextInt(100000000);
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
    String[] tokens = activePaxosID.split("-");
    if (tokens.length == 2) {
      return tokens[0];
    }
    GNS.getLogger().severe("Error Exception: String is not a valid activePaxosID. String = " + activePaxosID);
    return  null;
  }






  public static void handleIncomingPacket(JSONObject json) {

    try {
      switch (Packet.getPacketType(json)) {
        case NEW_ACTIVE_START_CONFIRM_TO_PRIMARY:
          NewActiveSetStartupPacket packet = new NewActiveSetStartupPacket(
                  json);
          newActiveStartupReceivedConfirmationFromActive(packet);
          break;
        case OLD_ACTIVE_STOP_CONFIRM_TO_PRIMARY:
          OldActiveSetStopPacket oldPacket = new OldActiveSetStopPacket(
                  json);
          oldActivesStoppedReceivedConfirmationFromActive(oldPacket);
          break;
        default:
          break;
      }
    } catch (JSONException e) {
      if (StartNameServer.debugMode) {
        GNS.getLogger().fine("JSON Exception here.");
      }
      e.printStackTrace();
    } catch (Exception e) {
      if (StartNameServer.debugMode) {
        GNS.getLogger().fine("Exception in Replication controller .... ." + e.getMessage());
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
  public static void handleNameRecordAddAtPrimary(ReplicaControllerRecord recordEntry, ValuesMap valuesMap, long initScoutDelay) throws FieldNotFoundException{
//        if (StartNameServer.debugMode) GNS.getLogger().fine(recordEntry.getName() +
//                "\tBefore Paxos instance created for name: " + recordEntry.getName()
//                        + " Primaries: " + primaries);
//    long initScoutDelay = 0;
//    if (StartNameServer.paxosStartMinDelaySec > 0 && StartNameServer.paxosStartMaxDelaySec > 0) {
//      initScoutDelay = StartNameServer.paxosStartMaxDelaySec*1000 + new Random().nextInt(StartNameServer.paxosStartMaxDelaySec*1000 - StartNameServer.paxosStartMinDelaySec*1000);
//    }
    PaxosManager.createPaxosInstance(getPrimaryPaxosID(recordEntry), recordEntry.getPrimaryNameservers(),
            recordEntry.toString(), initScoutDelay);
    if (StartNameServer.debugMode) GNS.getLogger().fine(" Primary-paxos created: Name = " + recordEntry.getName());

//		if (StartNameServer.debugMode) GNS.getLogger().fine(recordEntry.getName()  +
    ListenerReplicationPaxos.addNameRecordLocal(recordEntry.getName(),recordEntry.getActiveNameservers(),
            recordEntry.getActivePaxosID(),valuesMap, initScoutDelay);

    if (StartNameServer.debugMode) GNS.getLogger().fine(" Active-paxos and name record created. Name = " + recordEntry.getName());
//				"\tPaxos instance created for name: " + recordEntry.getName()
//						+ " Primaries: " + primaries);
//    if (startActives) {
//      startupNewActives(recordEntry, valuesMap);
//      if (StartNameServer.debugMode) GNS.getLogger().fine(recordEntry.getName() + "\t" +
//              "Startup new actives: " + recordEntry.getName());
//    }
//    else {
//      if (StartNameServer.debugMode) GNS.getLogger().fine(recordEntry.getName() + "\t" +
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

    ArrayList<Field> readFields = new ArrayList<Field>();
    readFields.add(ReplicaControllerRecord.MARKED_FOR_REMOVAL);

    try {
      ReplicaControllerRecord nameRecordPrimary = NameServer.getNameRecordPrimaryMultiField(removeRecord.getName(), readFields);
      if (nameRecordPrimary.isRemoved()) { // if removed, send confirm to client
        ConfirmUpdateLNSPacket confirmPacket = new ConfirmUpdateLNSPacket(true, removeRecord);
        NameServer.tcpTransport.sendToID(removeRecord.getLocalNameServerID(),confirmPacket.toJSONObject());
        if (StartNameServer.debugMode) {
          GNS.getLogger().fine("Record already remove. Sent confirmation to client. Name = " + removeRecord.getName());
        }
        return;
      }

      if (nameRecordPrimary.isMarkedForRemoval() == true) {
        if (StartNameServer.debugMode) {
          GNS.getLogger().fine("Already marked for removal. Name record will be deleted soon. So request is dropped.");
        }
        return;
      }
      // propose this to primary paxos
      String primaryPaxosID = getPrimaryPaxosID(nameRecordPrimary);
      PaxosManager.propose(primaryPaxosID,
              new RequestPacket(removeRecord.getType().getInt(), removeRecord.toString(), PaxosPacketType.REQUEST, false));
      if (StartNameServer.debugMode) {
        GNS.getLogger().fine("PAXOS PROPOSAL: Proposed mark for removal in primary paxos. Packet = " + removeRecord);
      }
      removeRecordRequests.put(getPrimaryPaxosID(nameRecordPrimary), removeRecord);

    } catch (RecordNotFoundException e) {
      // return failure, because record was not even found in deleted state
      ConfirmUpdateLNSPacket confirmPacket = new ConfirmUpdateLNSPacket(false, removeRecord);
      NameServer.tcpTransport.sendToID(removeRecord.getLocalNameServerID(),confirmPacket.toJSONObject());

      if (StartNameServer.debugMode) {
        GNS.getLogger().fine("Record not found. Sent failure confirmation to client. Name = " + removeRecord.getName());
      }
      GNS.getLogger().severe(" REMOVE RECORD ERROR!! Name: " + removeRecord.getName());

    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field not found exception. " + e.getMessage());
    }

  }


  private static ArrayList<Field> applyMarkedForRemovalFields = new ArrayList<Field>();

  private static ArrayList<Field> getApplyMarkedForRemovalFields() {
    synchronized (applyMarkedForRemovalFields) {
      if (applyMarkedForRemovalFields.size() > 0) return applyMarkedForRemovalFields;

      applyMarkedForRemovalFields.add(ReplicaControllerRecord.MARKED_FOR_REMOVAL);
      applyMarkedForRemovalFields.add(ReplicaControllerRecord.OLD_ACTIVE_NAMESERVERS_RUNNING);
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
      GNS.getLogger().fine("PAXOS DECISION remove record packet accepted by paxos: " + value);
    }

//    ArrayList<Field> readFields = new ArrayList<Field>();


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

      if (StartNameServer.debugMode) GNS.getLogger().fine("PAXOS DECISION applied. Name Record marked for removal " + rcRecord.getName());

      ReplicaControllerRecord.ACTIVE_STATE stage = rcRecord.getNewActiveTransitionStage();
      if (StartNameServer.debugMode) GNS.getLogger().fine("ACTIVE Transition currently in stage = " + stage + " name " + rcRecord.getName());

  //    if (isSmallestNodeRunning(nameRecord.getPrimaryNameservers()) == false) return;

      if (StartNameServer.debugMode) GNS.getLogger().fine("Remove record request has keys: " + removeRecordRequests.keySet());
      if (removeRecordRequests.containsKey(getPrimaryPaxosID(rcRecord.getName())) == false) {
        if (StartNameServer.debugMode) GNS.getLogger().fine("SKIP: remove record request does not not contain " + rcRecord.getName());
        return;
      }
      switch (stage) {
        case ACTIVE_RUNNING:
//          ReplicaControllerRecord rcRecord = new ReplicaControllerRecord(removeRecord.getName());
          rcRecord.updateActiveNameServers(rcRecord.getActiveNameservers(), getActivePaxosID(removeRecord.getName()));
          StopActiveSetTask stopTask = new StopActiveSetTask(rcRecord.getName(),
                  rcRecord.getOldActiveNameservers(), rcRecord.getOldActivePaxosID());
          NameServer.timer.schedule(stopTask, 0, TIMEOUT_INTERVAL);

          break;
        default:
          break;
      }
//		else if (stage == 2) { // old active stopped, new active yet to run
//			// when new active runs, send message to stop it.
//
//		}
//		else if (stage == 1) { // old active not yet stopped
//			// wait for old active to stop. that process must be in progress.
//		}
    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field Not Found Exception. " + e.getMessage());
      GNS.getLogger().severe(rcRecord.toString());
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.

    }

  }



//  public static void startupNewActives(ReplicaControllerRecord nameRecord, ValuesMap initialValue) throws FieldNotFoundException{
//    // this method will schedule a timer task to startup active replicas.
//    StartupActiveSetTask startupTask = new StartupActiveSetTask(
//            nameRecord.getName(),
//            nameRecord.getOldActiveNameservers(),
//            nameRecord.getActiveNameservers(),
//            nameRecord.getActivePaxosID(), nameRecord.getOldActivePaxosID(), initialValue);
//    // scheduled
//    NameServer.timer.schedule(startupTask, 0, TIMEOUT_INTERVAL);
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
//    NameServer.timer.schedule(task, 0, TIMEOUT_INTERVAL);
//  }


  /**
   * Primary has received message from an active that the paxos instance between new actives has started.
   * This method propose a request to ReplicaControllerRecord to update that new actives are running.
   * @param packet
   */
  public static void newActiveStartupReceivedConfirmationFromActive(NewActiveSetStartupPacket packet) {
    if (StartNameServer.debugMode) {
      GNS.getLogger().fine("NEW_ACTIVE_START: Received confirmation at primary. " + packet.getName());
    }

//    ReplicaControllerRecord nameRecordPrimary = NameServer.getNameRecordPrimaryLazy(packet.getName());
    String paxosID = getPrimaryPaxosID(packet.getName());

    ChangeActiveStatusPacket proposePacket = new ChangeActiveStatusPacket(packet.getNewActivePaxosID(),
            packet.getName(), PacketType.NEW_ACTIVE_START_CONFIRM_TO_PRIMARY);

    // write to replica controller record object using Primary-paxos that newActive is running
    PaxosManager.propose(paxosID, new RequestPacket(
            PacketType.NEW_ACTIVE_START_CONFIRM_TO_PRIMARY.getInt(),
            proposePacket.toString(), PaxosPacketType.REQUEST, false));
    if (StartNameServer.debugMode) {
      GNS.getLogger().fine("PAXOS PROPOSAL: New Active Started for Name: " + packet.getName()
              + " Paxos ID = " + packet.getNewActiveNameServers());
    }

  }


  private static ArrayList<Field> newActiveStartedFields = new ArrayList<Field>();

  private static ArrayList<Field> getNewActiveStartedFields() {
    synchronized (newActiveStartedFields) {
      if (newActiveStartedFields.size() > 0) return newActiveStartedFields;
      newActiveStartedFields.add(ReplicaControllerRecord.MARKED_FOR_REMOVAL);
      newActiveStartedFields.add(ReplicaControllerRecord.ACTIVE_PAXOS_ID);
      newActiveStartedFields.add(ReplicaControllerRecord.ACTIVE_NAMESERVERS);
      newActiveStartedFields.add(ReplicaControllerRecord.ACTIVE_NAMESERVERS_RUNNING);
      return newActiveStartedFields;
    }
  }

  /**
   * Executes the result of paxos message proposed by <code>newActiveStartupReceivedConfirmationFromActive</code>.
   * Writes to <code>ReplicaControllerRecord</code> that new actives have started.
   * @param decision
   * @throws JSONException
   */
  public static void newActiveStartedWriteToNameRecord(String decision)
          throws JSONException {
    if (StartNameServer.debugMode) GNS.getLogger().fine("PAXOS DECISION: new active started. write to nameRecord: "+ decision);

    ChangeActiveStatusPacket packet = new ChangeActiveStatusPacket( new JSONObject(decision));

    try {
      ReplicaControllerRecord rcRecord = NameServer.getNameRecordPrimaryMultiField(packet.getName(), getNewActiveStartedFields());
      if (rcRecord.setNewActiveRunning(packet.getPaxosID())) {
      if (StartNameServer.debugMode)  GNS.getLogger().fine("New Active paxos running for name : "
              + packet.getName() + " Paxos ID: " + packet.getPaxosID());
      } else {
        if (StartNameServer.debugMode) GNS.getLogger().fine("IGNORE MSG: NEW Active PAXOS ID NOT FOUND while setting "
                + "it to inactive. Already received msg before. Paxos ID = " + packet.getPaxosID());
      }
      if (rcRecord.isMarkedForRemoval() == true) {

        rcRecord.updateActiveNameServers(rcRecord.getActiveNameservers(), getActivePaxosID(packet.getName()));
        StopActiveSetTask stopTask = new StopActiveSetTask(packet.getName(), rcRecord.getOldActiveNameservers(),
                rcRecord.getOldActivePaxosID());
        NameServer.timer.schedule(stopTask, 0, TIMEOUT_INTERVAL);
      }

    } catch (RecordNotFoundException e) {
      GNS.getLogger().severe("Record does not exist !! Should not happen. " + packet.getName());
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    } catch (FieldNotFoundException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

  }

  private static ArrayList<Field> oldActiveStopConfirmFields = new ArrayList<Field>();

  private static ArrayList<Field> getOldActiveStopConfirmFields() {
    synchronized (oldActiveStopConfirmFields) {
      if (oldActiveStopConfirmFields.size() > 0) return oldActiveStopConfirmFields;
      oldActiveStopConfirmFields.add(ReplicaControllerRecord.MARKED_FOR_REMOVAL);
      oldActiveStopConfirmFields.add(ReplicaControllerRecord.OLD_ACTIVE_PAXOS_ID);
      return oldActiveStopConfirmFields;
    }
  }

  /**
   * Primary has received confirmation from an active that the old set of actives have stopped.
   * This method proposes a request to primary-paxos to write this to the database. Once the write is complete,
   * the new set of actives can be started.
   *
   * In case the name is to be removed, this message indicates that paxos between actives is stopped, and that
   * primary-paxos can now be stopped.
   * @param packet
   */
  public static void oldActivesStoppedReceivedConfirmationFromActive(OldActiveSetStopPacket packet) {
    // write to name record object using Primary-paxos that oldActive is stopped
    //
    // schedule new active startup event: StartupReplicaSetTask

    if (StartNameServer.debugMode) GNS.getLogger().fine("OLD ACTIVE STOP: Primary recvd confirmation. Name  = "+ packet.getName());

    String paxosID = getPrimaryPaxosID(packet.getName());


//    readFields.add(ReplicaControllerRecord.OLD_ACTIVE_NAMESERVERS_RUNNING);

    ReplicaControllerRecord rcRecord;
    try {
      rcRecord = NameServer.getNameRecordPrimaryMultiField(packet.getName(), getOldActiveStopConfirmFields());
    } catch (RecordNotFoundException e) {
      GNS.getLogger().severe("Exception: name record should exist in DB. Error. " + e.getMessage());
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      return;
    }
    try {

      if (rcRecord.isMarkedForRemoval() == true) {
        rcRecord.setOldActiveStopped(packet.getPaxosIDToBeStopped()); // imp: this ensures that StopPaxosID task will see the paxos instance as completed.
        PaxosManager.propose(paxosID, new RequestPacket(PacketType.PRIMARY_PAXOS_STOP.getInt(),
                        packet.toString(), PaxosPacketType.REQUEST, true));
        if (StartNameServer.debugMode) {
          GNS.getLogger().fine("PAXOS PROPOSAL PROPOSED STOP COMMAND because "
                  + "name record is marked for removal: " + packet.toString());
        }
        return;
      }

      ChangeActiveStatusPacket proposePacket = new ChangeActiveStatusPacket(packet.getPaxosIDToBeStopped(),
              packet.getName(), PacketType.OLD_ACTIVE_STOP_CONFIRM_TO_PRIMARY);

      PaxosManager.propose(paxosID, new RequestPacket(PacketType.OLD_ACTIVE_STOP_CONFIRM_TO_PRIMARY.getInt(),
                      proposePacket.toString(), PaxosPacketType.REQUEST, false));

      if (StartNameServer.debugMode) {
        GNS.getLogger().fine("PAXOS PROPOSAL: Old Active Stopped for  Name: " + packet.getName()
                + " Old Paxos ID = " + packet.getPaxosIDToBeStopped());
      }
    }catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field not found exception. " + e.getMessage());
      e.printStackTrace();
    }

  }

  /**
   *
   * @param value
   * @throws JSONException
   * @throws IOException
   */
  public static void applyStopPrimaryPaxos(String value) throws JSONException, IOException {
    if (StartNameServer.debugMode)  GNS.getLogger().fine("PAXOS DECISION stop primary paxos decision received.");
    OldActiveSetStopPacket packet = new OldActiveSetStopPacket(new JSONObject(value));
    String paxosID = getPrimaryPaxosID(packet.getName()//, packet.getRecordKey()
    );
//		String name = packet.getName();
    RemoveRecordPacket removeRecordPacket = removeRecordRequests.remove(paxosID);

    ReplicaControllerRecord record = new ReplicaControllerRecord(packet.getName());
    try {
      if (record != null) record.setRemoved();
    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field not found exception. " + e.getMessage());
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    if (StartNameServer.debugMode) GNS.getLogger().fine("RECORD MARKED AS REMOVED IN REPLICA CONTROLLER DB");

    if (removeRecordPacket != null) {
      ConfirmUpdateLNSPacket confirmPacket = new ConfirmUpdateLNSPacket(true, removeRecordPacket);
      NameServer.tcpTransport.sendToID(removeRecordPacket.getLocalNameServerID(),confirmPacket.toJSONObject());
//      NSListenerUDP.udpTransport.sendPacket(confirmPacket.toJSONObject(),
//              confirmPacket.getLocalNameServerId(), GNS.PortType.LNS_UDP_PORT);
      if (StartNameServer.debugMode) GNS.getLogger().fine("REMOVE RECORD SENT RESPONSE TO LNS");
    }
  }

  private static ArrayList<Field> getOldActiveStoppedFields = new ArrayList<Field>();

  private static ArrayList<Field> getGetOldActiveStoppedFields() {
    synchronized (getOldActiveStoppedFields) {
      if (getOldActiveStoppedFields.size() > 0) return getOldActiveStoppedFields;
      getOldActiveStoppedFields.add(ReplicaControllerRecord.OLD_ACTIVE_PAXOS_ID);
      getOldActiveStoppedFields.add(ReplicaControllerRecord.ACTIVE_PAXOS_ID);
      getOldActiveStoppedFields.add(ReplicaControllerRecord.OLD_ACTIVE_NAMESERVERS);
      getOldActiveStoppedFields.add(ReplicaControllerRecord.ACTIVE_NAMESERVERS);
      getOldActiveStoppedFields.add(ReplicaControllerRecord.PRIMARY_NAMESERVERS);
      return getOldActiveStoppedFields;
    }
  }


  /**
   * Executes the result of paxos message proposed by <code>oldActivesStoppedReceivedConfirmationFromActive</code>.
   * Writes to <code>ReplicaControllerRecord</code> that old actives have stopped; we notify new actives to start.
   * @param decision
   * @throws JSONException
   */
  public static void oldActiveStoppedWriteToNameRecord(String decision)
          throws JSONException {
    ChangeActiveStatusPacket packet = new ChangeActiveStatusPacket(new JSONObject(decision));
    ArrayList<Field> fields = new ArrayList<Field>();


    ReplicaControllerRecord rcRecord;
    try {
      rcRecord = NameServer.getNameRecordPrimaryMultiField(packet.getName(), getGetOldActiveStoppedFields());

      if (StartNameServer.debugMode) GNS.getLogger().fine("PAXOS DECISION: old active stopped. write to nameRecord: "+ decision);

      if (rcRecord.setOldActiveStopped(packet.getPaxosID())) {
        if (StartNameServer.debugMode) GNS.getLogger().fine("OLD Active paxos stopped. Name: "+ rcRecord.getName()
                + " Old Paxos ID: "+ packet.getPaxosID());
        if (isSmallestNodeRunning(rcRecord.getPrimaryNameservers())) {
          StartupActiveSetTask startupTask = new StartupActiveSetTask(
                  rcRecord.getName(),
                  rcRecord.getOldActiveNameservers(),
                  rcRecord.getActiveNameservers(),
                  rcRecord.getActivePaxosID(), rcRecord.getOldActivePaxosID(), null);
          // scheduled
          NameServer.timer.schedule(startupTask, 0, TIMEOUT_INTERVAL);
        }
      } else {
        if (StartNameServer.debugMode) GNS.getLogger().fine("INGORE MSG: OLD PAXOS ID NOT FOUND IN ReplicaControllerRecord" +
                " while setting it to inactive: " + packet.getPaxosID());
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


  public static void handleNodeFailure(FailureDetectionPacket fdPacket) {
    if (fdPacket.status == true) return; // node was down and it came up, don't worry about that

    int failedNode = fdPacket.responderNodeID;
    GNS.getLogger().info(" Failed Node Detected: replication controller working. " + failedNode);

//		if (node fails then what happens)
    BasicRecordCursor iterator = NameServer.replicaController.getAllRowsIterator();
//    if (StartNameServer.debugMode) GNS.getLogger().fine("Got iterator : " + replicationRound);

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
          if (StartNameServer.debugMode) GNS.getLogger().fine(" Handing Failure for Name: " + record.getName()
                  + " NAME RECORD: " + record);
          handlePrimaryFailureForNameRecord(record, failedNode);
        }
      } catch (FieldNotFoundException e) {
        GNS.getLogger().fine("Field not found exception. This should not happen because we read complete record. " + e.getMessage());
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    }
  }


  private static void handlePrimaryFailureForNameRecord(ReplicaControllerRecord nameRecord, int failedNode)
          throws FieldNotFoundException{

    ReplicaControllerRecord.ACTIVE_STATE stage = nameRecord.getNewActiveTransitionStage();
    if (StartNameServer.debugMode) {
      GNS.getLogger().fine("Handling node failure for name record: " + nameRecord.getName()
              + " Failed Node: " + failedNode + " STAGE = " + stage);
    }
    // worry only if I am smallest primary
    if (isSmallestNodeRunning(nameRecord.getPrimaryNameservers()) == false) return;
    GNS.getLogger().severe(" Smallest node for name = " + nameRecord.getName());
    switch (stage) {
      case ACTIVE_RUNNING:
        if (nameRecord.isMarkedForRemoval() && !nameRecord.isRemoved()) {
          // if in the process of deletion
          nameRecord.updateActiveNameServers(nameRecord.getActiveNameservers(), getActivePaxosID(nameRecord.getName()));
          StopActiveSetTask stopTask = new StopActiveSetTask(nameRecord.getName(),
                  nameRecord.getOldActiveNameservers(), nameRecord.getOldActivePaxosID());
          NameServer.timer.schedule(stopTask, 0, TIMEOUT_INTERVAL);
        }
        else {
          GNS.getLogger().severe("Reached here ... done nothing.");
        }
        break;
      case OLD_ACTIVE_RUNNING:
        // stop old actives, since we do not know whether old active is stopped or not.
        // if isMarkedForRemoval() == true: then
        // 			(1) we will make sure that old active has stopped
        //			(2) then remove name record
        StopActiveSetTask task = new StopActiveSetTask(nameRecord.getName(),
                nameRecord.getOldActiveNameservers(),
                nameRecord.getOldActivePaxosID());
        NameServer.timer.schedule(task, 0, TIMEOUT_INTERVAL);
        if (StartNameServer.debugMode) {
          GNS.getLogger().fine(" Started the old actives task. upon failure of node");
        }
        break;
      case NO_ACTIVE_RUNNING:
        // start to run new active replicas, since we do not know whether new active is running or not.
        // if isMarkedForRemoval() == true: then 
        //			(1) make sure new active has started 
        //			(2) next we will stop new active 
        //			(3) then remove name record

        if (nameRecord.getActivePaxosID().endsWith("-2")) return; // TODO MAGIC NUMBER used here
        // actives failed to start at the time request was added,
        // since I do not have the initial value for name record the client sent, I will not try to start the actives.

        StartupActiveSetTask startupTask = new StartupActiveSetTask(nameRecord.getName(),
                nameRecord.getOldActiveNameservers(),
                nameRecord.getActiveNameservers(),
                nameRecord.getActivePaxosID(), nameRecord.getOldActivePaxosID(), null);
        // scheduled
        NameServer.timer.schedule(startupTask, 0, TIMEOUT_INTERVAL);
        break;
      default:
        break;
    }
  }

  public static boolean isSmallestNodeRunning(Set<Integer> nameServers) {
    int smallestNSUp = -1;
    for (Integer ns : nameServers) {
      if (PaxosManager.isNodeUp(ns)) {
        smallestNSUp = ns;
        break;
//        if (smallestNSUp == -1 || primaryNS < smallestNSUp) {
//          smallestNSUp = primaryNS;
//        }
      }
    }
    if (smallestNSUp == NameServer.nodeID) {
      return true;
    } else {
      return false;
    }
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
