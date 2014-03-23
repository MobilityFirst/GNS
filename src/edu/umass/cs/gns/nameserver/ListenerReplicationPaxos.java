package edu.umass.cs.gns.nameserver;

import edu.umass.cs.gns.database.ColumnField;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.recordmap.NameRecord;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaController;
import edu.umass.cs.gns.packet.NewActiveSetStartupPacket;
import edu.umass.cs.gns.packet.OldActiveSetStopPacket;
import edu.umass.cs.gns.packet.Packet;
import edu.umass.cs.gns.packet.Packet.PacketType;
import edu.umass.cs.gns.packet.paxospacket.PaxosPacketType;
import edu.umass.cs.gns.packet.paxospacket.RequestPacket;
import edu.umass.cs.gns.util.BestServerSelection;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class listens for messages related to replication of name records from primary name servers and other active name
 * servers.
 *
 * @author abhigyan
 *
 */
public class ListenerReplicationPaxos {

  public static ConcurrentHashMap<Integer, NewActiveStartInfo> activeStartupInProgress =
          new ConcurrentHashMap<Integer, NewActiveStartInfo>();
  public static ConcurrentHashMap<Integer, NewActiveSetStartupPacket> activeStartupPacketsReceived =
          new ConcurrentHashMap<Integer, NewActiveSetStartupPacket>();

  public static void handleIncomingPacket(JSONObject json) {
    NameServer.getExecutorService().submit(new ReplicationWorkerPaxos(json));
  }


  public static void handleActivePaxosStop(JSONObject json ) {
    // STOP command is performed by paxos instance replica.
    OldActiveSetStopPacket oldActiveStopPacket = null;
    try {
      oldActiveStopPacket = new OldActiveSetStopPacket(json);
    } catch (JSONException e) {
      e.printStackTrace();
      return;
    }

    if (StartNameServer.debugMode) GNS.getLogger().info("PAXOS DECISION: Old Active Stopped: Name = "
            + oldActiveStopPacket.getName() + "\t" + oldActiveStopPacket);
    String paxosID = oldActiveStopPacket.getPaxosIDToBeStopped();


    NameRecord nameRecord;
    try {
      nameRecord = NameRecord.getNameRecordMultiField(NameServer.getRecordMap(), oldActiveStopPacket.getName(),ReplicationWorkerPaxos.getActivePaxosStopFields());
    } catch (RecordNotFoundException e) {
      GNS.getLogger().info("Record not found exception. Message = " + e.getMessage());
      return;
    }
    try{
      nameRecord.handleCurrentActiveStop(paxosID);
      ReplicationWorkerPaxos.sendOldActiveStopConfirmationToPrimary(oldActiveStopPacket);

    } catch (FieldNotFoundException e) {
      GNS.getLogger().info("FieldNotFoundException. " + e.getMessage());
      e.printStackTrace();
    } catch (JSONException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void createPaxosInstanceForName(String name, Set<Integer> activeNameServers, String activePaxosID,
                                                ValuesMap previousValue, long initScoutDelay, int ttl){
    NameRecord nameRecord = new NameRecord(       NameServer.getRecordMap(), name, activeNameServers, activePaxosID, previousValue, ttl);
    createPaxosInstanceForName(nameRecord, initScoutDelay);
  }
  public static void createPaxosInstanceForName(NameRecord nameRecord, long initScoutDelay){
    try {


      if (StartNameServer.eventualConsistency == false) {
        boolean created = NameServer.getPaxosManager().createPaxosInstance(nameRecord.getName(),
                ReplicaController.getVersionFromPaxosID(nameRecord.getActivePaxosID()),
                nameRecord.getActiveNameServers(), nameRecord.toString());
        if (StartNameServer.debugMode) GNS.getLogger().info(" NAME RECORD ADDED AT ACTIVE NODE: "
                + "name record = " + nameRecord.getName());
        if (created) {
          if (StartNameServer.debugMode) GNS.getLogger().info(" PAXOS INSTANCE CREATED AT ACTIVE NAME SERVER. " + nameRecord.getName());
        } else {
          if (StartNameServer.debugMode) GNS.getLogger().info(" PAXOS INSTANCE NOT CREATED. "  + nameRecord.getName());
        }
      }

    } catch (Exception e) {
      GNS.getLogger().info(" Exception Exception Exception: ****************");
      e.getMessage();
      e.printStackTrace();
    }
  }

}

/**
 * Timer object used to handle updates message related to replication of record between actives replicas and
 * between an active replica and a replica controller.
 */
class ReplicationWorkerPaxos extends TimerTask {

  JSONObject json;

  public ReplicationWorkerPaxos(JSONObject myJSON) {
    this.json = myJSON;
  }

  @Override
  public void run() {
    try {
      switch (Packet.getPacketType(json)) {

        case NEW_ACTIVE_START:
          if (StartNameServer.debugMode) GNS.getLogger().info("Received msg NEW_ACTIVE_START: " + json.toString());
          NewActiveSetStartupPacket packet = new NewActiveSetStartupPacket(json);

          // sanity check: am I in set? otherwise quit.
          if (!packet.getNewActiveNameServers().contains(NameServer.getNodeID())) {
            GNS.getLogger().info("ERROR: NewActiveSetStartupPacket reached "
                    + "a non-active name server." + packet.toString());
            break;
          }
          // create name server
          NewActiveStartInfo activeStartInfo = new NewActiveStartInfo(new NewActiveSetStartupPacket(json));
          ListenerReplicationPaxos.activeStartupInProgress.put(packet.getID(), activeStartInfo);
          // send to all nodes, except self
          packet.changePacketTypeToForward();
          if (StartNameServer.debugMode) GNS.getLogger().info("NEW_ACTIVE_START: forwarded msg to nodes; "
                  + packet.getNewActiveNameServers());

          NameServer.getTcpTransport().sendToIDs(packet.getNewActiveNameServers(),packet.toJSONObject(), NameServer.getNodeID());

          // start-up paxos instance at this node.

          CopyStateFromOldActiveTask copyTask = new CopyStateFromOldActiveTask(packet);
          NameServer.getTimer().schedule(copyTask, 0, ReplicaController.RC_TIMEOUT_MILLIS / 4);
          break;

        case NEW_ACTIVE_START_FORWARD:
          packet = new NewActiveSetStartupPacket(json);
          // Abhigyan: keeping this code as we might enable this option in future
//          if (packet.getPreviousValue() != null  && packet.getPreviousValue().isEmpty() == false) {
//            if(StartNameServer.debugMode) GNS.getLogger().info(packet.getName()
//                    + "\tUsing Value in NewActiveSetStartupPacket To Create Name Record." + packet.getPreviousValue());
//            addNameRecord(packet, packet.getPreviousValue());
//          }
//          else {
          copyTask = new CopyStateFromOldActiveTask(packet);
          NameServer.getTimer().schedule(copyTask, 0, ReplicaController.RC_TIMEOUT_MILLIS /4);
//          }
          break;

        case NEW_ACTIVE_START_PREV_VALUE_REQUEST:
          packet = new NewActiveSetStartupPacket(json);
          if (StartNameServer.debugMode) GNS.getLogger().info(" Received NEW_ACTIVE_START_PREV_VALUE_REQUEST at node " + NameServer.getNodeID());
          // obtain name record


          try {
            NameRecord nameRecord = NameRecord.getNameRecordMultiField(NameServer.getRecordMap(), packet.getName(), getPrevValueRequestFields());
            ValuesMap value = null;
            try {
              value = nameRecord.getOldValuesOnPaxosIDMatch(packet.getOldActivePaxosID());
            } catch (FieldNotFoundException e) {
              packet.changePreviousValueCorrect(false);
              //              e.printStackTrace();
            }
            if (value == null) {
              packet.changePreviousValueCorrect(false);
            } else {
              // update previous value
              packet.changePreviousValueCorrect(true);
              packet.changePreviousValue(value);
              packet.setTTL(nameRecord.getTimeToLive());
            }
          } catch (RecordNotFoundException e) {
            packet.changePreviousValueCorrect(false);
//            e.printStackTrace();  
          }
          packet.changePacketTypeToPreviousValueResponse();

          if (StartNameServer.debugMode) GNS.getLogger().info(" NEW_ACTIVE_START_PREV_VALUE_REQUEST reply sent to: " + packet.getSendingActive());
          // reply to sending active
          NameServer.getTcpTransport().sendToID(packet.getSendingActive(), packet.toJSONObject());
          // send current value to
          break;

        case NEW_ACTIVE_START_PREV_VALUE_RESPONSE:

          packet = new NewActiveSetStartupPacket(json);
          if (StartNameServer.debugMode) GNS.getLogger().info(" Received NEW_ACTIVE_START_PREV_VALUE_RESPONSE at node " + NameServer.getNodeID());
          if (packet.getPreviousValueCorrect()) {
            NewActiveSetStartupPacket originalPacket = ListenerReplicationPaxos.activeStartupPacketsReceived.remove(packet.getID());
            if (originalPacket != null) {
              addNameRecord(originalPacket, packet.getPreviousValue(), packet.getTTL());
            } else {
              if (StartNameServer.debugMode) GNS.getLogger().info(" NewActiveSetStartupPacket not found for response.");
            }
          } else {
            if (StartNameServer.debugMode) GNS.getLogger().info(" Old Active did not return previous value.");
          }
          break;

        case NEW_ACTIVE_START_RESPONSE:
          packet = new NewActiveSetStartupPacket(json);
          NewActiveStartInfo info = ListenerReplicationPaxos.activeStartupInProgress.get(packet.getID());
          if (StartNameServer.debugMode) GNS.getLogger().info("NEW_ACTIVE_START: received confirmation from "
                  + "node: " + packet.getSendingActive());
          if (info != null) {
            info.receivedResponseFromActive(packet.getSendingActive());
            if (info.haveMajorityActivesResponded()) {
              if (StartNameServer.debugMode) GNS.getLogger().info("NEW_ACTIVE_START: received confirmation from majority. name = " + packet.getName());
              info.packet.changePacketTypeToConfirmation();
              NameServer.getTcpTransport().sendToID(info.packet.getSendingPrimary(),info.packet.toJSONObject());
              ListenerReplicationPaxos.activeStartupInProgress.remove(packet.getID());
            }
          }
          break;
        case OLD_ACTIVE_STOP:

          OldActiveSetStopPacket oldActiveStopPacket = new OldActiveSetStopPacket(json);
          if (StartNameServer.debugMode) GNS.getLogger().info("Received Old Active Stop Packet: " + json);
          String paxosID = oldActiveStopPacket.getPaxosIDToBeStopped();
          // if this is current active:

          NameRecord nameRecord1;
          try {
            nameRecord1 = NameRecord.getNameRecordMultiField(NameServer.getRecordMap(), oldActiveStopPacket.getName(), getOldActiveStopFields());
          } catch (RecordNotFoundException e) {
            GNS.getLogger().info("Record not found exception. Name = " + oldActiveStopPacket.getName());
            break;
          }
          try {
            if (StartNameServer.debugMode) GNS.getLogger().info("NAME RECORD NOW: " + nameRecord1);
            int paxosStatus = nameRecord1.getPaxosStatus(paxosID);
            if (StartNameServer.debugMode) GNS.getLogger().info("PaxosIDtoBeStopped = " + paxosID + " PaxosStatus = " + paxosStatus);
            if (paxosStatus == 1) { // this paxos ID is current active
              // propose STOP command for this paxos instance
              // Change Packet Type in oldActiveStop: This will help paxos identify that
              // this is a stop packet. See: PaxosManager.isStopCommand()
              oldActiveStopPacket.changePacketTypeToPaxosStop();

              // Put client ID = PAXOS_STOP.getInt() so that PaxosManager can route decision
              // to this class.
              if (StartNameServer.eventualConsistency) {
                NameServer.getTcpTransport().sendToIDs(nameRecord1.getActiveNameServers(), oldActiveStopPacket.toJSONObject());
              } else {
                NameServer.getPaxosManager().propose(nameRecord1.getName(), new RequestPacket(PacketType.ACTIVE_PAXOS_STOP.getInt(),
                        oldActiveStopPacket.toString(), PaxosPacketType.REQUEST, true));
              }

              if (StartNameServer.debugMode) GNS.getLogger().info("PAXOS PROPOSE: STOP Current Active Set. Paxos ID = " + paxosID);
            } else if (paxosStatus == 2) { // this is the old paxos ID
              // send confirmation to primary that this paxos ID is stopped.
              sendOldActiveStopConfirmationToPrimary(oldActiveStopPacket);
            } else {
              // if new active start packet comes before old active stop is committed, this situation might arise.
              GNS.getLogger().info("PAXOS ID Neither current nor old. Ignore msg = " + paxosID);
            }
          } catch (FieldNotFoundException e) {
            GNS.getLogger().info("FieldNotFoundException: " + e.getMessage());
            e.printStackTrace();
          }
          break;
        case ACTIVE_PAXOS_STOP:
          ListenerReplicationPaxos.handleActivePaxosStop(json);

          break;
        default:
          break;
      }
    } catch (JSONException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static  ArrayList<ColumnField> activePaxosStopFields = new ArrayList<ColumnField>();

  static ArrayList<ColumnField> getActivePaxosStopFields() {
    synchronized (activePaxosStopFields) {
      if (activePaxosStopFields.size() > 0)return  activePaxosStopFields;
      activePaxosStopFields.add(NameRecord.ACTIVE_PAXOS_ID);
      activePaxosStopFields.add(NameRecord.VALUES_MAP);
      return activePaxosStopFields;
    }
  }

  private static ArrayList<ColumnField> oldActiveStopFields = new ArrayList<ColumnField>();

  private static ArrayList<ColumnField> getOldActiveStopFields() {
    synchronized (oldActiveStopFields) {
      if (oldActiveStopFields.size() > 0) return  oldActiveStopFields;
      oldActiveStopFields.add(NameRecord.ACTIVE_PAXOS_ID);
      oldActiveStopFields.add(NameRecord.OLD_ACTIVE_PAXOS_ID);
      oldActiveStopFields.add(NameRecord.ACTIVE_NAMESERVERS);
      return oldActiveStopFields;
    }
  }

  private static ArrayList<ColumnField> prevValueRequestFields = new ArrayList<ColumnField>();

  private static ArrayList<ColumnField> getPrevValueRequestFields() {
    synchronized (prevValueRequestFields) {
      if (prevValueRequestFields.size() > 0) return prevValueRequestFields;
      prevValueRequestFields.add(NameRecord.OLD_ACTIVE_PAXOS_ID);
      prevValueRequestFields.add(NameRecord.OLD_VALUES_MAP);
      prevValueRequestFields.add(NameRecord.TIME_TO_LIVE);
      return prevValueRequestFields;
    }
  }

  /**
   *
   * @param oldActiveStopPacket
   * @throws JSONException
   * @throws IOException
   */
  static void sendOldActiveStopConfirmationToPrimary(OldActiveSetStopPacket oldActiveStopPacket)
          throws IOException, JSONException {

    // confirm to primary name server that this set of actives has stopped
    if (oldActiveStopPacket.getActiveReceiver() == NameServer.getNodeID()) {
      Long groupChangeStartTime = ReplicaController.groupChangeStartTimes.get(oldActiveStopPacket.getName());
      if (groupChangeStartTime != null) {
        long groupChangeDuration = System.currentTimeMillis()  - groupChangeStartTime;
        if (StartNameServer.debugMode) GNS.getLogger().info("\tStopActiveCompleted\t" + oldActiveStopPacket.getName() + "\t" + groupChangeDuration+ "\t");
      }
      // the active node who received this node, sends confirmation to primary
      // confirm to primary
      oldActiveStopPacket.changePacketTypeToConfirm();

      NameServer.getTcpTransport().sendToID(oldActiveStopPacket.getPrimarySender(), oldActiveStopPacket.toJSONObject());

      if (StartNameServer.debugMode) GNS.getLogger().info("OLD ACTIVE STOP: Name Record Updated. Sent confirmation to primary."
              + " OldPaxosID = " + oldActiveStopPacket.getPaxosIDToBeStopped());
    } else {
      // other nodes do nothing.
      if (StartNameServer.debugMode) GNS.getLogger().info("OLD ACTIVE STOP: Name Record Updated. OldPaxosID = "
              + oldActiveStopPacket.getPaxosIDToBeStopped());
    }
  }

  private  void addNameRecord(NewActiveSetStartupPacket originalPacket, ValuesMap previousValue, int ttl)
          throws  JSONException, IOException{
    try {

      try {
        NameRecord nameRecord = new NameRecord(NameServer.getRecordMap(), originalPacket.getName(), originalPacket.getNewActiveNameServers(),
                originalPacket.getNewActivePaxosID(), previousValue, ttl);
        NameRecord.addNameRecord(NameServer.getRecordMap(), nameRecord);
        if (StartNameServer.debugMode) GNS.getLogger().info(" NAME RECORD ADDED AT ACTIVE NODE: "
                + "name record = " + originalPacket.getName());
        if (StartNameServer.eventualConsistency == false) {
          boolean created = NameServer.getPaxosManager().createPaxosInstance(originalPacket.getName(),
                  ReplicaController.getVersionFromPaxosID(originalPacket.getNewActivePaxosID()),
                  originalPacket.getNewActiveNameServers(), nameRecord.toString());
          if (created) {
            if (StartNameServer.debugMode) GNS.getLogger().info(" PAXOS INSTANCE CREATED AT ACTIVE NAME SERVER. " + nameRecord.getName());
          } else {
            if (StartNameServer.debugMode) GNS.getLogger().info(" PAXOS INSTANCE NOT CREATED. "  + nameRecord.getName());
          }
        }
      } catch (RecordExistsException e) {
        NameRecord nameRecord = null;

//        if (previousValue.containsKey(NameRecordKey.EdgeRecord.getName()))
        try {
          nameRecord = NameRecord.getNameRecord(NameServer.getRecordMap(), originalPacket.getName());
          nameRecord.handleNewActiveStart(originalPacket.getNewActiveNameServers(),
                  originalPacket.getNewActivePaxosID(), previousValue);
          if (StartNameServer.eventualConsistency == false) {
            boolean created = NameServer.getPaxosManager().createPaxosInstance(originalPacket.getName(),
                    ReplicaController.getVersionFromPaxosID(originalPacket.getNewActivePaxosID()),
                    originalPacket.getNewActiveNameServers(), nameRecord.toString());
            if (created) {
              if (StartNameServer.debugMode) GNS.getLogger().info(" PAXOS INSTANCE CREATED AT ACTIVE NAME SERVER. " + nameRecord.getName());
            } else {
              if (StartNameServer.debugMode) GNS.getLogger().info(" PAXOS INSTANCE NOT CREATED. "  + nameRecord.getName());
            }
          }
        } catch (FieldNotFoundException e1) {
          GNS.getLogger().severe("Field not found exception: " + e.getMessage());
          e1.printStackTrace();
        }
        catch (RecordNotFoundException e1) {
          GNS.getLogger().severe("Not possible because record just existed.");
          e1.printStackTrace();
        }

        if (StartNameServer.debugMode) GNS.getLogger().info(" NAME RECORD UPDATED AT ACTIVE  NODE. Name record = " + nameRecord);
      } catch (FieldNotFoundException e) {
        e.printStackTrace();
      }

      // send reply to main active
      originalPacket.changePacketTypeToResponse();
      int sendingActive = originalPacket.getSendingActive();
      originalPacket.changeSendingActive(NameServer.getNodeID());

      if (StartNameServer.debugMode) GNS.getLogger().info("NEW_ACTIVE_START: replied to active sending the startup packet from node: " + sendingActive);

      NameServer.getTcpTransport().sendToID(sendingActive, originalPacket.toJSONObject());
    } catch (Exception e) {
      GNS.getLogger().info(" Exception Exception Exception: ****************");
      e.getMessage();
      e.printStackTrace();
    }
  }



}

class NewActiveStartInfo {

  public NewActiveSetStartupPacket packet;
  private HashSet<Integer> activesResponded = new HashSet<Integer>();
  boolean sent = false;

  public NewActiveStartInfo(NewActiveSetStartupPacket packet) {
    this.packet = packet;
  }

  public synchronized void receivedResponseFromActive(int ID) {
    activesResponded.add(ID);
  }

  public synchronized boolean haveMajorityActivesResponded() {

    if (sent == false && activesResponded.size()*2 > packet.getNewActiveNameServers().size()) {
      sent = true;
      return true;
    }
    return false;
  }
}


class CopyStateFromOldActiveTask extends TimerTask {

  NewActiveSetStartupPacket packet;
  HashSet<Integer> oldActivesQueried;

  public CopyStateFromOldActiveTask(NewActiveSetStartupPacket packet) {
    this.packet = packet;
    oldActivesQueried = new HashSet<Integer>();
  }

  @Override
  public void run() {


    try {
      // do book-keeping for this packet
      if (StartNameServer.debugMode) GNS.getLogger().info(" NEW_ACTIVE_START_FORWARD received packet: " + packet.toJSONObject());
      if (oldActivesQueried.size() == 0) {
        ListenerReplicationPaxos.activeStartupPacketsReceived.put(packet.getID(), packet);
      }

      if (!ListenerReplicationPaxos.activeStartupPacketsReceived.containsKey(packet.getID())) {
        GNS.getLogger().info(" COPY State from Old Active Successful! Cancel Task; Actives Queried: " + oldActivesQueried);
        this.cancel();
        return;
      }

      // make a copy
      NewActiveSetStartupPacket packet2 = new NewActiveSetStartupPacket(packet.toJSONObject());
      // select old active to send request to
      int oldActive = BestServerSelection.getSmallestLatencyNSNotFailed(packet.getOldActiveNameServers(), oldActivesQueried);

      if (oldActive == -1) {
        GNS.getLogger().severe(" Exception ERROR:  No More Actives Left To Query. Cancel Task!!! paxosID " + packet);
        this.cancel();
        return;
      }
      oldActivesQueried.add(oldActive);
      if (StartNameServer.debugMode) GNS.getLogger().info(" OLD ACTIVE SELECTED = : " + oldActive);
      // change packet type
      packet2.changePacketTypeToPreviousValueRequest();
      // change sending active
      packet2.changeSendingActive(NameServer.getNodeID());

      try {
        NameServer.getTcpTransport().sendToID(oldActive, packet2.toJSONObject());
      } catch (IOException e) {
        GNS.getLogger().severe(" IOException here: " + e.getMessage());
        e.printStackTrace();
      } catch (JSONException e) {
        GNS.getLogger().severe(" JSONException here: " + e.getMessage());
        e.printStackTrace();
      }
      if (StartNameServer.debugMode) GNS.getLogger().info(" REQUESTED VALUE from OLD ACTIVE. PACKET: " + packet2);

    } catch (JSONException e) {
      e.printStackTrace();
    } catch (Exception e) {
      GNS.getLogger().severe("Exception in Copy State from old actives task. " + e.getMessage());
      e.printStackTrace();
    }
  }
}