package edu.umass.cs.gns.nameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.database.Field;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaController;
import edu.umass.cs.gns.packet.NewActiveSetStartupPacket;
import edu.umass.cs.gns.packet.OldActiveSetStopPacket;
import edu.umass.cs.gns.packet.Packet;
import edu.umass.cs.gns.packet.Packet.PacketType;
import edu.umass.cs.gns.packet.paxospacket.PaxosPacketType;
import edu.umass.cs.gns.packet.paxospacket.RequestPacket;
import edu.umass.cs.gns.paxos.PaxosManager;
import edu.umass.cs.gns.util.BestServerSelection;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class listens for messages related to dynamic replication of name records from primary name servers and other active name
 * servers
 *
 * @author abhigyan
 *
 */
public class ListenerReplicationPaxos {

  public static ConcurrentHashMap<Integer, NewActiveStartInfo> activeStartupInProgress =
          new ConcurrentHashMap<Integer, NewActiveStartInfo>();
  public static ConcurrentHashMap<Integer, NewActiveSetStartupPacket> activeStartupPacketsReceived =
          new ConcurrentHashMap<Integer, NewActiveSetStartupPacket>();

  //	public static ConcurrentHashMap<Integer, NewActiveStartInfo>
  public static void handleIncomingPacket(JSONObject json) {
    NameServer.executorService.submit(new ReplicationWorkerPaxos(json));
  }


  public static void handleActivePaxosStop(JSONObject json ) {
    // STOP command is performed by paxos instance replica.
    OldActiveSetStopPacket oldActiveStopPacket = null;
    try {
      oldActiveStopPacket = new OldActiveSetStopPacket(json);
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      return;
    }

    if (StartNameServer.debugMode) GNS.getLogger().fine("PAXOS DECISION: Old Active Stopped: Name = "
            + oldActiveStopPacket.getName() + "\t" + oldActiveStopPacket);
    String paxosID = oldActiveStopPacket.getPaxosIDToBeStopped();


    NameRecord nameRecord;
    try {
      nameRecord = NameServer.getNameRecordMultiField(oldActiveStopPacket.getName(),ReplicationWorkerPaxos.getActivePaxosStopFields());
    } catch (RecordNotFoundException e) {
      GNS.getLogger().fine("Record not found exception. Message = " + e.getMessage());
      return;
    }
    try{

      nameRecord.handleCurrentActiveStop(paxosID);

      ReplicationWorkerPaxos.sendOldActiveStopConfirmationToPrimary(oldActiveStopPacket);


    } catch (FieldNotFoundException e) {
      GNS.getLogger().fine("FieldNotFoundException. " + e.getMessage());
      e.printStackTrace();
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }

  public static void addNameRecordLocal(String name, Set<Integer> activeNameServers, String activePaxosID,
                                         ValuesMap previousValue, long initScoutDelay, int ttl){
    try {
//      long initScoutDelay = 0;
//      if (StartNameServer.paxosStartMinDelaySec > 0 && StartNameServer.paxosStartMaxDelaySec > 0) {
//        initScoutDelay = StartNameServer.paxosStartMaxDelaySec*1000 + new Random().nextInt(StartNameServer.paxosStartMaxDelaySec*1000 - StartNameServer.paxosStartMinDelaySec*1000);
//      }
      // try add: if add fails, try update.
      try {
        NameRecord nameRecord = new NameRecord(name, activeNameServers, activePaxosID, previousValue, ttl);
        NameServer.addNameRecord(nameRecord);
        boolean created = PaxosManager.createPaxosInstance(activePaxosID,
                activeNameServers, nameRecord.toString(), initScoutDelay);
        if (StartNameServer.debugMode) GNS.getLogger().fine(" NAME RECORD ADDED AT ACTIVE NODE: "
                + "name record = " + name);
        if (created) {
          if (StartNameServer.debugMode) GNS.getLogger().fine(" PAXOS INSTANCE CREATED AT ACTIVE NAME SERVER. " + name);
        } else {
          if (StartNameServer.debugMode) GNS.getLogger().fine(" PAXOS INSTANCE NOT CREATED. "  + name);
        }

      } catch (RecordExistsException e) {
        NameRecord nameRecord = null;
        try {
          nameRecord = NameServer.getNameRecord(name);
        } catch (RecordNotFoundException e1) {
          if (StartNameServer.debugMode) GNS.getLogger().fine("ERROR: record not found. not possible because it already exists.");

          e1.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        try {
          nameRecord.handleNewActiveStart(activeNameServers, activePaxosID, previousValue);
        } catch (FieldNotFoundException e1) {
          GNS.getLogger().fine("FieldNotFoundException: " + e1.getMessage());
          e1.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        if (StartNameServer.debugMode) GNS.getLogger().fine(" NAME RECORD UPDATED AT ACTIVE  NODE. "
                + "name record = " + nameRecord);
        boolean created = PaxosManager.createPaxosInstance(activePaxosID,
                activeNameServers, nameRecord.toString(), initScoutDelay);
        if (StartNameServer.debugMode) GNS.getLogger().fine(" NAME RECORD ADDED AT ACTIVE NODE: "
                + "name record = " + name);
        if (created) {
          if (StartNameServer.debugMode) GNS.getLogger().fine(" PAXOS INSTANCE CREATED AT ACTIVE NAME SERVER. " + name);
        } else {
          if (StartNameServer.debugMode) GNS.getLogger().fine(" PAXOS INSTANCE NOT CREATED. "  + name);
        }


      }
//      if (nameRecord == null) {
//
//        nameRecord = new NameRecord(name);
//
//        nameRecord.handleNewActiveStart(activeNameServers,
//                activePaxosID, previousValue);
//        // first add name record, then create paxos instance for it.
//
//        NameServer.addNameRecord(nameRecord);
//
//        //DBNameRecord.addNameRecord(nameRecord);
//
//      } else {
//        nameRecord.handleNewActiveStart(activeNameServers,
//                activePaxosID, previousValue);
//        NameServer.updateNameRecord(nameRecord);
//        //DBNameRecord.updateNameRecord(nameRecord);
//
//      }
//      // put the previous value obtained in the name record.
//      if (StartNameServer.debugMode) GNS.getLogger().fine(" NEW_ACTIVE_START_PREV_VALUE_RESPONSE. "
//              + "Name Record Value: " + nameRecord);
//      // fire up paxos instance


    } catch (Exception e) {
      GNS.getLogger().fine(" Exception Exception Exception: ****************");
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
          if (StartNameServer.debugMode) GNS.getLogger().fine("Received msg NEW_ACTIVE_START: " + json.toString());
          NewActiveSetStartupPacket packet = new NewActiveSetStartupPacket(json);

          // sanity check: am I in set? otherwise quit.
          if (!packet.getNewActiveNameServers().contains(NameServer.nodeID)) {
            GNS.getLogger().fine("ERROR: NewActiveSetStartupPacket reached "
                    + "a non-active name server." + packet.toString());
            break;
          }
          // create name server
          NewActiveStartInfo activeStartInfo = new NewActiveStartInfo(new NewActiveSetStartupPacket(json));
          ListenerReplicationPaxos.activeStartupInProgress.put(packet.getID(), activeStartInfo);
          // send to all nodes, except self
          packet.changePacketTypeToForward();
          if (StartNameServer.debugMode) GNS.getLogger().fine("NEW_ACTIVE_START: forwarded msg to nodes; "
                  + packet.getNewActiveNameServers());
//          NameServer.tcpTransport.sendToAll(packet.toJSONObject(), packet.getNewActiveNameServers(),
//                  GNS.PortType.PERSISTENT_TCP_PORT, NameServer.nodeID);
          NameServer.tcpTransport.sendToIDs(packet.getNewActiveNameServers(),packet.toJSONObject(), NameServer.nodeID);

//          if (packet.getPreviousValue() != null  && packet.getPreviousValue().isEmpty() == false) {
//            if(StartNameServer.debugMode) GNS.getLogger().fine(packet.getName() +
//                    "\tUsing Value in NewActiveSetStartupPacket To Create Name Record." + packet.getPreviousValue());
//            addNameRecord(packet, packet.getPreviousValue());
//            break;
//          }

          // start-up paxos instance at this node.

          CopyStateFromOldActiveTask copyTask = new CopyStateFromOldActiveTask(packet);
          NameServer.timer.schedule(copyTask, 0, ReplicaController.TIMEOUT_INTERVAL/4);
          break;

        case NEW_ACTIVE_START_FORWARD:
          packet = new NewActiveSetStartupPacket(json);
//          if (packet.getPreviousValue() != null  && packet.getPreviousValue().isEmpty() == false) {
//            if(StartNameServer.debugMode) GNS.getLogger().fine(packet.getName()
//                    + "\tUsing Value in NewActiveSetStartupPacket To Create Name Record." + packet.getPreviousValue());
//            addNameRecord(packet, packet.getPreviousValue());
//          }
//          else {
            copyTask = new CopyStateFromOldActiveTask(packet);
            NameServer.timer.schedule(copyTask, 0, ReplicaController.TIMEOUT_INTERVAL/4);
//          }
          break;

        case NEW_ACTIVE_START_PREV_VALUE_REQUEST:
          packet = new NewActiveSetStartupPacket(json);
          if (StartNameServer.debugMode) GNS.getLogger().fine(" Received NEW_ACTIVE_START_PREV_VALUE_REQUEST at node " + NameServer.nodeID);
          // obtain name record


          try {
            NameRecord nameRecord = NameServer.getNameRecordMultiField(packet.getName(), getPrevValueRequestFields());
            ValuesMap value = null;
            try {
              value = nameRecord.getOldValuesOnPaxosIDMatch(packet.getOldActivePaxosID());
            } catch (FieldNotFoundException e) {
              packet.changePreviousValueCorrect(false);
  //              e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
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
//            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
          }
          packet.changePacketTypeToPreviousValueResponse();

          if (StartNameServer.debugMode) GNS.getLogger().fine(" NEW_ACTIVE_START_PREV_VALUE_REQUEST reply sent to: " + packet.getSendingActive());
          // reply to sending active
//          NameServer.tcpTransport.sendToID(packet.toJSONObject(), packet.getSendingActive(), GNS.PortType.PERSISTENT_TCP_PORT);
          NameServer.tcpTransport.sendToID(packet.getSendingActive(), packet.toJSONObject());
          // send current value to
          break;

        case NEW_ACTIVE_START_PREV_VALUE_RESPONSE:

          packet = new NewActiveSetStartupPacket(json);
          if (StartNameServer.debugMode) GNS.getLogger().fine(" Received NEW_ACTIVE_START_PREV_VALUE_RESPONSE at node " + NameServer.nodeID);
          if (packet.getPreviousValueCorrect()) {
            NewActiveSetStartupPacket originalPacket = ListenerReplicationPaxos.activeStartupPacketsReceived.remove(packet.getID());
            if (originalPacket != null) {
              addNameRecord(originalPacket, packet.getPreviousValue(), packet.getTTL());
            } else {
              if (StartNameServer.debugMode) GNS.getLogger().fine(" NewActiveSetStartupPacket not found for response.");
            }
          } else {
            if (StartNameServer.debugMode) GNS.getLogger().fine(" Old Active did not return previous value.");
          }
          break;

        case NEW_ACTIVE_START_RESPONSE:
          packet = new NewActiveSetStartupPacket(json);
          NewActiveStartInfo info = ListenerReplicationPaxos.activeStartupInProgress.get(packet.getID());
          if (StartNameServer.debugMode) GNS.getLogger().fine("NEW_ACTIVE_START: received confirmation from "
                  + "node: " + packet.getSendingActive());
          if (info != null) {
            info.receivedResponseFromActive(packet.getSendingActive());
            if (info.haveMajorityActivesResponded()) {
              if (StartNameServer.debugMode) GNS.getLogger().fine("NEW_ACTIVE_START: received confirmation from majority. name = " + packet.getName());
              info.packet.changePacketTypeToConfirmation();
//              NameServer.tcpTransport.sendToID(info.packet.toJSONObject(),
//                      info.packet.getSendingPrimary(), GNS.PortType.PERSISTENT_TCP_PORT);
              NameServer.tcpTransport.sendToID(info.packet.getSendingPrimary(),info.packet.toJSONObject());
              ListenerReplicationPaxos.activeStartupInProgress.remove(packet.getID());
            }
          }
          break;
        case OLD_ACTIVE_STOP:
          OldActiveSetStopPacket oldActiveStopPacket = new OldActiveSetStopPacket(json);
          if (StartNameServer.debugMode) GNS.getLogger().fine("Received Old Active Stop Packet: " + json);
          String paxosID = oldActiveStopPacket.getPaxosIDToBeStopped();
          // if this is current active:

          NameRecord nameRecord1 = null;
          try {
            nameRecord1 = NameServer.getNameRecordMultiField(oldActiveStopPacket.getName(), getOldActiveStopFields());
          } catch (RecordNotFoundException e) {
            GNS.getLogger().fine("Record not found exception. Name = " + oldActiveStopPacket.getName());
            break;
          }
          try {
            if (StartNameServer.debugMode) GNS.getLogger().fine("NAME RECORD NOW: " + nameRecord1);
            int paxosStatus = nameRecord1.getPaxosStatus(paxosID);
            if (StartNameServer.debugMode) GNS.getLogger().fine("PaxosIDtoBeStopped = " + paxosID + " PaxosStatus = " + paxosStatus);
            if (paxosStatus == 1) { // this paxos ID is current active
              // propose STOP command for this paxos instance
              // Change Packet Type in oldActiveStop: This will help paxos identify that
              // this is a stop packet. See: PaxosManager.isStopCommand()
              oldActiveStopPacket.changePacketTypeToPaxosStop();
              // Put client ID = PAXOS_STOP.getInt() so that PaxosManager can route decision
              // to this class.
              PaxosManager.propose(paxosID, new RequestPacket(PacketType.ACTIVE_PAXOS_STOP.getInt(),
                      oldActiveStopPacket.toString(), PaxosPacketType.REQUEST, true));
              Long groupChangeStartTime = ReplicaController.groupChangeStartTimes.get(oldActiveStopPacket.getName());
              if (groupChangeStartTime != null) {
                long groupChangeDuration = System.currentTimeMillis()  - groupChangeStartTime;
                if (StartNameServer.debugMode)
                  GNS.getLogger().fine("\tStopActiveFinallyProposed\t" + oldActiveStopPacket.getName() + "\t" + groupChangeDuration+ "\t");
              }
              if (StartNameServer.debugMode) GNS.getLogger().fine("PAXOS PROPOSE: STOP Current Active Set. Paxos ID = " + paxosID);
            } else if (paxosStatus == 2) { // this is the old paxos ID
              // send confirmation to primary that this paxos ID is stopped.
              sendOldActiveStopConfirmationToPrimary(oldActiveStopPacket);
            } else {
              // if new active start packet comes before old active stop is committed, this situation might arise.
              GNS.getLogger().fine("PAXOS ID Neither current nor old. Ignore msg = " + paxosID);
            }
          } catch (FieldNotFoundException e) {
            GNS.getLogger().fine("FieldNotFoundException: " + e.getMessage());
            e.printStackTrace();
          }
          break;
        case ACTIVE_PAXOS_STOP:
          // STOP command is performed by paxos instance replica.
          oldActiveStopPacket = new OldActiveSetStopPacket(json);

          if (StartNameServer.debugMode) GNS.getLogger().fine("PAXOS DECISION: Old Active Stopped: Name = "
                  + oldActiveStopPacket.getName() + "\t" + oldActiveStopPacket);
          paxosID = oldActiveStopPacket.getPaxosIDToBeStopped();


          NameRecord nameRecord;
          try {
            nameRecord = NameServer.getNameRecordMultiField(oldActiveStopPacket.getName(),getActivePaxosStopFields());
          } catch (RecordNotFoundException e) {
            GNS.getLogger().fine("Record not found exception. Message = " + e.getMessage());
            break;
          }
          try{

            nameRecord.handleCurrentActiveStop(paxosID);

            sendOldActiveStopConfirmationToPrimary(oldActiveStopPacket);


          } catch (FieldNotFoundException e) {
            GNS.getLogger().fine("FieldNotFoundException. " + e.getMessage());
            e.printStackTrace();
          }
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
  private static  ArrayList<Field> activePaxosStopFields = new ArrayList<Field>();
  static ArrayList<Field> getActivePaxosStopFields() {
    synchronized (activePaxosStopFields) {
      if (activePaxosStopFields.size() > 0)return  activePaxosStopFields;
      activePaxosStopFields.add(NameRecord.ACTIVE_PAXOS_ID);
      activePaxosStopFields.add(NameRecord.VALUES_MAP);
      return activePaxosStopFields;
    }
  }

  private static ArrayList<Field> oldActiveStopFields = new ArrayList<Field>();

  private static ArrayList<Field> getOldActiveStopFields() {
    synchronized (oldActiveStopFields) {
      if (oldActiveStopFields.size() > 0) return  oldActiveStopFields;
      oldActiveStopFields.add(NameRecord.ACTIVE_PAXOS_ID);
      oldActiveStopFields.add(NameRecord.OLD_ACTIVE_PAXOS_ID);
      return oldActiveStopFields;
    }
  }

  private static ArrayList<Field> prevValueRequestFields = new ArrayList<Field>();

  private static ArrayList<Field> getPrevValueRequestFields() {
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
    if (oldActiveStopPacket.getActiveReceiver() == NameServer.nodeID) {
      Long groupChangeStartTime = ReplicaController.groupChangeStartTimes.get(oldActiveStopPacket.getName());
      if (groupChangeStartTime != null) {
        long groupChangeDuration = System.currentTimeMillis()  - groupChangeStartTime;
        if (StartNameServer.debugMode) GNS.getLogger().fine("\tStopActiveCompleted\t" + oldActiveStopPacket.getName() + "\t" + groupChangeDuration+ "\t");
      }
      // the active node who received this node, sends confirmation to primary
      // confirm to primary
      oldActiveStopPacket.changePacketTypeToConfirm();
//      NameServer.tcpTransport.sendToID(oldActiveStopPacket.toJSONObject(),
//              oldActiveStopPacket.getPrimarySender(),
//              GNS.PortType.PERSISTENT_TCP_PORT);
      NameServer.tcpTransport.sendToID(oldActiveStopPacket.getPrimarySender(), oldActiveStopPacket.toJSONObject());

      if (StartNameServer.debugMode) GNS.getLogger().fine("OLD ACTIVE STOP: Name Record Updated. Sent confirmation to primary."
              + " OldPaxosID = " + oldActiveStopPacket.getPaxosIDToBeStopped());
    } else {
      // other nodes do nothing.
      if (StartNameServer.debugMode) GNS.getLogger().fine("OLD ACTIVE STOP: Name Record Updated. OldPaxosID = "
              + oldActiveStopPacket.getPaxosIDToBeStopped());
    }
  }

  private  void addNameRecord(NewActiveSetStartupPacket originalPacket, ValuesMap previousValue, int ttl)
          throws  JSONException, IOException{
    try {

      int path = 0;
      try {
        NameRecord nameRecord = new NameRecord(originalPacket.getName(), originalPacket.getNewActiveNameServers(),
                originalPacket.getNewActivePaxosID(), previousValue, ttl);
        NameServer.addNameRecord(nameRecord);
        if (StartNameServer.debugMode) GNS.getLogger().fine(" NAME RECORD ADDED AT ACTIVE NODE: "
                + "name record = " + originalPacket.getName());
        boolean created = PaxosManager.createPaxosInstance(originalPacket.getNewActivePaxosID(),
                originalPacket.getNewActiveNameServers(), nameRecord.toString(), 0);
        if (created) {
          if (StartNameServer.debugMode) GNS.getLogger().fine(" PAXOS INSTANCE CREATED AT ACTIVE NAME SERVER. " + nameRecord.getName());
        } else {
          if (StartNameServer.debugMode) GNS.getLogger().fine(" PAXOS INSTANCE NOT CREATED. "  + nameRecord.getName());
        }
      } catch (RecordExistsException e) {
        path = 1;
        NameRecord nameRecord = null;

//        if (previousValue.containsKey(NameRecordKey.EdgeRecord.getName()))
        try {
          nameRecord = NameServer.getNameRecord(originalPacket.getName());
          nameRecord.handleNewActiveStart(originalPacket.getNewActiveNameServers(),
                  originalPacket.getNewActivePaxosID(), previousValue);
          boolean created = PaxosManager.createPaxosInstance(originalPacket.getNewActivePaxosID(),
                  originalPacket.getNewActiveNameServers(), nameRecord.toString(), 0);
          if (created) {
            if (StartNameServer.debugMode) GNS.getLogger().fine(" PAXOS INSTANCE CREATED AT ACTIVE NAME SERVER. " + nameRecord.getName());
          } else {
            if (StartNameServer.debugMode) GNS.getLogger().fine(" PAXOS INSTANCE NOT CREATED. "  + nameRecord.getName());
          }
        } catch (FieldNotFoundException e1) {
          GNS.getLogger().fine("Field not found exception: " + e.getMessage());
          e1.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        catch (RecordNotFoundException e1) {
          GNS.getLogger().fine("Not possible because record just existed.");
          e1.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        if (StartNameServer.debugMode) GNS.getLogger().fine(" NAME RECORD UPDATED AT ACTIVE  NODE. Name record = " + nameRecord);
      } catch (FieldNotFoundException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }

      //NameServer.getNameRecord(originalPacket.getName());
      //NameRecord nameRecord = DBNameRecord.getNameRecord(originalPacket.getName());
//
//      if (nameRecord == null) {
//        nameRecord = new NameRecord(originalPacket.getName());
//
//        nameRecord.handleNewActiveStart(originalPacket.getNewActiveNameServers(),
//                originalPacket.getNewActivePaxosID(), previousValue);
//        // first add name record, then create paxos instance for it.
//
//        NameServer.addNameRecord(nameRecord);
//
//        GNS.getLogger().fine(" NAME RECORD ADDED AT ACTIVE NODE: "
//                + "name record = " + nameRecord);
//      } else {
//        nameRecord.handleNewActiveStart(originalPacket.getNewActiveNameServers(),
//                originalPacket.getNewActivePaxosID(), previousValue);
//        NameServer.updateNameRecord(nameRecord);
//        //DBNameRecord.updateNameRecord(nameRecord);
//        GNS.getLogger().fine(" NAME RECORD UPDATED AT ACTIVE  NODE. "
//                + "name record = " + nameRecord);
//      }
//      // put the previous value obtained in the name record.
//      GNS.getLogger().fine(" NEW_ACTIVE_START_PREV_VALUE_RESPONSE. "
//              + "Name Record Value: " + nameRecord);
//      // fire up paxos instance

      // send reply to main active
      originalPacket.changePacketTypeToResponse();
      int sendingActive = originalPacket.getSendingActive();
      originalPacket.changeSendingActive(NameServer.nodeID);
//      NameRecord nameRecord2 = NameServer.getNameRecord(originalPacket.getName());
//      if (nameRecord2.containsActiveNameServer(NameServer.nodeID)) {
//        GNS.getLogger().fine("NameRecord contains active name server " + nameRecord2.getName() + "\t"
//                + NameServer.nodeID + "\tNameRecord = " + nameRecord2.toString()+ "\tPath = " + path + "\tPacket = " + originalPacket);
//      }
//      else {
//        GNS.getLogger().fine("NameRecord does not contains active name server " + nameRecord2.getName() + "\t"
//                + NameServer.nodeID + "\tNameRecord = " + nameRecord2.toString() + "\tPath = " + path + "\tPacket = " + originalPacket);
//        System.exit(2);
//      }
//
//      if (nameRecord2.containsKey(NameRecordKey.EdgeRecord.getName())) {
//        GNS.getLogger().fine("NameRecord  contains key edge record " + nameRecord2.getName()
//                + " Values Map = "  + nameRecord2.getValuesMap() + "\tNameRecord = " + nameRecord2.toString() + "\t Path = " + path  + " \tPrevious Value = " + previousValue);
//      }
//      else {
//        GNS.getLogger().fine("NameRecord  does not contains key edge record " + nameRecord2.getName()
//                + " Values Map = "  + nameRecord2.getValuesMap()
//                + "\tNameRecord = " + nameRecord2.toString() + "\t Path = " + path  + " \tPrevious Value = " + previousValue);
//        System.exit(2);
//      }
      if (StartNameServer.debugMode) GNS.getLogger().fine("NEW_ACTIVE_START: replied to active sending the startup packet from node: " + sendingActive);
//      NameServer.tcpTransport.sendToID(originalPacket.toJSONObject(), sendingActive,
//              GNS.PortType.PERSISTENT_TCP_PORT);
      NameServer.tcpTransport.sendToID(sendingActive, originalPacket.toJSONObject());
    } catch (Exception e) {
      GNS.getLogger().fine(" Exception Exception Exception: ****************");
      e.getMessage();
      e.printStackTrace();
    }
  }



//	/**
//	 *
//	 * @param packet
//	 * @throws JSONException
//	 */
//	private void requestValuesFromOldActives(NewActiveSetStartupPacket packet) throws JSONException {
//		GNRS.getLogger().fine(" NEW_ACTIVE_START_FORWARD received packet: " + packet.toJSONObject());
//		// do book-keeping for this packet
//		ListenerReplicationPaxos.activeStartupPacketsReceived.put(packet.getID(), packet);
//
//		// make a copy
//		NewActiveSetStartupPacket packet2 = new NewActiveSetStartupPacket(packet.toJSONObject());
//		// select old active to send request to
//		int oldActive = selectOldActiveToRequestPreviousValue(packet2.getOldActiveNameServers());
//
//		GNRS.getLogger().fine(" OLD ACTIVE SELECTED = : " + oldActive);
//		// change packet type
//		packet2.changePacketTypeToPreviousValueRequest();
//		// change sending active
//		packet2.changeSendingActive(NameServer.nodeID);
//
//
//		// send this packet to obtain previous value.
//		try
//		{
//			NameServer.tcpTransport.sendToID(packet2.toJSONObject(), oldActive, GNRS.PortType.PERSISTENT_TCP_PORT);
//		} catch (IOException e)
//		{
//			GNRS.getLogger().fine(" IOException here: " + e.getMessage());
//			e.printStackTrace();
//		} catch (JSONException e)
//		{
//			GNRS.getLogger().fine(" JSONException here: " + e.getMessage());
//			e.printStackTrace();
//		}
//		GNRS.getLogger().fine(" REQUESTED VALUE from OLD ACTIVE. PACKET: " + packet2);
//		// TODO retransmit.
//
//	}
//	/**
//	 *
//	 * @param oldActives
//	 * @return
//	 */
//	private int selectOldActiveToRequestPreviousValue(Set<Integer> oldActives) {
//		if (oldActives.contains(NameServer.nodeID)) return NameServer.nodeID;
//		// choose any for now.
//		for (int x: oldActives) {
//			return x;
//		}
//		// TODO: choose one with lowest latency
//		return -1;
//	}
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
    if (sent == false && packet.getNewActiveNameServers().size() == activesResponded.size()) {
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
      if (StartNameServer.debugMode) GNS.getLogger().fine(" NEW_ACTIVE_START_FORWARD received packet: " + packet.toJSONObject());
      if (oldActivesQueried.size() == 0) {
        ListenerReplicationPaxos.activeStartupPacketsReceived.put(packet.getID(), packet);
      }

      if (!ListenerReplicationPaxos.activeStartupPacketsReceived.containsKey(packet.getID())) {
        if (StartNameServer.debugMode) GNS.getLogger().fine(" COPY State from Old Active Successful! Cancel Task; Actives Queried: " + oldActivesQueried);
        this.cancel();
        return;
      }

      // make a copy
      NewActiveSetStartupPacket packet2 = new NewActiveSetStartupPacket(packet.toJSONObject());
      // select old active to send request to
      int oldActive = BestServerSelection.getSmallestLatencyNS(packet.getOldActiveNameServers(), oldActivesQueried);

      if (oldActive == -1) {
        GNS.getLogger().fine(" ERROR:  No More Actives Left To Query. Cancel Task!!!");
        this.cancel();
        return;
      }
      oldActivesQueried.add(oldActive);
      if (StartNameServer.debugMode) GNS.getLogger().fine(" OLD ACTIVE SELECTED = : " + oldActive);
      // change packet type
      packet2.changePacketTypeToPreviousValueRequest();
      // change sending active
      packet2.changeSendingActive(NameServer.nodeID);

      try {
//        NameServer.tcpTransport.sendToID(packet2.toJSONObject(), oldActive, GNS.PortType.PERSISTENT_TCP_PORT);
        NameServer.tcpTransport.sendToID(oldActive, packet2.toJSONObject());
      } catch (IOException e) {
        GNS.getLogger().fine(" IOException here: " + e.getMessage());
        e.printStackTrace();
      } catch (JSONException e) {
        GNS.getLogger().fine(" JSONException here: " + e.getMessage());
        e.printStackTrace();
      }
      if (StartNameServer.debugMode) GNS.getLogger().fine(" REQUESTED VALUE from OLD ACTIVE. PACKET: " + packet2);

    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }
}