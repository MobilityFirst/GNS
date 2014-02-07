/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved
 */
package edu.umass.cs.gns.nameserver;

import edu.umass.cs.gns.client.UpdateOperation;
import edu.umass.cs.gns.clientprotocol.Defs;
import edu.umass.cs.gns.database.BasicRecordCursor;
import edu.umass.cs.gns.database.ColumnField;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaController;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaControllerRecord;
import edu.umass.cs.gns.packet.*;
import edu.umass.cs.gns.packet.paxospacket.PaxosPacketType;
import edu.umass.cs.gns.packet.paxospacket.RequestPacket;
import edu.umass.cs.gns.paxos.PaxosManager;
import edu.umass.cs.gns.util.BestServerSelection;
import edu.umass.cs.gns.util.HashFunction;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Handle client requests - ADD/REMOVE/LOOKUP/UPDATE + REQUESTACTIVES
 *
 * @author abhigyan
 */
public class ClientRequestWorker extends TimerTask {

  JSONObject incomingJSON;
  Packet.PacketType packetType;
  private static ConcurrentHashMap<Integer, UpdateStatus> addInProgress = new ConcurrentHashMap<Integer, UpdateStatus>();
//  private static ConcurrentHashMap<Integer, ConfirmUpdateLNSPacket> proposedUpdates = new ConcurrentHashMap<Integer, ConfirmUpdateLNSPacket>();
  /**
   * ID assigned to updates received from LNS. The next update from a LNS will be assigned id = updateIDCount + 1;
   */
  private static Integer updateIDcount = 0;
  private static final Object lock = new ReentrantLock();

  static int incrementUpdateID() {
    synchronized (lock) {
      return ++updateIDcount;
    }
  }

  public ClientRequestWorker(JSONObject json, Packet.PacketType packetType) {
    this.packetType = packetType;
    this.incomingJSON = json;
  }

  public static void handleIncomingPacket(JSONObject json, Packet.PacketType packetType) {
    NameServer.executorService.submit(new ClientRequestWorker(json, packetType));
  }

  @Override
  public void run() {
    //
    long t0 = System.currentTimeMillis();
    try {
      switch (packetType) {
        case DNS:
          // LOOKUP
          handleDNSPacket();
          break;
        case UPDATE_ADDRESS_LNS:
          handleUpdateAddressLNS();
          break;
        // we are not handling UPDATE_ADDRESS_NS msg type through switch-case because this case handleUpdateAddressNS
        case UPDATE_ADDRESS_NS:
          if (StartNameServer.eventualConsistency) {
            handleUpdateAddressNS(incomingJSON);
          }
          break;

        // Request current actives
        case REQUEST_ACTIVES:
          handleRequestActivesPacket();
          break;
        case NAME_SERVER_LOAD:
          handleNameServerLoadPacket();
          break;
        // ADD
        case ADD_RECORD_LNS:
          // receive from LNS, and send to other primaries
          handleAddRecordLNSPacket();
          break;
//        case ADD_RECORD_NS:
//          // receive from primary which received the request
//          handleAddRecordNS();
//          break;
//        case CONFIRM_ADD_NS:  // ***NEW***
//          // receive from primary which received the request
//          handleConfirmAddNS();
//          break;
//        case ADD_COMPLETE:
//          handleAddCompletePacket();
//          break;

        // REMOVE
        case REMOVE_RECORD_LNS:
          ReplicaController.handleNameRecordRemoveRequestAtPrimary(incomingJSON);
          break;

        // SELECT
        case SELECT_REQUEST:
          Select.handleSelectRequest(incomingJSON);
          break;
        case SELECT_RESPONSE:
          Select.handleSelectResponse(incomingJSON);
          break;
      }
    } catch (JSONException e) {
      GNS.getLogger().severe("JSON Exception in client request worker. " + e.getMessage());
      e.printStackTrace();
    } catch (IOException e) {
      GNS.getLogger().severe("IO Exception in client request worker. " + e.getMessage());
      e.printStackTrace();
    } catch (Exception e) {
      GNS.getLogger().severe("Exception in client request worker. " + e.getMessage());
      e.printStackTrace();
    }
    long t1 = System.currentTimeMillis();
    if (t1 - t0 > 100) {
      GNS.getLogger().severe("Long delay " + (t1 - t0) + "ms. Packet: " + incomingJSON);
    }
  }

  private void handleNameServerLoadPacket() throws JSONException {
    NameServerLoadPacket nsLoad = new NameServerLoadPacket(incomingJSON);
    nsLoad.setLoadValue(NameServer.loadMonitor.getAverage());
    NameServer.sendToLNS(nsLoad.toJSONObject(), nsLoad.getLnsID());
  }
  //
  private static ArrayList<ColumnField> addRecordLNSFields = new ArrayList<ColumnField>();

  private static ArrayList<ColumnField> getAddRecordLNSFields() {
    synchronized (addRecordLNSFields) {
      if (addRecordLNSFields.size() > 0) {
        return addRecordLNSFields;
      }
      addRecordLNSFields.add(ReplicaControllerRecord.MARKED_FOR_REMOVAL);
      return addRecordLNSFields;
    }
  }

  private void handleAddRecordLNSPacket() throws JSONException, IOException {

    AddRecordPacket addRecordPacket;
    String name;
    NameRecordKey nameRecordKey;
    ResultValue value;
    addRecordPacket = new AddRecordPacket(incomingJSON);
    name = addRecordPacket.getName();
    nameRecordKey = addRecordPacket.getRecordKey();
    value = addRecordPacket.getValue();

    GNS.getLogger().info(" ADD FROM LNS (ns " + NameServer.nodeID + ") : " + name + "/" + nameRecordKey.toString() + ", " + value);

    ConfirmUpdateLNSPacket confirmPacket = new ConfirmUpdateLNSPacket(true, addRecordPacket);

    Set<Integer> primaryNameServers = HashFunction.getPrimaryReplicas(addRecordPacket.getName());
    UpdateStatus status = new UpdateStatus(addRecordPacket.getName(), addRecordPacket.getLocalNameServerID(), primaryNameServers, confirmPacket);
    status.addNameServerResponded(NameServer.nodeID);

    Random r = new Random();
    int nsReqID = r.nextInt(); // request ID assigned by this name server
    addInProgress.put(nsReqID, status);

    // prepare add record packet to be sent to other replica controllers
    GNS.getLogger().info(" NS Put request ID as " + nsReqID + " LNS request ID is " + confirmPacket.getLNSRequestID());
    addRecordPacket.setLNSRequestID(nsReqID);
    addRecordPacket.setLocalNameServerID(NameServer.nodeID);
    addRecordPacket.setType(Packet.PacketType.ADD_RECORD_NS);

    RequestPacket requestPacket = new RequestPacket(Packet.PacketType.ADD_RECORD_NS.getInt(),
            addRecordPacket.toString(), PaxosPacketType.REQUEST, false);
    String primaryPaxosID = ReplicaController.getPrimaryPaxosID(name);
    PaxosManager.propose(primaryPaxosID, requestPacket);

  }

  public static void handleAddRecordNS(JSONObject incomingJSON) throws JSONException, IOException {

    AddRecordPacket addRecordPacket;
    String name;
    NameRecordKey nameRecordKey;
    ResultValue value;
    addRecordPacket = new AddRecordPacket(incomingJSON);
    name = addRecordPacket.getName();
    nameRecordKey = addRecordPacket.getRecordKey();
    value = addRecordPacket.getValue();
    GNS.getLogger().info(" ADD FROM NS (ns " + NameServer.nodeID + ") : " + name + "/" + nameRecordKey.toString() + ", " + value);

    ReplicaControllerRecord rcRecord = new ReplicaControllerRecord(name, true);//NameServer.getNameRecord(name);

    try {
      NameServer.addNameRecordPrimary(rcRecord);

      ValuesMap valuesMap = new ValuesMap();
      valuesMap.put(addRecordPacket.getRecordKey().getName(), addRecordPacket.getValue());
      try {
        NameRecord nameRecord = new NameRecord(name, rcRecord.getActiveNameservers(), rcRecord.getActivePaxosID(),
                valuesMap, addRecordPacket.getTTL());
        try {
          NameServer.addNameRecord(nameRecord);
        } catch (RecordExistsException e) {
          GNS.getLogger().severe("ERROR: Exception: name record exists but replica controller does not exist. This should never happen ");
          e.printStackTrace();
        }
        ListenerReplicationPaxos.createPaxosInstanceForName(rcRecord.getName(), rcRecord.getActiveNameservers(),
                rcRecord.getActivePaxosID(), valuesMap, 0, addRecordPacket.getTTL());
        GNS.getLogger().info(" Active-paxos and name record created. Name = " + rcRecord.getName());
        UpdateStatus status = addInProgress.remove(addRecordPacket.getLNSRequestID());
        if (status != null) {
          NameServer.sendToLNS(status.getConfirmUpdateLNSPacket().toJSONObject(), status.getLocalNameServerID());
        } else {
          GNS.getLogger().info(" Status record missing for Name = " + rcRecord.getName() + " request id: " + addRecordPacket.getLNSRequestID());
        }
      } catch (FieldNotFoundException e) {
        GNS.getLogger().info("Field not found exception. Should not happen because we initialized all fields in record. " + e.getMessage());
        e.printStackTrace();
      }


    } catch (RecordExistsException e) {
      UpdateStatus status = addInProgress.remove(addRecordPacket.getLNSRequestID());
      if (status != null) {
        // send failure
        ConfirmUpdateLNSPacket confirmPkt = status.getConfirmUpdateLNSPacket();
        confirmPkt.convertToFailPacket();
        NameServer.sendToLNS(confirmPkt.toJSONObject(), status.getLocalNameServerID());
        GNS.getLogger().info("Record already exists ... sent error to client" + e.getMessage());
      } else {
        GNS.getLogger().info(" Status record missing for request id: " + addRecordPacket.getLNSRequestID());
      }
      GNS.getLogger().info("Record already exists ... continue " + e.getMessage());
    }

  }

  private void handleUpdateAddressLNS() throws JSONException, IOException {
//    long t0 = System.currentTimeMillis();
//    if (StartNameServer.debugMode) {
//      GNS.getLogger().severe(" Recvd Update Address from LNS: " + incomingJSON);
//    }

    UpdateAddressPacket updatePacket = new UpdateAddressPacket(incomingJSON);


    if (updatePacket.getOperation().isUpsert()) {
      handleUpsert(updatePacket);

    } else {
      handleUpdate(updatePacket);
    }
//    long t1 = System.currentTimeMillis();
//    NameServer.loadMonitor.add((int)(t1 - t0));
  }

  /**
   * Handles the upsert case of UpdateAddressPacket
   *
   * @param updatePacket
   * @throws JSONException
   * @throws IOException
   */
  private void handleUpsert(UpdateAddressPacket updatePacket) throws JSONException, IOException {
    // this must be primary
    ReplicaControllerRecord nameRecordPrimary = null;
    try {
      nameRecordPrimary = NameServer.getNameRecordPrimaryMultiField(updatePacket.getName(),
              ReplicaControllerRecord.MARKED_FOR_REMOVAL, ReplicaControllerRecord.ACTIVE_NAMESERVERS);
      try {
        if (nameRecordPrimary.isMarkedForRemoval()) {
          ConfirmUpdateLNSPacket failConfirmPacket =
                  ConfirmUpdateLNSPacket.createFailPacket(updatePacket);
          NameServer.tcpTransport.sendToID(updatePacket.getLocalNameServerId(), failConfirmPacket.toJSONObject());
          if (StartNameServer.debugMode) {
            GNS.getLogger().fine(" UPSERT-FAILED because name record deleted already\t" + updatePacket.getName()
                    + "\t" + NameServer.nodeID + "\t" + updatePacket.getLocalNameServerId());// + "\t" + updatePacket.getSequenceNumber());
          }
        }
      } catch (FieldNotFoundException e) {
        GNS.getLogger().fine("Field not found exception. " + e.getMessage());
        e.printStackTrace();
      }

      // record does not exist, so we can do an ADD
      int activeID = -1;
      Set<Integer> activeNS;
      try {
        activeNS = nameRecordPrimary.getActiveNameservers();
      } catch (FieldNotFoundException e1) {
        GNS.getLogger().fine("Field not found exception. " + e1.getMessage());
        e1.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        return;
      }
      if (activeNS != null) {
        activeID = BestServerSelection.getSmallestLatencyNS(activeNS, null);
      }

      if (activeID != -1) {
        // forward update to active NS
        // updated to use a less kludgey operation - Westy
        if (updatePacket.getOperation().isUpsert()) {
          updatePacket.setOperation(updatePacket.getOperation().getNonUpsertEquivalent());
        }
        if (StartNameServer.debugMode) {
          GNS.getLogger().fine("UPSERT forwarded as UPDATE to active: " + activeID);
        }
        NameServer.tcpTransport.sendToID(activeID, updatePacket.toJSONObject());
        // could not find activeNS for this name
      } else {
        // send error to LNS
        ConfirmUpdateLNSPacket failConfirmPacket =
                ConfirmUpdateLNSPacket.createFailPacket(updatePacket);
        NameServer.tcpTransport.sendToID(updatePacket.getLocalNameServerId(), failConfirmPacket.toJSONObject());
//          NSListenerUDP.udpTransport.sendPacket(failConfirmPacket.toJSONObject(),
//                  updatePacket.getLocalNameServerId(), GNS.PortType.LNS_UDP_PORT);
        String msg = " UPSERT-FAILED\t" + updatePacket.getName()
                + "\t" + NameServer.nodeID + "\t" + updatePacket.getLocalNameServerId();// + "\t" + updatePacket.getSequenceNumber();
        if (StartNameServer.debugMode) {
          GNS.getLogger().fine(msg);
        }
      }
    } catch (RecordNotFoundException e) {
      // do an INSERT (AKA ADD) operation

      AddRecordPacket addRecordPacket = new AddRecordPacket(updatePacket.getRequestID(), updatePacket.getName(),
              updatePacket.getRecordKey(), updatePacket.getUpdateValue(), updatePacket.getLocalNameServerId(),
              updatePacket.getTTL()); // is getTTL() only used with upsert?
      addRecordPacket.setLNSRequestID(updatePacket.getLNSRequestID());
      incomingJSON = addRecordPacket.toJSONObject();
      handleAddRecordLNSPacket();
      if (StartNameServer.debugMode) {
        GNS.getLogger().fine(" NS processing UPSERT changed to ADD: " + incomingJSON);
      }
    }
  }
  private static ArrayList<ColumnField> lnsUpdateFields = new ArrayList<ColumnField>();

  private static ArrayList<ColumnField> getLNSUpdateFields() {
    synchronized (lnsUpdateFields) {
      if (lnsUpdateFields.size() == 0) {
        lnsUpdateFields.add(NameRecord.ACTIVE_NAMESERVERS);
        lnsUpdateFields.add(NameRecord.ACTIVE_PAXOS_ID);
      }
      return lnsUpdateFields;
    }
  }

//  /**
//   * Handles the update case of UpdateAddressPacket
//   * @param updatePacket
//   * @throws JSONException
//   * @throws IOException
//   */
//  private void handleUpdate(UpdateAddressPacket updatePacket) throws JSONException,IOException{
//    // HANDLE NON-UPSERT CASE
//
//    //NameRecord nameRecord = NameServer.getNameRecord(updatePacket.getName());
////    ArrayList<Field> lnsUpdateFields = new ArrayList<Field>();
//
//
//
//    NameRecord nameRecord;
//    try {
//      nameRecord = NameServer.getNameRecordMultiField(updatePacket.getName(), getLNSUpdateFields(), null);
//    } catch (RecordNotFoundException e) {
//      if (StartNameServer.debugMode) GNS.getLogger().fine("Record not found Exception. Returned error to client. Name =\t" +updatePacket.getName());
////      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//      return;
//    }
//
//    try{
//      //NameRecord nameRecord = NameServer.getNameRecordLazy(updatePacket.getName());
//      //NameRecord nameRecord = DBNameRecord.getNameRecord(updatePacket.getName());
//
//      if (nameRecord == null || !nameRecord.containsActiveNameServer(NameServer.nodeID)) {
//        ConfirmUpdateLNSPacket failConfirmPacket =
//                ConfirmUpdateLNSPacket.createFailPacket(updatePacket, NameServer.nodeID);
//        // inform LNS of failed request
//        NameServer.tcpTransport.sendToID(updatePacket.getLocalNameServerId(),failConfirmPacket.toJSONObject());
//        //        NSListenerUDP.udpTransport.sendPacket(failConfirmPacket.toJSONObject(),
//        //                updatePacket.getLocalNameServerId(), GNS.PortType.LNS_UDP_PORT);
//        if (StartNameServer.debugMode) GNS.getLogger().fine(" UpdateRequest-InvalidNameServer\t" + updatePacket.getName()
//                + "\t" + NameServer.nodeID + "\t" + updatePacket.getLocalNameServerId() + "\t" + updatePacket.getSequenceNumber());
//      } else {
//
//        if (StartNameServer.debugMode) GNS.getLogger().fine(" PAXOS PROPOSAL: propose update request: " + updatePacket.toString());
//
//        // Save request info to send confirmation to LNS later.
//        updatePacket.setNameServerId(NameServer.nodeID); // to check that I had proposed this message
//        int updateID = incrementUpdateID();
//        updatePacket.setNSRequestID(updateID); // to check the request ID.
//        updatePacket.setType(Packet.PacketType.UPDATE_ADDRESS_NS); //
//        ConfirmUpdateLNSPacket confirmPacket = ConfirmUpdateLNSPacket.createSuccessPacket(updatePacket,
//                NameServer.nodeID, nameRecord.getActiveNameServers().size());
//        proposedUpdates.put(updateID, confirmPacket);
//        //        proposedUpdatesTime.put(updateID, System.currentTimeMillis());
//        if (StartNameServer.debugMode) {
//          GNS.getLogger().fine(" Update Packet : " + updatePacket);
//        }
//        // Propose to paxos
//        String activePaxosID = nameRecord.getActivePaxosID();
//        if (StartNameServer.debugMode) {
//          GNS.getLogger().fine(" Update proposed to paxosID = " + activePaxosID);
//        }
//        PaxosManager.propose(activePaxosID, new RequestPacket(updatePacket.getType().getInt(),
//                updatePacket.toString(), PaxosPacketType.REQUEST, false));
//        if (StartNameServer.debugMode) GNS.getLogger().fine(" Update Packet Type : " + updatePacket.getType().getInt());
//      }
////        long t1 = System.currentTimeMillis();
////        if (t1 - t0 > 50) {
////          if (StartNameServer.debugMode) {
////            GNS.getLogger().severe("UpdateLongDelay = " + (t1 - t0) + " in handleUpdateAddressNS UpdatePacket = "
////                    + updatePacket + " NameRecord = " + nameRecord);
////          }
////        }
//    }catch (FieldNotFoundException e) {
//      GNS.getLogger().fine(" FieldNotFoundException: " + e.getMessage());
//      e.printStackTrace();
//    }
//  }
  /**
   * Handles the update case of UpdateAddressPacket
   *
   * @param updatePacket
   * @throws JSONException
   * @throws IOException
   */
  private void handleUpdate(UpdateAddressPacket updatePacket) throws JSONException, IOException {
    long t0 = System.currentTimeMillis();
    // HANDLE NON-UPSERT CASE

    //NameRecord nameRecord = NameServer.getNameRecord(updatePacket.getName());
//    ArrayList<Field> lnsUpdateFields = new ArrayList<Field>();

    if (StartNameServer.debugMode) {
      GNS.getLogger().fine(" Update Packet Type: " + updatePacket.getType().getInt());
    }

    // Save request info to send confirmation to LNS later.
    updatePacket.setNameServerId(NameServer.nodeID); // to check that I had proposed this message
//    int updateID = incrementUpdateID();
//    updatePacket.setNSRequestID(updateID); // to check the request ID.
    updatePacket.setType(Packet.PacketType.UPDATE_ADDRESS_NS); //
//    ConfirmUpdateLNSPacket confirmPacket = ConfirmUpdateLNSPacket.createSuccessPacket(updatePacket);
    if (StartNameServer.eventualConsistency) {
      boolean sendFailure = false;
      ArrayList<ColumnField> fields = new ArrayList<ColumnField>();
      fields.add(NameRecord.ACTIVE_NAMESERVERS);
      try {
        NameRecord nameRecord = NameServer.getNameRecordMultiField(updatePacket.getName(), fields);
        try {
          if (nameRecord.containsActiveNameServer(NameServer.nodeID)) {
            NameServer.tcpTransport.sendToIDs(nameRecord.getActiveNameServers(), updatePacket.toJSONObject());
          } else {
            sendFailure = true;
          }
        } catch (FieldNotFoundException e) {
          e.printStackTrace();
        }
      } catch (RecordNotFoundException e) {
        sendFailure = true;

      }
      if (sendFailure) {
        ConfirmUpdateLNSPacket failConfirmPacket = ConfirmUpdateLNSPacket.createFailPacket(updatePacket);
        // inform LNS of failed request
        NameServer.sendToLNS(failConfirmPacket.toJSONObject(), updatePacket.getLocalNameServerId());

        if (StartNameServer.debugMode) {
          GNS.getLogger().fine(" UpdateRequest-InvalidNameServer\t" + updatePacket.getName()
                  + "\t" + NameServer.nodeID + "\t" + updatePacket.getLocalNameServerId());// + "\t" + updatePacket.getSequenceNumber());
        }
      }

      return;
    }
//    proposedUpdates.put(updateID, confirmPacket);

    if (StartNameServer.debugMode) {
      GNS.getLogger().fine(" Update Packet : " + updatePacket);
    }

    // Propose to paxos

    String activePaxosID = updatePacket.getName(); // nameRecord.getActivePaxosID();
//    if (StartNameServer.debugMode) {
//      GNS.getLogger().severe(" Update proposed to paxosID = " + activePaxosID);
//    }

    String paxosID = PaxosManager.propose(activePaxosID, new RequestPacket(updatePacket.getType().getInt(),
            updatePacket.toString(), PaxosPacketType.REQUEST, false));
//    GNS.getLogger().severe(" PAXOS PROPOSAL: propose update request: " + updatePacket + "result = " + paxosID);

    if (paxosID == null) {
      ConfirmUpdateLNSPacket failConfirmPacket = ConfirmUpdateLNSPacket.createFailPacket(updatePacket);
      // inform LNS of failed request
      NameServer.sendToLNS(failConfirmPacket.toJSONObject(), updatePacket.getLocalNameServerId());

      if (StartNameServer.debugMode) {
        GNS.getLogger().fine(" UpdateRequest-InvalidNameServer\t" + updatePacket.getName()
                + "\t" + NameServer.nodeID + "\t" + updatePacket.getLocalNameServerId());// + "\t" + updatePacket.getSequenceNumber());
      }
    }
    long t1 = System.currentTimeMillis();

    if (t1 - t0 > 10) {
      GNS.getLogger().warning("Long latency HandleUpdate " + (t1 - t0));
    }


//    NameRecord nameRecord;
//    try {
//      nameRecord = NameServer.getNameRecordMultiField(updatePacket.getName(), getLNSUpdateFields(), null);
//    } catch (RecordNotFoundException e) {
//      if (StartNameServer.debugMode) GNS.getLogger().fine("Record not found Exception. Returned error to client. Name =\t" +updatePacket.getName());
////      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//      return;
//    }
//
//    try{
//      //NameRecord nameRecord = NameServer.getNameRecordLazy(updatePacket.getName());
//      //NameRecord nameRecord = DBNameRecord.getNameRecord(updatePacket.getName());
//
//      if (nameRecord == null || !nameRecord.containsActiveNameServer(NameServer.nodeID)) {
//        ConfirmUpdateLNSPacket failConfirmPacket =
//                ConfirmUpdateLNSPacket.createFailPacket(updatePacket, NameServer.nodeID);
//        // inform LNS of failed request
//        NameServer.tcpTransport.sendToID(updatePacket.getLocalNameServerId(),failConfirmPacket.toJSONObject());
//        //        NSListenerUDP.udpTransport.sendPacket(failConfirmPacket.toJSONObject(),
//        //                updatePacket.getLocalNameServerId(), GNS.PortType.LNS_UDP_PORT);
//        if (StartNameServer.debugMode) GNS.getLogger().fine(" UpdateRequest-InvalidNameServer\t" + updatePacket.getName()
//                + "\t" + NameServer.nodeID + "\t" + updatePacket.getLocalNameServerId() + "\t" + updatePacket.getSequenceNumber());
//      } else {
//
//        if (StartNameServer.debugMode) GNS.getLogger().fine(" PAXOS PROPOSAL: propose update request: " + updatePacket.toString());
//
//        // Save request info to send confirmation to LNS later.
//        updatePacket.setNameServerId(NameServer.nodeID); // to check that I had proposed this message
//        int updateID = incrementUpdateID();
//        updatePacket.setNSRequestID(updateID); // to check the request ID.
//        updatePacket.setType(Packet.PacketType.UPDATE_ADDRESS_NS); //
//        ConfirmUpdateLNSPacket confirmPacket = ConfirmUpdateLNSPacket.createSuccessPacket(updatePacket,
//                NameServer.nodeID, nameRecord.getActiveNameServers().size());
//        proposedUpdates.put(updateID, confirmPacket);
//        //        proposedUpdatesTime.put(updateID, System.currentTimeMillis());
//        if (StartNameServer.debugMode) {
//          GNS.getLogger().fine(" Update Packet : " + updatePacket);
//        }
//        // Propose to paxos
//        String activePaxosID = nameRecord.getActivePaxosID();
//        if (StartNameServer.debugMode) {
//          GNS.getLogger().fine(" Update proposed to paxosID = " + activePaxosID);
//        }
//        PaxosManager.propose(activePaxosID, new RequestPacket(updatePacket.getType().getInt(),
//                updatePacket.toString(), PaxosPacketType.REQUEST, false));
//        if (StartNameServer.debugMode) GNS.getLogger().fine(" Update Packet Type : " + updatePacket.getType().getInt());
//      }
////        long t1 = System.currentTimeMillis();
////        if (t1 - t0 > 50) {
////          if (StartNameServer.debugMode) {
////            GNS.getLogger().severe("UpdateLongDelay = " + (t1 - t0) + " in handleUpdateAddressNS UpdatePacket = "
////                    + updatePacket + " NameRecord = " + nameRecord);
////          }
////        }
//    }catch (FieldNotFoundException e) {
//      GNS.getLogger().fine(" FieldNotFoundException: " + e.getMessage());
//      e.printStackTrace();
//    }
  }

  /**
   * When Paxos commits an address update, this method inserts the updated address in DB at each active replica.
   *
   * @throws JSONException
   * @throws IOException
   */
  public static void handleUpdateAddressNS(JSONObject incomingJSON) throws JSONException, IOException {

//    long t0 = System.currentTimeMillis();
    if (StartNameServer.debugMode) {
      GNS.getLogger().fine("PAXOS DECISION: Update Confirmed ...   " + incomingJSON);
    }
    UpdateAddressPacket updatePacket = new UpdateAddressPacket(incomingJSON);
//    GNS.getLogger().severe("PAXOS DECISION: Update Confirmed Name " + updatePacket);
    NameRecord nameRecord;

    if (updatePacket.getOperation().equals(UpdateOperation.REPLACE_ALL)) { // we don't need to read for replace-all
      nameRecord = new NameRecord(updatePacket.getName());
    } else {
      try {
        nameRecord = NameServer.getNameRecordMultiField(updatePacket.getName(), null, updatePacket.getRecordKey().getName());
//      GNS.getLogger().severe("reading name record for update.  Operation: " +  updatePacket.getOperation());
      } catch (RecordNotFoundException e) {
        GNS.getLogger().severe(" Error: name record not found before update. Return. Name = " + updatePacket.getName());
//      e.printStackTrace();
        return;
      }
    }
    // Apply update
    if (StartNameServer.debugMode) {
      GNS.getLogger().fine("NAME RECORD is: " + nameRecord.toString());
    }
    boolean result;
    try {
      result = nameRecord.updateKey(updatePacket.getRecordKey().getName(), updatePacket.getUpdateValue(),
              updatePacket.getOldValue(), updatePacket.getOperation());

      if (StartNameServer.debugMode) {
        GNS.getLogger().fine("Update operation result = " + result + "\t"
                + updatePacket.getUpdateValue());
      }

//      try {
//        NameRecord nameRecord2 = NameServer.getNameRecordMultiField(updatePacket.getName(), null, updatePacket.getRecordKey().getName());
//        if (StartNameServer.debugMode) {
//          GNS.getLogger().fine("NAME RECORD after Update is: " + nameRecord2.toString());
//        }
//      } catch (RecordNotFoundException e) {
//        GNS.getLogger().fine(" Exception: name record not found. Name = " + updatePacket.getName());
//        e.printStackTrace();
//        return;
//      }
//      tB = System.currentTimeMillis();

      if (!result) { // update failed
        if (StartNameServer.debugMode) {
          GNS.getLogger().fine("Update operation failed " + incomingJSON);
        }
        if (updatePacket.getNameServerId() == NameServer.nodeID) { //if this node proposed this update
          // send error message to client
          ConfirmUpdateLNSPacket failPacket = new ConfirmUpdateLNSPacket(Packet.PacketType.CONFIRM_UPDATE_LNS,
                  updatePacket.getRequestID(), updatePacket.getLNSRequestID(), false);
////          ConfirmUpdateLNSPacket confirmUpdateLNSPacket = proposedUpdates.remove(updatePacket.getNSRequestID());
//          confirmUpdateLNSPacket.convertToFailPacket();
          // for small packets use UDP
          NameServer.sendToLNS(failPacket.toJSONObject(), updatePacket.getLocalNameServerId());

          if (StartNameServer.debugMode) {
            GNS.getLogger().fine("Error msg sent to client for failed update " + incomingJSON);
          }
        }
        return;
      }

      boolean msgLNS = false;
      if (StartNameServer.debugMode) {
        GNS.getLogger().fine("Update applied" + incomingJSON);
      }

//      if (StartNameServer.experimentMode) {
//        int nameInt = Integer.parseInt(updatePacket.getName());
//        Random random = new Random(nameInt);
//        if(GNS.sampleFraction > 0 && random.nextInt()% ((int)(1/GNS.sampleFraction)) == 0) {
//          msgLNS = true;
//        }
//      }

      if (updatePacket.getNameServerId() == NameServer.nodeID) {
        msgLNS = true;
      }

//        nameRecord.incrementUpdateRequest(); // Abhigyan: commented this because we are using lns votes for this calculation.
      if (msgLNS) {
        ConfirmUpdateLNSPacket confirmPacket = new ConfirmUpdateLNSPacket(Packet.PacketType.CONFIRM_UPDATE_LNS,
                updatePacket.getRequestID(), updatePacket.getLNSRequestID(), true);
//        ConfirmUpdateLNSPacket confirmPacket = proposedUpdates.remove(updatePacket.getNSRequestID());
//        Long t_1 = proposedUpdatesTime.remove(updatePacket.getNSRequestID());
//        if (t_1 != null && t0 - t_1 > 50) {
//          if (StartNameServer.debugMode) {
//            GNS.getLogger().severe("UpdateLongDelay = " + (t0 - t_1) + " in between Paxos propose & decision. UpdatePacket = "
//                    + updatePacket + " NameRecord = " + nameRecord);
//          }
//        }
        NameServer.sendToLNS(confirmPacket.toJSONObject(), updatePacket.getLocalNameServerId());
        if (StartNameServer.debugMode) {
          GNS.getLogger().fine("NS Sent confirmation to LNS. Sent packet: " + confirmPacket.toJSONObject());
        }
      }
//      tC = System.currentTimeMillis();
    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field not found exception. Exception = " + e.getMessage());
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      return;
    }

//    if (StartNameServer.debugMode) {
//      GNS.getLogger().fine("Saved name record to database.");
//    }

//    long t1 = System.currentTimeMillis();
//    NameServer.loadMonitor.add((int)(t1 - t0));
//    if (t1 - t0 > 10) {
//      GNS.getLogger().severe("Long latency HandleUpdateAddressNS " + (t1 - t0) + " Breakdown: " + (tA - t0) + "\t" + (tB - tA) + "\t" + (tC - tB) + "\t" + (t1 - tC));
//    }

//    long t1 = System.currentTimeMillis();
//    if (t1 - t0 > 50) {
//      if (StartNameServer.debugMode) {
//        GNS.getLogger().severe("UpdateLongDelay = " + (t1 - t0) + " in handleUpdateAddressLNS UpdatePacket = "
//                + updatePacket + "NameRecord = " + nameRecord);
//      }
//    }

  }
  // code written to test local name server: return same response everytime
  private static DNSPacket response = null;

  private static void setDNSPacket(DNSPacket resp1) {
    synchronized (lock) {
      response = resp1;
    }
  }

  // code written to test local name server: return same response each time
  private static DNSPacket getDNSPacket() {
    synchronized (lock) {
      return response;
    }
  }
  private static ArrayList<ColumnField> dnsField = new ArrayList<ColumnField>();

  public static ArrayList<ColumnField> getDNSPacketFields() {
    synchronized (dnsField) {
      if (dnsField.size() == 0) {
        dnsField.add(NameRecord.ACTIVE_NAMESERVERS);
        dnsField.add(NameRecord.TIME_TO_LIVE);
      }
      return dnsField;
    }
  }

  private void handleDNSPacket() throws IOException, JSONException {
//    long t0 = System.currentTimeMillis();
//    long tA = 0;
//    long tB = 0;
//    long tC = 0;
//    long tD = 0;
//    long tE = 0;
//    long tF = 0;
//    long tG = 0;
    if (StartNameServer.debugMode) {
      GNS.getLogger().finer("NS recvd DNS lookup request: " + incomingJSON);
    }
    DNSPacket dnsPacket = new DNSPacket(incomingJSON);
//    tA  = System.currentTimeMillis();
    if (dnsPacket.isQuery()) {
      int lnsId = dnsPacket.getLnsId();

      NameRecord nameRecord = null;
      try {
        if (Defs.ALLFIELDS.equals(dnsPacket.getQrecordKey().getName())) {
          // need everything so just grab all the fields
          nameRecord = NameServer.getNameRecord(dnsPacket.getQname());
        } else {
//          tB = System.currentTimeMillis();
          nameRecord = NameServer.getNameRecordMultiField(dnsPacket.getQname(),
                  getDNSPacketFields(),
                  dnsPacket.getQrecordKey().getName());
//          tC = System.currentTimeMillis();
        }

        if (StartNameServer.debugMode) {
          GNS.getLogger().fine("Name record read is " + nameRecord.toJSONObject());
        }

      } catch (RecordNotFoundException e) {
        GNS.getLogger().warning("Record not found for name: " + dnsPacket.getQname() + " Key = " + dnsPacket.getQrecordKey());
//        e.printStackTrace();
        // name record will be null
      }
//      tD  = System.currentTimeMillis();
      dnsPacket = makeResponsePacket(dnsPacket, nameRecord);
//      tE  = System.currentTimeMillis();
      NameServer.sendToLNS(dnsPacket.toJSONObject(), lnsId);

//      tF  = System.currentTimeMillis();
//      tG  = System.currentTimeMillis();
//      int responseTime = (int) (System.currentTimeMillis() - t0);
//      NameServer.loadMonitor.add(responseTime);
    } else {
      GNS.getLogger().severe("DNS Packet isn't a query!");
    }
//    long t1 = System.currentTimeMillis();
//    if (t1 - t0 > 20) {
//      GNS.getLogger().severe(" DNSPacket longlatency " + (t1 - t0) + "\tbreakdown\t" + (tA - t0)  + "\t"+ (tB - tA)  + "\t"  + (tC - tB)+ "\t" + (tD - tC) + "\t" + (tE - tD) + "\t" + (tF - tE) + "\t" + (tG - tF) + "\t" + (t1 - tG)) ;
//    }
  }

  private DNSPacket makeResponsePacket(DNSPacket dnsPacket, NameRecord nameRecord) {
    dnsPacket.getHeader().setQr(DNSRecordType.RESPONSE);
    // change it to a response packet
    String qName = dnsPacket.getQname();
    String qKey = dnsPacket.getQrecordKey().getName();
    try {
      // check if this is current set of ACTIVES (not primary!).
      if (nameRecord != null && nameRecord.containsActiveNameServer(NameServer.nodeID)) {
        if (qName != null) {
          dnsPacket.setActiveNameServers(nameRecord.getActiveNameServers());
          //Generate the response packet
          // assume no error... change it below if there is an error
          dnsPacket.getHeader().setRcode(DNSRecordType.RCODE_NO_ERROR);
          dnsPacket.setTTL(nameRecord.getTimeToLive());
          if (nameRecord.containsKey(qKey)) {
            dnsPacket.setSingleReturnValue(nameRecord.getKey(qKey));
            GNS.getLogger().fine("NS sending DNS lookup response: Name = " + qName);

          } else if (Defs.ALLFIELDS.equals(qKey)) {
            dnsPacket.setRecordValue(nameRecord.getValuesMap());
            GNS.getLogger().finer("NS sending multiple value DNS lookup response: Name = " + qName);
          } else { // send error msg.
            GNS.getLogger().severe("Record doesn't contain field: " + qKey + " name  = " + qName);
            dnsPacket.getHeader().setRcode(DNSRecordType.RCODE_ERROR);
          }
        } else { // send error msg.
          GNS.getLogger().finer("QNAME of query is NULL!");
          dnsPacket.getHeader().setRcode(DNSRecordType.RCODE_ERROR);
        }
      } else { // send invalid error msg.
        dnsPacket.getHeader().setRcode(DNSRecordType.RCODE_ERROR_INVALID_ACTIVE_NAMESERVER);
        if (nameRecord == null) {
          GNS.getLogger().info("Invalid actives. Name = " + qName);
        } else {
          GNS.getLogger().info("Invalid actives. Name = " + qName + " Actives = " + nameRecord.getActiveNameServers());
        }
      }
    } catch (FieldNotFoundException e) {
      if (StartNameServer.debugMode) {
        GNS.getLogger().severe("Field not found exception: " + e.getMessage());
      }
      dnsPacket.getHeader().setRcode(DNSRecordType.RCODE_ERROR);
    }
    return dnsPacket;

  }

  /**
   * Returns the set of active name servers for a name record to the local name server.
   *
   * @throws JSONException
   */
  private void handleRequestActivesPacket() throws JSONException, IOException {
//    long t0 = System.currentTimeMillis();
//    long tA = 0;
//    long tB = 0;
//    long tC = 0;
//    long tD = 0;
//    long tE = 0;
//    long tF = 0;
//    long tG = 0;
    if (StartNameServer.debugMode) {
      GNS.getLogger().fine("NS recvd request actives packet " + incomingJSON);
    }
//    tA = System.currentTimeMillis();
    RequestActivesPacket packet = new RequestActivesPacket(incomingJSON);
//    tB = System.currentTimeMillis();
    if (StartNameServer.debugMode) {
      GNS.getLogger().fine("Name = " + packet.getName());
    }

    boolean sendError = false;
    try {
      ReplicaControllerRecord rcRecord = NameServer.getNameRecordPrimaryMultiField(packet.getName(),
              ReplicaControllerRecord.MARKED_FOR_REMOVAL, ReplicaControllerRecord.ACTIVE_NAMESERVERS);
//      tD = System.currentTimeMillis();
//      if (tB - tA > 10) {
//        GNS.getLogger().severe(" RequestActivesDB longlatency " );
//      }
      if (rcRecord.isMarkedForRemoval()) {
        sendError = true;
      } else { // send reply to client
        packet.setActiveNameServers(rcRecord.getActiveNameservers());
//        tE = System.currentTimeMillis();
        NameServer.sendToLNS(packet.toJSONObject(), packet.getLNSID());
//        tF = System.currentTimeMillis();
        if (StartNameServer.debugMode) {
          GNS.getLogger().fine("Sent actives for " + packet.getName() //+ " " + packet.getRecordKey()
                  + " Actives = " + rcRecord.getActiveNameservers());
        }
//        tG = System.currentTimeMillis();
      }
    } catch (RecordNotFoundException e) {
      sendError = true;
    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field not found exception. " + e.getMessage());
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      return;
    }

    if (sendError) {
      packet.setActiveNameServers(null);
      NameServer.sendToLNS(packet.toJSONObject(), packet.getLNSID());
      if (StartNameServer.debugMode) {
        GNS.getLogger().fine("Error: Record does not exist for " + packet.getName());
      }
    }
//    long t1 = System.currentTimeMillis();
//    if (t1 - t0 > 20) {
//        GNS.getLogger().severe(" RequestActives longlatency " + (t1 - t0) + "\tbreakdown\t" + (tA - t0)  + "\t"+ (tB - tA)  + "\t"  + (tC - tB)+ "\t" + (tD - tC) + "\t" + (tE - tD) + "\t" + (tF - tE) + "\t" + (tG - tF) + "\t" + (t1 - tG)) ;
//    }
  }
}

class UpdateStatus {

  private String name;
  private int localNameServerID;
  private Set<Integer> allNameServers;
  private Set<Integer> nameServersResponded;
  private ConfirmUpdateLNSPacket confirmUpdate;

  public UpdateStatus(String name, int localNameServerID, Set<Integer> allNameServers,
          ConfirmUpdateLNSPacket confirmUpdate) {
    this.name = name;
    this.localNameServerID = localNameServerID;
    this.allNameServers = allNameServers;
    nameServersResponded = new HashSet<Integer>();
    this.confirmUpdate = confirmUpdate;
  }

  public ConfirmUpdateLNSPacket getConfirmUpdateLNSPacket() {
    return confirmUpdate;
  }

  public Set<Integer> getAllNameServers() {
    return allNameServers;
  }

  public int getLocalNameServerID() {
    return localNameServerID;
  }

  public String getName() {
    return name;
  }

  public void addNameServerResponded(int nameServerID) {
    nameServersResponded.add(nameServerID);
  }

  public boolean haveMajorityNSSentResponse() {
    GNS.getLogger().fine("All ns size:" + allNameServers.size());
    GNS.getLogger().fine("Responded ns size:" + nameServersResponded.size());
    if (allNameServers.size() == 0) {
      return true;
    }
    if (nameServersResponded.size() * 2 > allNameServers.size()) {
      return true;
    }
    return false;
  }
}
//    ReplicaControllerRecord rcRecord;
//
//    try {
//        GNS.getLogger().severe("before  ...");
//      rcRecord = NameServer.getNameRecordPrimaryMultiField(name, getAddRecordLNSFields());
//        GNS.getLogger().severe("after here ...");
//      try {
//        if (rcRecord.isAdded() || rcRecord.isMarkedForRemoval()) {
//          GNS.getLogger().fine(" ADD (ns " + NameServer.nodeID + ") : Record already exists");
//          ConfirmUpdateLNSPacket confirmPacket = new ConfirmUpdateLNSPacket(false, addRecordPacket);
//          JSONObject jsonConfirm = confirmPacket.toJSONObject();
//          NameServer.tcpTransport.sendToID(addRecordPacket.getLocalNameServerID(), jsonConfirm);
//            GNS.getLogger().fine("here3 ...");
//          return;
//        }
//      } catch (FieldNotFoundException e) {
//        GNS.getLogger().severe("Field not found exception. " + e.getMessage());
//        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        return;
//      }
//    } catch (RecordNotFoundException e) {
//        GNS.getLogger().severe(" after here2 ...");
//      // this is good. we can add
//      // Abhigyan:
//      // if GUID exists in DB:
//      //       if deleted OR ReplicaControllerRecord.isAdded() == true:
//      //            we will send an error to client saying record exist
//      //       if add is not complete, i.e., ReplicaControllerRecord.isAdded() == false:
//      //            we will add the record assuming that the same user is trying to add the record
//
//      // prepare confirm packet, and create an update status object
//      ConfirmUpdateLNSPacket confirmPacket = new ConfirmUpdateLNSPacket(true, addRecordPacket);
//
//      Set<Integer> primaryNameServers = HashFunction.getPrimaryReplicas(addRecordPacket.getName());
//      UpdateStatus status = new UpdateStatus(addRecordPacket.getName(), addRecordPacket.getLocalNameServerID(), primaryNameServers, confirmPacket);
//      status.addNameServerResponded(NameServer.nodeID);
//
//      Random r = new Random();
//      int nsReqID = r.nextInt(); // request ID assigned by this name server
//      addInProgress.put(nsReqID, status);
//
//      // prepare add record packet to be sent to other replica controllers
//      GNS.getLogger().fine(" NS Put request ID as " + nsReqID + " LNS request ID is " + confirmPacket.getLNSRequestID());
//      addRecordPacket.setLNSRequestID(nsReqID);
//      addRecordPacket.setLocalNameServerID(NameServer.nodeID);
//      addRecordPacket.setType(Packet.PacketType.ADD_RECORD_NS);
//
//      RequestPacket requestPacket = new RequestPacket(Packet.PacketType.ADD_RECORD_NS.getInt(),
//              addRecordPacket.toString(), PaxosPacketType.REQUEST, false);
//      String primaryPaxosID = ReplicaController.getPrimaryPaxosID(name);
//        GNS.getLogger().fine("here4 ..."  + primaryPaxosID);
//      String x = PaxosManager.propose(primaryPaxosID, requestPacket);
//        GNS.getLogger().fine("here5 ..."  + x);
// send to other replica controllers
//    NameServer.tcpTransport.sendToAll(addRecordPacket.toJSONObject(), primaryNameServers,
//            PortType.PERSISTENT_TCP_PORT, NameServer.nodeID);
//      NameServer.tcpTransport.sendToIDs(primaryNameServers, addRecordPacket.toJSONObject(), NameServer.nodeID);
//
//      GNS.getLogger().fine("ADD REQUEST FROM LNS (ns " + NameServer.nodeID + ") : "
//              + name + "/" + nameRecordKey.toString() + ", "
//              + value + " - SENDING TO OTHER PRIMARIES: " + primaryNameServers.toString());
//
//      // process add locally
//      ValuesMap valuesMap = new ValuesMap();
//      valuesMap.put(nameRecordKey.getName(), value);
//      rcRecord = new ReplicaControllerRecord(name, true);
//      try {
//        NameServer.addNameRecordPrimary(rcRecord);
//      } catch (RecordExistsException e1) {
//        GNS.getLogger().fine("Record already exists ... continue");
//      }
//      GNS.getLogger().fine(" Replica controller record created for name: " + addRecordPacket.getName());
//
//      try {
//        ReplicaController.handleNameRecordAddAtPrimary(rcRecord, valuesMap, 0, addRecordPacket.getTTL());
//      } catch (FieldNotFoundException e1) {
//        GNS.getLogger().fine("Field not found exception. Should not happen because we initialized all fields in record. " + e1.getMessage());
//        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//      }
//
//    }
//  }
//  private void handleAddRecordLNSPacketPrev() throws JSONException, IOException {
//    //System.out.println(" Hello ..... starting method .... ");
//    AddRecordPacket addRecordPacket;
//    String name;
//    NameRecordKey nameRecordKey;
//    ResultValue value;
//    addRecordPacket = new AddRecordPacket(incomingJSON);
//    name = addRecordPacket.getName();
//    nameRecordKey = addRecordPacket.getRecordKey();
//    value = addRecordPacket.getValue();
//    GNS.getLogger().info(" ADD FROM LNS (ns " + NameServer.nodeID + ") : " + name + "/" + nameRecordKey.toString() + ", " + value);
//
//    ReplicaControllerRecord rcRecord;
//    try {
//      rcRecord = NameServer.getNameRecordPrimaryMultiField(name, getAddRecordLNSFields());
//      try {
//        if (rcRecord.isAdded() || rcRecord.isMarkedForRemoval()) {
//          GNS.getLogger().fine(" ADD (ns " + NameServer.nodeID + ") : Record already exists");
//          ConfirmUpdateLNSPacket confirmPacket = new ConfirmUpdateLNSPacket(false, addRecordPacket);
//          JSONObject jsonConfirm = confirmPacket.toJSONObject();
//          NameServer.tcpTransport.sendToID(addRecordPacket.getLocalNameServerID(), jsonConfirm);
//          return;
//        }
//      } catch (FieldNotFoundException e) {
//        GNS.getLogger().severe("Field not found exception. " + e.getMessage());
//        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        return;
//      }
//    } catch (RecordNotFoundException e) {
//      // this is good. we can add
//      // Abhigyan:
//      // if GUID exists in DB:
//      //       if deleted OR ReplicaControllerRecord.isAdded() == true:
//      //            we will send an error to client saying record exist
//      //       if add is not complete, i.e., ReplicaControllerRecord.isAdded() == false:
//      //            we will add the record assuming that the same user is trying to add the record
//
//      // prepare confirm packet, and create an update status object
//      ConfirmUpdateLNSPacket confirmPacket = new ConfirmUpdateLNSPacket(true, addRecordPacket);
//
//      Set<Integer> primaryNameServers = HashFunction.getPrimaryReplicas(addRecordPacket.getName());
//      UpdateStatus status = new UpdateStatus(addRecordPacket.getName(), addRecordPacket.getLocalNameServerID(), primaryNameServers, confirmPacket);
//      status.addNameServerResponded(NameServer.nodeID);
//
//      Random r = new Random();
//      int nsReqID = r.nextInt(); // request ID assigned by this name server
//      addInProgress.put(nsReqID, status);
//
//      // prepare add record packet to be sent to other replica controllers
//      GNS.getLogger().fine(" NS Put request ID as " + nsReqID + " LNS request ID is " + confirmPacket.getLNSRequestID());
//      addRecordPacket.setLNSRequestID(nsReqID);
//      addRecordPacket.setLocalNameServerID(NameServer.nodeID);
//      addRecordPacket.setType(Packet.PacketType.ADD_RECORD_NS);
//
//      // send to other replica controllers
////    NameServer.tcpTransport.sendToAll(addRecordPacket.toJSONObject(), primaryNameServers,
////            PortType.PERSISTENT_TCP_PORT, NameServer.nodeID);
//      NameServer.tcpTransport.sendToIDs(primaryNameServers, addRecordPacket.toJSONObject(), NameServer.nodeID);
//
//      GNS.getLogger().fine("ADD REQUEST FROM LNS (ns " + NameServer.nodeID + ") : "
//              + name + "/" + nameRecordKey.toString() + ", "
//              + value + " - SENDING TO OTHER PRIMARIES: " + primaryNameServers.toString());
//
//      // process add locally
//      ValuesMap valuesMap = new ValuesMap();
//      valuesMap.put(nameRecordKey.getName(), value);
//      rcRecord = new ReplicaControllerRecord(name, true);
//      try {
//        NameServer.addNameRecordPrimary(rcRecord);
//      } catch (RecordExistsException e1) {
//        GNS.getLogger().fine("Record already exists ... continue");
//      }
//      GNS.getLogger().fine(" Replica controller record created for name: " + addRecordPacket.getName());
//
//      try {
//        ReplicaController.handleNameRecordAddAtPrimary(rcRecord, valuesMap, 0, addRecordPacket.getTTL());
//      } catch (FieldNotFoundException e1) {
//        GNS.getLogger().fine("Field not found exception. Should not happen because we initialized all fields in record. " + e1.getMessage());
//        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//      }
//    }
//  }
//  private void handleAddCompletePacket() throws JSONException {
//
//    AddCompletePacket packet = new AddCompletePacket(incomingJSON);
//    ReplicaControllerRecord rcRecord = new ReplicaControllerRecord(packet.getName());
//    try {
//      GNS.getLogger().fine(" Name Add Complete: " + packet.getName());
//      rcRecord.setAdded();
//    } catch (FieldNotFoundException e) {
//      GNS.getLogger().fine("Field not found exception. " + e.getMessage());
//      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//    }
//  }
//  private void handleAddRecordNSPrev() throws JSONException, IOException {
//
//    AddRecordPacket addRecordPacket;
//    String name;
//    NameRecordKey nameRecordKey;
//    ResultValue value;
//    addRecordPacket = new AddRecordPacket(incomingJSON);
//    name = addRecordPacket.getName();
//    nameRecordKey = addRecordPacket.getRecordKey();
//    value = addRecordPacket.getValue();
//    if (StartNameServer.debugMode) {
//      GNS.getLogger().info(" ADD FROM NS (ns " + NameServer.nodeID + ") : "
//              + name + "/" + nameRecordKey.toString() + ", " + value);
//    }
//
//
//    ReplicaControllerRecord rcRecord = new ReplicaControllerRecord(name, true);//NameServer.getNameRecord(name);
//    try {
//      NameServer.addNameRecordPrimary(rcRecord);
//    } catch (RecordExistsException e) {
//      GNS.getLogger().fine("Record already exists ... continue " + e.getMessage());
////      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//    }
//
//    ValuesMap valuesMap = new ValuesMap();
//    valuesMap.put(addRecordPacket.getRecordKey().getName(), addRecordPacket.getValue());
//
//    try {
//    ListenerReplicationPaxos.createPaxosInstanceForName(rcRecord.getName(), rcRecord.getActiveNameservers(),
//            rcRecord.getActivePaxosID(), valuesMap, 0, addRecordPacket.getTTL());
//
//    if (StartNameServer.debugMode) GNS.getLogger().info(" Active-paxos and name record created. Name = " +
//            rcRecord.getName());
//    } catch (FieldNotFoundException e) {
//      GNS.getLogger().fine("Field not found exception. Should not happen because we initialized all fields in record. " + e.getMessage());
//      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//    }
//
////    try {
////      ReplicaController.handleNameRecordAddAtPrimary(rcRecord, valuesMap, 0, addRecordPacket.getTTL());
////    } catch (FieldNotFoundException e) {
////      GNS.getLogger().fine("Field not found exception. Should not happen because we initialized all fields in record. " + e.getMessage());
////      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
////    }
//
//    ConfirmAddNSPacket pkt = new ConfirmAddNSPacket(addRecordPacket.getLNSRequestID(), NameServer.nodeID);
////    NameServer.tcpTransport.sendToID( pkt.toJSONObject(), addRecordPacket.getLocalNameServerID(), PortType.PERSISTENT_TCP_PORT);
//    NameServer.tcpTransport.sendToID(addRecordPacket.getLocalNameServerID(), pkt.toJSONObject());
//
//  }
//  private void handleConfirmAddNS() throws JSONException, IOException {
//    ConfirmAddNSPacket packet = new ConfirmAddNSPacket(incomingJSON);
//    UpdateStatus status = addInProgress.get(packet.getPacketID());
//    if (status == null) {
//      return;
//    }
//
//    status.addNameServerResponded(packet.getNameServerID());
//    if (status.haveMajorityNSSentResponse() == false) {
//      return;
//    }
//
//    status = addInProgress.remove(packet.getPacketID());
//    if (status == null) {
//      return;
//    }
//
//
//    JSONObject jsonConfirm = status.getConfirmUpdateLNSPacket().toJSONObject();
//    GNS.getLogger().fine("Sending ADD REQUEST CONFIRM (ns " + NameServer.nodeID + ") : to " + status.getLocalNameServerID());
//    GNS.getLogger().fine("Sending ADD REQUEST CONFIRM to LNS " + jsonConfirm);
//    NameServer.tcpTransport.sendToID(status.getLocalNameServerID(), jsonConfirm);
//
//    // confirm to every one that add is complete
//    AddCompletePacket pkt2 = new AddCompletePacket(status.getName());
////    NameServer.tcpTransport.sendToAll(pkt2.toJSONObject(),status.getAllNameServers(),PortType.PERSISTENT_TCP_PORT);
//    GNS.getLogger().fine("Sending AddCompletePacket to all NS");
//    NameServer.tcpTransport.sendToIDs(status.getAllNameServers(), pkt2.toJSONObject());
//  }

