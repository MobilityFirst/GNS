package edu.umass.cs.gns.nameserver;

import edu.umass.cs.gns.client.UpdateOperation;
import edu.umass.cs.gns.httpserver.Protocol;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.fields.Field;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaController;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaControllerRecord;
import edu.umass.cs.gns.packet.*;
import edu.umass.cs.gns.packet.paxospacket.PaxosPacketType;
import edu.umass.cs.gns.packet.paxospacket.RequestPacket;
import edu.umass.cs.gns.paxos.PaxosManager;
import edu.umass.cs.gns.util.HashFunction;
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
    try {
      switch (packetType) {
        case DNS:
          // LOOKUP
          handleDNSPacket();
          break;
        // ADD
        case ADD_RECORD_LNS:
          // receive from LNS, and send to other primaries
          handleAddRecordLNSPacket();
          break;
        case ADD_RECORD_NS:
          // receive from primary which received the request
          handleAddRecordNS();
          break;
        case CONFIRM_ADD_NS:  // ***NEW***
          // receive from primary which received the request
          handleConfirmAddNS();
          break;
        case ADD_COMPLETE:
          handleAddCompletePacket();
          break;
        // REMOVE
        case REMOVE_RECORD_LNS:
          ReplicaController.handleNameRecordRemoveRequestAtPrimary(incomingJSON);
          break;

        // UPDATE
        case UPDATE_ADDRESS_LNS:
          handleUpdateAddressLNS();
          break;
        case UPDATE_ADDRESS_NS:
          handleUpdateAddressNS();
          break;

        // Request current actives
        case REQUEST_ACTIVES:
          handleRequestActivesPacket();
          break;


      }
    } catch (JSONException e) {
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (Exception e) {
      GNS.getLogger().severe(" EXCEPTION Exception Exception  in client request worker. " + e.getMessage());
      e.printStackTrace();
    }

  }

  private void handleAddCompletePacket() throws JSONException {

    AddCompletePacket packet = new AddCompletePacket(incomingJSON);
    ReplicaControllerRecord rcRecord = new ReplicaControllerRecord(packet.getName());
    try {
      GNS.getLogger().fine(" Name Add Complete: " + packet.getName());
      rcRecord.setAdded();
    } catch (FieldNotFoundException e) {
      GNS.getLogger().fine("Field not found exception. " + e.getMessage());
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
//    if (rcRecord!= null) {
//
//    }
//    else {
//      GNS.getLogger().severe(" Error: Exception: Name does not exist: " + packet.getName());
//    }



  }
  private static ArrayList<Field> addRecordLNSFields = new ArrayList<Field>();

  private static ArrayList<Field> getAddRecordLNSFields() {
    synchronized (addRecordLNSFields) {
      if (addRecordLNSFields.size() > 0) {
        return addRecordLNSFields;
      }
      addRecordLNSFields.add(ReplicaControllerRecord.MARKED_FOR_REMOVAL);
      return addRecordLNSFields;
    }
  }

  private void handleAddRecordLNSPacket() throws JSONException, IOException {
    System.out.println(" Hello ..... starting method .... ");
    AddRecordPacket addRecordPacket;
    String name;
    NameRecordKey nameRecordKey;
    ArrayList<String> value;
    addRecordPacket = new AddRecordPacket(incomingJSON);
    name = addRecordPacket.getName();
    nameRecordKey = addRecordPacket.getRecordKey();
    value = addRecordPacket.getValue();
    GNS.getLogger().info(" ADD FROM LNS (ns " + NameServer.nodeID
            + ") : " + name + "/" + nameRecordKey.toString() + ", "
            + value);


    ReplicaControllerRecord rcRecord;
    try {
      rcRecord = NameServer.getNameRecordPrimaryMultiField(name, getAddRecordLNSFields());
      try {
        if (rcRecord.isAdded() || rcRecord.isMarkedForRemoval()) {
          GNS.getLogger().info(" ADD (ns " + NameServer.nodeID + ") : Record already exists");


          ConfirmUpdateLNSPacket confirmPacket = new ConfirmUpdateLNSPacket(false, addRecordPacket);
          JSONObject jsonConfirm = confirmPacket.toJSONObject();
          NameServer.tcpTransport.sendToID(addRecordPacket.getLocalNameServerID(), jsonConfirm);
          return;
        }
      } catch (FieldNotFoundException e) {
        GNS.getLogger().severe("Field not found exception. " + e.getMessage());
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        return;
      }
    } catch (RecordNotFoundException e) {
      // this is good. we can add
      // Abhigyan:
      // if GUID exists in DB:
      //       if deleted OR ReplicaControllerRecord.isAdded() == true:
      //            we will send an error to client saying record exist
      //       if add is not complete, i.e., ReplicaControllerRecord.isAdded() == false:
      //            we will add the record assuming that the same user is trying to add the record

      // prepare confirm packet, and create an update status object
      ConfirmUpdateLNSPacket confirmPacket = new ConfirmUpdateLNSPacket(true, addRecordPacket);

      Set<Integer> primaryNameServers = HashFunction.getPrimaryReplicas(addRecordPacket.getName());
      UpdateStatus status = new UpdateStatus(addRecordPacket.getName(), addRecordPacket.getLocalNameServerID(), primaryNameServers, confirmPacket);
      status.addNameServerResponded(NameServer.nodeID);

      Random r = new Random();
      int nsReqID = r.nextInt(); // request ID assigned by this name server
      addInProgress.put(nsReqID, status);

      // prepare add record packet to be sent to other replica controllers
      GNS.getLogger().fine(" NS Put request ID as " + nsReqID + " LNS request ID is " + confirmPacket.getLNSRequestID());
      addRecordPacket.setLNSRequestID(nsReqID);
      addRecordPacket.setLocalNameServerID(NameServer.nodeID);
      addRecordPacket.setType(Packet.PacketType.ADD_RECORD_NS);

      // send to other replica controllers
//    NameServer.tcpTransport.sendToAll(addRecordPacket.toJSONObject(), primaryNameServers,
//            PortType.PERSISTENT_TCP_PORT, NameServer.nodeID);
      NameServer.tcpTransport.sendToIDs(primaryNameServers, addRecordPacket.toJSONObject(), NameServer.nodeID);

      GNS.getLogger().info("ADD REQUEST FROM LNS (ns " + NameServer.nodeID + ") : "
              + name + "/" + nameRecordKey.toString() + ", "
              + value + " - SENDING TO OTHER PRIMARIES: " + primaryNameServers.toString());

      // process add locally
      ValuesMap valuesMap = new ValuesMap();
      valuesMap.put(nameRecordKey.getName(), value);
      rcRecord = new ReplicaControllerRecord(name, true);
      try {
        NameServer.addNameRecordPrimary(rcRecord);
      } catch (RecordExistsException e1) {
        GNS.getLogger().fine("Record already exists ... continue");
      }
      GNS.getLogger().fine(" Replica controller record created for name: " + addRecordPacket.getName());

      try {
        ReplicaController.handleNameRecordAddAtPrimary(rcRecord, valuesMap, 0);
      } catch (FieldNotFoundException e1) {
        GNS.getLogger().fine("Field not found exception. Should not happen because we initialized all fields in record. " + e1.getMessage());
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    }




//      GNS.getLogger().fine(" Full record: "  + rcRecord);
//    if (NameServer.getNameRecordPrimary(name) == null)
//
//    else
//      NameServer.updateNameRecordPrimary(rcRecord);


//      ReplicaControllerRecord temp = NameServer.getNameRecordPrimary(rcRecord.getName());
//      GNS.getLogger().fine(" Full record 2: "  + temp);
//
//      temp = NameServer.getNameRecordPrimaryLazy(rcRecord.getName());
//      GNS.getLogger().fine(" Full record 3: "  + temp);

//      ReplicaControllerRecord rc1 = NameServer.getNameRecordPrimary(nameRecord.getName());
//      GNS.getLogger().fine(" Name record read from DB: "  + rc1);

//        NameRecord record = new NameRecord(name, nameRecordKey, value);
//        // add the name record, which also creates a paxos instance for this name record
//        NameServer.addNameRecord(record);
//      if (nameRecord == null) {
    // create and add the record

//      } else {
//        nameRecord.put(nameRecordKey.getName(), new QueryResultValue(value));
//        NameServer.updateNameRecord(nameRecord);
//      }
    // send back a confirmation



  }

  private void handleAddRecordNS() throws JSONException, IOException {

    AddRecordPacket addRecordPacket;
    String name;
    NameRecordKey nameRecordKey;
    ArrayList<String> value;
    addRecordPacket = new AddRecordPacket(incomingJSON);
    name = addRecordPacket.getName();
    nameRecordKey = addRecordPacket.getRecordKey();
    value = addRecordPacket.getValue();
    if (StartNameServer.debugMode) {
      GNS.getLogger().info(" ADD FROM NS (ns " + NameServer.nodeID + ") : "
              + name + "/" + nameRecordKey.toString() + ", " + value);
    }


    ReplicaControllerRecord rcRecord = new ReplicaControllerRecord(name, true);//NameServer.getNameRecord(name);
    try {
      NameServer.addNameRecordPrimary(rcRecord);
    } catch (RecordExistsException e) {
      GNS.getLogger().fine("Record already exists ... continue " + e.getMessage());
//      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

    ValuesMap valuesMap = new ValuesMap();
    valuesMap.put(addRecordPacket.getRecordKey().getName(), addRecordPacket.getValue());
    try {
      ReplicaController.handleNameRecordAddAtPrimary(rcRecord, valuesMap, 0);
    } catch (FieldNotFoundException e) {
      GNS.getLogger().fine("Field not found exception. Should not happen because we initialized all fields in record. " + e.getMessage());
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

    ConfirmAddNSPacket pkt = new ConfirmAddNSPacket(addRecordPacket.getLNSRequestID(), NameServer.nodeID);
//    NameServer.tcpTransport.sendToID( pkt.toJSONObject(), addRecordPacket.getLocalNameServerID(), PortType.PERSISTENT_TCP_PORT);
    NameServer.tcpTransport.sendToID(addRecordPacket.getLocalNameServerID(), pkt.toJSONObject());

  }

  private void handleConfirmAddNS() throws JSONException, IOException {
//    GNS.getLogger().info("here asdf");
    ConfirmAddNSPacket packet = new ConfirmAddNSPacket(incomingJSON);
    UpdateStatus status = addInProgress.get(packet.getPacketID());
    if (status == null) {
      return;
    }

    status.addNameServerResponded(packet.getNameServerID());
    if (status.haveMajorityNSSentResponse() == false) {
      return;
    }

    status = addInProgress.remove(packet.getPacketID());
    if (status == null) {
      return;
    }


    JSONObject jsonConfirm = status.getConfirmUpdateLNSPacket().toJSONObject();
    GNS.getLogger().info("Sending ADD REQUEST CONFIRM (ns " + NameServer.nodeID + ") : to "
            + +status.getLocalNameServerID());
    GNS.getLogger().info("Sending ADD REQUEST CONFIRM to LNS " + jsonConfirm);
    NameServer.tcpTransport.sendToID(status.getLocalNameServerID(), jsonConfirm);

    // confirm to every one that add is complete
    AddCompletePacket pkt2 = new AddCompletePacket(status.getName());
//    NameServer.tcpTransport.sendToAll(pkt2.toJSONObject(),status.getAllNameServers(),PortType.PERSISTENT_TCP_PORT);
    NameServer.tcpTransport.sendToIDs(status.getAllNameServers(), pkt2.toJSONObject());
    GNS.getLogger().info("Sending AddCompletePacket to all NS" + jsonConfirm);

  }

  private void handleUpdateAddressLNS() throws JSONException, IOException {
//    long t0 = System.currentTimeMillis();
    if (StartNameServer.debugMode) {
      GNS.getLogger().fine(" Recvd Update Address from LNS: " + incomingJSON);
    }

    UpdateAddressPacket updatePacket = new UpdateAddressPacket(incomingJSON);

    // IF this is an UPSERT operation
    if (updatePacket.getOperation().isUpsert()) {
      handleUpsert(updatePacket);
      // END OF UPSERT HANDLING
    } else {
      handleUpdate(updatePacket);
    }
  }

  /**
   * Handles the upsert case of UpdateAddressPacket
   *
   * @param updatePacket
   * @throws JSONException
   * @throws IOException
   */
  private void handleUpsert(UpdateAddressPacket updatePacket) throws JSONException, IOException {
    ArrayList<Field> readFields = new ArrayList<Field>();
    readFields.add(ReplicaControllerRecord.MARKED_FOR_REMOVAL);
    readFields.add(ReplicaControllerRecord.ACTIVE_NAMESERVERS);
    // this must be primary
    ReplicaControllerRecord nameRecordPrimary = null;
    try {
      nameRecordPrimary = NameServer.getNameRecordPrimaryMultiField(updatePacket.getName(), readFields);
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
        for (int x : activeNS) {
          activeID = x;
        }
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
              updatePacket.getTTL());
      addRecordPacket.setLNSRequestID(updatePacket.getLNSRequestID());
      incomingJSON = addRecordPacket.toJSONObject();
      handleAddRecordLNSPacket();
      if (StartNameServer.debugMode) {
        GNS.getLogger().fine(" NS processing UPSERT changed to ADD: "
                + incomingJSON);
      }
    }
  }
  private static ArrayList<Field> lnsUpdateFields = new ArrayList<Field>();

  private static ArrayList<Field> getLNSUpdateFields() {
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
    int updateID = incrementUpdateID();
    updatePacket.setNSRequestID(updateID); // to check the request ID.
    updatePacket.setType(Packet.PacketType.UPDATE_ADDRESS_NS); //
    ConfirmUpdateLNSPacket confirmPacket = ConfirmUpdateLNSPacket.createSuccessPacket(updatePacket);
//    proposedUpdates.put(updateID, confirmPacket);

    if (StartNameServer.debugMode) {
      GNS.getLogger().fine(" Update Packet : " + updatePacket);
    }
    // Propose to paxos
    String activePaxosID = updatePacket.getName(); // nameRecord.getActivePaxosID();
    if (StartNameServer.debugMode) {
      GNS.getLogger().fine(" Update proposed to paxosID = " + activePaxosID);
    }
    boolean propose = PaxosManager.propose(activePaxosID, new RequestPacket(updatePacket.getType().getInt(),
            updatePacket.toString(), PaxosPacketType.REQUEST, false));
    if (StartNameServer.debugMode) {
      GNS.getLogger().fine(" PAXOS PROPOSAL: propose update request: " + updatePacket
              + "result = " + propose);
    }
    if (propose == false) {
      ConfirmUpdateLNSPacket failConfirmPacket =
              ConfirmUpdateLNSPacket.createFailPacket(updatePacket);
      // inform LNS of failed request
      NameServer.tcpTransport.sendToID(updatePacket.getLocalNameServerId(), failConfirmPacket.toJSONObject());
      //        NSListenerUDP.udpTransport.sendPacket(failConfirmPacket.toJSONObject(),
      //                updatePacket.getLocalNameServerId(), GNS.PortType.LNS_UDP_PORT);
      if (StartNameServer.debugMode) {
        GNS.getLogger().fine(" UpdateRequest-InvalidNameServer\t" + updatePacket.getName()
                + "\t" + NameServer.nodeID + "\t" + updatePacket.getLocalNameServerId());// + "\t" + updatePacket.getSequenceNumber());
      }
    }
    long t1 = System.currentTimeMillis();

    if (t1 - t0 > 10) {
      GNS.getLogger().severe("Long latency HandleUpdate " + (t1 - t0));
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
   * When Paxos commits an address update that is proposed, this method executes the address update in DB.
   *
   * @throws JSONException
   * @throws IOException
   */
  private void handleUpdateAddressNS() throws JSONException, IOException {

    long t0 = System.currentTimeMillis();
    if (StartNameServer.debugMode) {
      GNS.getLogger().fine("PAXOS DECISION: Update Confirmed ...   " + incomingJSON);
    }
    UpdateAddressPacket updatePacket = new UpdateAddressPacket(incomingJSON);
    NameRecord nameRecord;
    if (updatePacket.getOperation().equals(UpdateOperation.REPLACE_ALL)) {
      nameRecord = new NameRecord(updatePacket.getName());
    } else {
      try {
        nameRecord = NameServer.getNameRecordMultiField(updatePacket.getName(), null, updatePacket.getRecordKey().getName());
      } catch (RecordNotFoundException e) {
        GNS.getLogger().fine(" Exception: name record not found. Name = " + updatePacket.getName());
        e.printStackTrace();
        return;
      }
    }
    long tA = System.currentTimeMillis();
    long tB = 0;
    long tC = 0;

    // Apply update
    if (StartNameServer.debugMode) {
      GNS.getLogger().fine("NAME RECORD is: " + nameRecord.toString());
    }
    boolean result;
    try {
      result = nameRecord.updateKey(updatePacket.getRecordKey().getName(), updatePacket.getUpdateValue(),
              updatePacket.getOldValue(), updatePacket.getOperation());
      tB = System.currentTimeMillis();
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
          NameServer.tcpTransport.sendToID(updatePacket.getLocalNameServerId(), failPacket.toJSONObject());
          if (StartNameServer.debugMode) {
            GNS.getLogger().fine("Error msg sent to client for failed update " + incomingJSON);
          }
        }
        // return now.
        return;
      }

      if (StartNameServer.debugMode) {
        GNS.getLogger().fine("Update applied" + incomingJSON);
      }

      if (updatePacket.getNameServerId() == NameServer.nodeID) {

//        nameRecord.incrementUpdateRequest(); // Abhigyan: commented this because we are using lns votes for this calculation.

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
        if (confirmPacket != null) {
          // send confirmation to
          NameServer.tcpTransport.sendToID(updatePacket.getLocalNameServerId(), confirmPacket.toJSONObject());
//          NSListenerUDP.udpTransport.sendPacket(confirmPacket.toJSONObject(),
//                  confirmPacket.getLocalNameServerId(), GNS.PortType.LNS_UDP_PORT);
          if (StartNameServer.debugMode) {
            GNS.getLogger().fine("NS Sent confirmation to LNS. Sent packet: " + confirmPacket.toJSONObject());
          }
        }
      }
      tC = System.currentTimeMillis();
    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field not found exception. Exception = " + e.getMessage());
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      return;
    }
    if (StartNameServer.debugMode) {
      GNS.getLogger().fine("Saved name record to database.");
    }
    long t1 = System.currentTimeMillis();

    if (t1 - t0 > 10) {
      GNS.getLogger().severe("Long latency HandleUpdateAddressNS " + (t1 - t0) + " Breakdown: " + (tA - t0) + "\t" + (tB - tA) + "\t" + (tC - tB) + "\t" + (t1 - tC));
    }

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
  private static ArrayList<Field> dnsField = new ArrayList<Field>();

  public static ArrayList<Field> getDNSPacketFields() {
    synchronized (dnsField) {
      if (dnsField.size() == 0) {
        dnsField.add(NameRecord.ACTIVE_NAMESERVERS);
        dnsField.add(NameRecord.TIME_TO_LIVE);
      }
      return dnsField;
    }
  }

  private void handleDNSPacket() throws IOException, JSONException {
    GNS.getLogger().finer("NS recvd DNS lookup request: " + incomingJSON);
    DNSPacket dnsPacket = new DNSPacket(incomingJSON);

    if (dnsPacket.isQuery()) {
      int lnsId = dnsPacket.getLnsId();

//      ArrayList<Field> userField = new ArrayList<Field>();
//      userField.add(new Field(dnsPacket.getQrecordKey().getName(), FieldType.LIST_STRING));

      NameRecord nameRecord = null;
      try {
        if (Protocol.ALLFIELDS.equals(dnsPacket.getQrecordKey().getName())) {
          // need everything so just grab all the fields
          nameRecord = NameServer.getNameRecord(dnsPacket.getQname());
        } else {
          nameRecord = NameServer.getNameRecordMultiField(dnsPacket.getQname(),
                  getDNSPacketFields(),
                  dnsPacket.getQrecordKey().getName());
        }

        GNS.getLogger().finer("Name record read is " + nameRecord.toJSONObject());

      } catch (RecordNotFoundException e) {
        GNS.getLogger().severe("Record not found for name: " + dnsPacket.getQname() + " Key = " + dnsPacket.getQrecordKey());
//        e.printStackTrace();
        // name record will be null
      }
      dnsPacket = makeResponsePacket(dnsPacket, nameRecord);
      NameServer.tcpTransport.sendToID(lnsId, dnsPacket.toJSONObject());
      // long responseTime = System.currentTimeMillis() - requestRecvdTime;
      // TODO update average response time for load balancing
    } else {
      GNS.getLogger().severe("DNS Packet isn't a query!");
    }
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
          //Generate the response packet
          // assume no error... change it below if there is an error
          dnsPacket.getHeader().setRcode(DNSRecordType.RCODE_NO_ERROR);
          dnsPacket.setTTL(nameRecord.getTimeToLive());
          if (nameRecord.containsKey(qKey)) {
            dnsPacket.setSingleReturnValue(nameRecord.getKey(qKey));
            GNS.getLogger().finer("NS sending DNS lookup response: Name = " + qName);

          } else if (Protocol.ALLFIELDS.equals(qKey)) {
            dnsPacket.setRecordValue(nameRecord.getValuesMap());
            GNS.getLogger().finer("NS sending multiple value DNS lookup response: Name = " + qName);
          } else { // send error msg.
            GNS.getLogger().finer("Record doesn't contain field: " + qKey);
            dnsPacket.getHeader().setRcode(DNSRecordType.RCODE_ERROR);
          }
        } else { // send error msg.
          GNS.getLogger().finer("QNAME of query is NULL!");
          dnsPacket.getHeader().setRcode(DNSRecordType.RCODE_ERROR);
        }
      } else { // send invalid error msg.
        dnsPacket.getHeader().setRcode(DNSRecordType.RCODE_ERROR_INVALID_ACTIVE_NAMESERVER);
        GNS.getLogger().finer("Invalid actives. Name: " + qName);
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
    if (StartNameServer.debugMode) {
      GNS.getLogger().fine("NS recvd request actives packet " + incomingJSON);
    }

    RequestActivesPacket packet = new RequestActivesPacket(incomingJSON);

    if (StartNameServer.debugMode) {
      GNS.getLogger().fine("Name = " + packet.getName());
    }

    ArrayList<Field> readFields = new ArrayList<Field>();
    readFields.add(ReplicaControllerRecord.MARKED_FOR_REMOVAL);
    readFields.add(ReplicaControllerRecord.ACTIVE_NAMESERVERS);

    boolean sendError = false;
    try {
      ReplicaControllerRecord rcRecord = NameServer.getNameRecordPrimaryMultiField(packet.getName(), readFields);

      if (rcRecord.isMarkedForRemoval()) {
        sendError = true;
      } else { // send reply to client
        packet.setActiveNameServers(rcRecord.getActiveNameservers());
        NameServer.tcpTransport.sendToID(packet.getLNSID(), packet.toJSONObject());
        if (StartNameServer.debugMode) {
          GNS.getLogger().fine("Sent actives for " + packet.getName() //+ " " + packet.getRecordKey()
                  + " Actives = " + rcRecord.getActiveNameservers());
        }
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
      NameServer.tcpTransport.sendToID(packet.getLNSID(), packet.toJSONObject());
      if (StartNameServer.debugMode) {
        GNS.getLogger().fine("Error: Record does not exist for " + packet.getName());
      }
    }

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
