/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved
 */
package edu.umass.cs.gns.nameserver;

import edu.umass.cs.gns.clientsupport.Defs;
import edu.umass.cs.gns.clientsupport.GuidInfo;
import edu.umass.cs.gns.clientsupport.MetaDataTypeName;
import edu.umass.cs.gns.clientsupport.UpdateOperation;
import edu.umass.cs.gns.database.ColumnField;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.clientsupport.NSAccessSupport;
import edu.umass.cs.gns.nameserver.clientsupport.NSAccountAccess;
import edu.umass.cs.gns.nameserver.clientsupport.SiteToSiteQueryHandler;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaController;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaControllerRecord;
import edu.umass.cs.gns.packet.*;
import edu.umass.cs.gns.packet.paxospacket.PaxosPacketType;
import edu.umass.cs.gns.packet.paxospacket.RequestPacket;
import edu.umass.cs.gns.util.BestServerSelection;
import edu.umass.cs.gns.util.ConsistentHashing;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Random;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Handle client requests - ADD/REMOVE/LOOKUP/UPDATE + REQUESTACTIVES
 *
 * @author abhigyan
 */
public class ClientRequestWorker extends TimerTask {

  private JSONObject incomingJSON;
  private Packet.PacketType packetType;
  private static ConcurrentHashMap<Integer, UpdateStatus> addInProgress = new ConcurrentHashMap<Integer, UpdateStatus>();

  public ClientRequestWorker(JSONObject json, Packet.PacketType packetType) {
    this.packetType = packetType;
    this.incomingJSON = json;
  }

  public static void handleIncomingPacket(JSONObject json, Packet.PacketType packetType) {
    NameServer.getExecutorService().submit(new ClientRequestWorker(json, packetType));
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
        case UPDATE_ADDRESS_NS:
          if (StartNameServer.eventualConsistency) { // eventualConsistency used only for experiments
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
      GNS.getLogger().warning("Long delay " + (t1 - t0) + "ms. Packet: " + incomingJSON);
    }
  }

  /**
   * After the update request has been committed by Paxos, this method does the update in database at each r
   * active replica for that name. It also sends confirmation to the client.
   *
   * @throws JSONException
   * @throws IOException
   */
  public static void handleUpdateAddressNS(JSONObject incomingJSON) throws JSONException, IOException {
    long t0 = System.currentTimeMillis();
    if (StartNameServer.debugMode) {
      GNS.getLogger().fine("PAXOS DECISION: Update Confirmed ...   " + incomingJSON);
    }
    UpdateAddressPacket updatePacket = new UpdateAddressPacket(incomingJSON);
    NameRecord nameRecord;

    if (updatePacket.getOperation().equals(UpdateOperation.REPLACE_ALL)) { // we don't need to read for replace-all
      nameRecord = new NameRecord(NameServer.getRecordMap(), updatePacket.getName());
    } else {
      try {
        nameRecord = NameRecord.getNameRecordMultiField(NameServer.getRecordMap(), updatePacket.getName(), null, updatePacket.getRecordKey().getName());
      } catch (RecordNotFoundException e) {
        GNS.getLogger().severe(" Error: name record not found before update. Return. Name = " + updatePacket.getName());
        e.printStackTrace();
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
        GNS.getLogger().fine("Update operation result = " + result + "\t" + updatePacket.getUpdateValue());
      }



      if (!result) { // update failed
        if (StartNameServer.debugMode) {
          GNS.getLogger().fine("Update operation failed " + incomingJSON);
        }
        if (updatePacket.getNameServerId() == NameServer.getNodeID()) { //if this node proposed this update
          // send error message to client
          ConfirmUpdateLNSPacket failPacket = new ConfirmUpdateLNSPacket(Packet.PacketType.CONFIRM_UPDATE_LNS,
                  updatePacket.getRequestID(), updatePacket.getLNSRequestID(), NSResponseCode.ERROR);
          NameServer.returnToSender(failPacket.toJSONObject(), updatePacket.getLocalNameServerId());

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

      if (updatePacket.getNameServerId() == NameServer.getNodeID()) {
        msgLNS = true;
      }

      // Abhigyan: commented this because we are using lns votes for this calculation.
      // this should be uncommented once active replica starts to send read/write statistics for name.
//        nameRecord.incrementUpdateRequest();
      if (msgLNS) {
        ConfirmUpdateLNSPacket confirmPacket = new ConfirmUpdateLNSPacket(Packet.PacketType.CONFIRM_UPDATE_LNS,
                updatePacket.getRequestID(), updatePacket.getLNSRequestID(), NSResponseCode.NO_ERROR);

        NameServer.returnToSender(confirmPacket.toJSONObject(), updatePacket.getLocalNameServerId());
        if (StartNameServer.debugMode) {
          GNS.getLogger().fine("NS Sent confirmation to LNS. Sent packet: " + confirmPacket.toJSONObject());
        }
      }
    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field not found exception. Exception = " + e.getMessage());
      e.printStackTrace();
      return;
    }

    long t1 = System.currentTimeMillis();
    if (t1 - t0 > 20) {
      GNS.getLogger().warning("Long latency HandleUpdateAddressNS " + (t1 - t0));
    }



  }

  /**
   * After the add request for a name has been committed by Paxos, this method executes the add at each replica
   * controller for that name. It inserts ReplicaControllerRecord  and NameRecord in database, and
   * creates a paxos instance corresponding to that name.
   *
   * @throws JSONException
   * @throws IOException
   */
  public static void handleAddRecordNS(JSONObject incomingJSON, boolean recover) throws JSONException, IOException {

    AddRecordPacket addRecordPacket;
    String name;
    NameRecordKey nameRecordKey;
    ResultValue value;
    addRecordPacket = new AddRecordPacket(incomingJSON);
    name = addRecordPacket.getName();
    nameRecordKey = addRecordPacket.getRecordKey();
    value = addRecordPacket.getValue();
    GNS.getLogger().fine(" ADD FROM NS (ns " + NameServer.getNodeID() + ") : " + name + "/" + nameRecordKey.toString() + ", " + value);

    ReplicaControllerRecord rcRecord = new ReplicaControllerRecord(NameServer.getReplicaController(), name, true);//NameServer.getNameRecord(name);

    try {
      ReplicaControllerRecord.addNameRecordPrimary(NameServer.getReplicaController(), rcRecord);

      if (recover) {
        GNS.getLogger().fine("Adding record: " + addRecordPacket.getName());
        return;
      }

      ValuesMap valuesMap = new ValuesMap();
      valuesMap.put(addRecordPacket.getRecordKey().getName(), addRecordPacket.getValue());
      try {
        NameRecord nameRecord = new NameRecord(NameServer.getRecordMap(), name, rcRecord.getActiveNameservers(), rcRecord.getActivePaxosID(),
                valuesMap, addRecordPacket.getTTL());
        try {
          NameRecord.addNameRecord(NameServer.getRecordMap(), nameRecord);

        } catch (RecordExistsException e) {
          GNS.getLogger().severe("ERROR: Exception: name record exists but replica controller does not exist. This should never happen ");
          e.printStackTrace();
        }
        ListenerReplicationPaxos.createPaxosInstanceForName(rcRecord.getName(), rcRecord.getActiveNameservers(),
                rcRecord.getActivePaxosID(), valuesMap, 0, addRecordPacket.getTTL());
        GNS.getLogger().fine(" Active-paxos and name record created. Name = " + rcRecord.getName());
        UpdateStatus status = addInProgress.remove(addRecordPacket.getLNSRequestID());
        if (status != null) {
          NameServer.returnToSender(status.getConfirmUpdateLNSPacket().toJSONObject(), status.getLocalNameServerID());
        } else {
          GNS.getLogger().fine(" Status record missing for Name = " + rcRecord.getName() + " request id: " + addRecordPacket.getLNSRequestID());
        }
      } catch (FieldNotFoundException e) {
        GNS.getLogger().severe("Field not found exception. Should not happen because we initialized all fields in record. " + e.getMessage());
        e.printStackTrace();
      }
    } catch (RecordExistsException e) {
      UpdateStatus status = addInProgress.remove(addRecordPacket.getLNSRequestID());
      if (status != null) {
        // send failure
        ConfirmUpdateLNSPacket confirmPkt = status.getConfirmUpdateLNSPacket();
        confirmPkt.convertToFailPacket(NSResponseCode.ERROR);
        NameServer.returnToSender(confirmPkt.toJSONObject(), status.getLocalNameServerID());
        GNS.getLogger().fine("Record already exists ... sent error to client" + e.getMessage());
      } else {
        GNS.getLogger().fine(" Status record missing for request id: " + addRecordPacket.getLNSRequestID());
      }
      GNS.getLogger().fine("Record already exists ... continue " + e.getMessage());
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

    GNS.getLogger().fine(" ADD FROM LNS (ns " + NameServer.getNodeID() + ") : " + name + "/" + nameRecordKey.toString() + ", " + value);

    ConfirmUpdateLNSPacket confirmPacket = new ConfirmUpdateLNSPacket(NSResponseCode.NO_ERROR, addRecordPacket);

    Set<Integer> primaryNameServers = ConsistentHashing.getReplicaControllerSet(addRecordPacket.getName());
    UpdateStatus status = new UpdateStatus(addRecordPacket.getName(), addRecordPacket.getLocalNameServerID(), primaryNameServers, confirmPacket);
    status.addNameServerResponded(NameServer.getNodeID());

    Random r = new Random();
    int nsReqID = r.nextInt(); // request ID assigned by this name server
    addInProgress.put(nsReqID, status);

    // prepare add record packet to be sent to other replica controllers
    GNS.getLogger().fine(" NS Put request ID as " + nsReqID + " LNS request ID is " + confirmPacket.getLNSRequestID());
    addRecordPacket.setLNSRequestID(nsReqID);
    addRecordPacket.setLocalNameServerID(NameServer.getNodeID());
    addRecordPacket.setType(Packet.PacketType.ADD_RECORD_NS);

    RequestPacket requestPacket = new RequestPacket(Packet.PacketType.ADD_RECORD_NS.getInt(),
            addRecordPacket.toString(), PaxosPacketType.REQUEST, false);
    String primaryPaxosID = ReplicaController.getPrimaryPaxosID(name);
    NameServer.getPaxosManager().propose(primaryPaxosID, requestPacket);

  }

  private void handleUpdateAddressLNS() throws JSONException, IOException,
          InvalidKeyException, InvalidKeySpecException, NoSuchAlgorithmException, SignatureException {
    long startTime = System.currentTimeMillis();
    UpdateAddressPacket updatePacket = new UpdateAddressPacket(incomingJSON);

    // First we do signature and ACL checks
    String guid = updatePacket.getName();
    String field = updatePacket.getRecordKey().getName();
    String writer = updatePacket.getAccessor();
    String signature = updatePacket.getSignature();
    String message = updatePacket.getMessage();
    NSResponseCode errorCode = NSResponseCode.NO_ERROR;
    if (writer != null) { // writer will be null for internal system reads
      errorCode = signatureAndACLCheck(guid, field, writer, signature, message, MetaDataTypeName.WRITE_WHITELIST);
    }
    // return an error packet if one of the checks doesn't pass
    if (errorCode.isAnError()) {
      ConfirmUpdateLNSPacket failConfirmPacket = ConfirmUpdateLNSPacket.createFailPacket(updatePacket, errorCode);
      NameServer.returnToSender(failConfirmPacket.toJSONObject(), updatePacket.getLocalNameServerId());
      return;
    }

    // all checks pass, do the update
    if (updatePacket.getOperation().isUpsert()) {
      handleUpsert(updatePacket);
    } else {
      handleUpdate(updatePacket);
    }
//    NameServer.loadMonitor.add((int) (System.currentTimeMillis() - startTime));
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
    ReplicaControllerRecord nameRecordPrimary;
    try {
      nameRecordPrimary = ReplicaControllerRecord.getNameRecordPrimaryMultiField(NameServer.getReplicaController(), updatePacket.getName(),
              ReplicaControllerRecord.MARKED_FOR_REMOVAL, ReplicaControllerRecord.ACTIVE_NAMESERVERS);
      try {
        if (nameRecordPrimary.isMarkedForRemoval()) {
          ConfirmUpdateLNSPacket failConfirmPacket =
                  ConfirmUpdateLNSPacket.createFailPacket(updatePacket, NSResponseCode.ERROR);
          NameServer.getTcpTransport().sendToID(updatePacket.getLocalNameServerId(), failConfirmPacket.toJSONObject());
          if (StartNameServer.debugMode) {
            GNS.getLogger().fine(" UPSERT-FAILED because name record deleted already\t" + updatePacket.getName()
                    + "\t" + NameServer.getNodeID() + "\t" + updatePacket.getLocalNameServerId());// + "\t" + updatePacket.getSequenceNumber());
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
        e1.printStackTrace();
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
        NameServer.getTcpTransport().sendToID(activeID, updatePacket.toJSONObject());
        // could not find activeNS for this name
      } else {
        // send error to LNS
        ConfirmUpdateLNSPacket failConfirmPacket =
                ConfirmUpdateLNSPacket.createFailPacket(updatePacket, NSResponseCode.ERROR);
        NameServer.returnToSender(failConfirmPacket.toJSONObject(), updatePacket.getLocalNameServerId());

        String msg = " UPSERT-FAILED\t" + updatePacket.getName()
                + "\t" + NameServer.getNodeID() + "\t" + updatePacket.getLocalNameServerId();
        if (StartNameServer.debugMode) {
          GNS.getLogger().fine(msg);
        }
      }
    } catch (RecordNotFoundException e) {
      // do an INSERT (AKA ADD) operation

      AddRecordPacket addRecordPacket = new AddRecordPacket(updatePacket.getRequestID(), updatePacket.getName(),
              updatePacket.getRecordKey(), updatePacket.getUpdateValue(), updatePacket.getLocalNameServerId(),
              updatePacket.getTTL()); //  getTTL() is used only with upsert.
      addRecordPacket.setLNSRequestID(updatePacket.getLNSRequestID());
      incomingJSON = addRecordPacket.toJSONObject();
      handleAddRecordLNSPacket();
      if (StartNameServer.debugMode) {
        GNS.getLogger().fine(" NS processing UPSERT changed to ADD: " + incomingJSON);
      }
    }
  }

  /**
   * Handles the update case of UpdateAddressPacket
   *
   * @param updatePacket
   * @throws JSONException
   * @throws IOException
   */
  private void handleUpdate(UpdateAddressPacket updatePacket) throws JSONException, IOException {
    long startTime = System.currentTimeMillis();
    // HANDLE NON-UPSERT CASE

    if (StartNameServer.debugMode) {
      GNS.getLogger().fine(" Update Packet Type: " + updatePacket.getType().getInt());
    }

    // Save request info to send confirmation to LNS later.
    updatePacket.setNameServerId(NameServer.getNodeID()); // to check that I had proposed this message

    updatePacket.setType(Packet.PacketType.UPDATE_ADDRESS_NS); //
    if (StartNameServer.eventualConsistency) {
      boolean sendFailure = false;
      ArrayList<ColumnField> fields = new ArrayList<ColumnField>();
      fields.add(NameRecord.ACTIVE_NAMESERVERS);
      try {
        NameRecord nameRecord = NameRecord.getNameRecordMultiField(NameServer.getRecordMap(), updatePacket.getName(), fields);
        try {
          if (nameRecord.containsActiveNameServer(NameServer.getNodeID())) {
            NameServer.getTcpTransport().sendToIDs(nameRecord.getActiveNameServers(), updatePacket.toJSONObject());
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
        ConfirmUpdateLNSPacket failConfirmPacket = ConfirmUpdateLNSPacket.createFailPacket(updatePacket, NSResponseCode.ERROR);
        // inform LNS of failed request
        NameServer.returnToSender(failConfirmPacket.toJSONObject(), updatePacket.getLocalNameServerId());

        if (StartNameServer.debugMode) {
          GNS.getLogger().fine(" UpdateRequest-InvalidNameServer\t" + updatePacket.getName()
                  + "\t" + NameServer.getNodeID() + "\t" + updatePacket.getLocalNameServerId());// + "\t" + updatePacket.getSequenceNumber());
        }
      }

      return;
    }

    if (StartNameServer.debugMode) {
      GNS.getLogger().fine(" Update Packet : " + updatePacket);
    }

    // Propose to paxos

    String activePaxosID = updatePacket.getName(); // nameRecord.getActivePaxosID();

    String paxosID = NameServer.getPaxosManager().propose(activePaxosID, new RequestPacket(updatePacket.getType().getInt(),
            updatePacket.toString(), PaxosPacketType.REQUEST, false));
    if (StartNameServer.debugMode) {
      GNS.getLogger().fine(" Update proposed to paxosID = " + paxosID);
    }

    if (paxosID == null) {
      ConfirmUpdateLNSPacket failConfirmPacket = ConfirmUpdateLNSPacket.createFailPacket(updatePacket, NSResponseCode.ERROR);
      // inform LNS of failed request
      NameServer.returnToSender(failConfirmPacket.toJSONObject(), updatePacket.getLocalNameServerId());

      if (StartNameServer.debugMode) {
        GNS.getLogger().fine(" UpdateRequest-InvalidNameServer\t" + updatePacket.getName()
                + "\t" + NameServer.getNodeID() + "\t" + updatePacket.getLocalNameServerId());// + "\t" + updatePacket.getSequenceNumber());
      }
    }

    long endTime = System.currentTimeMillis();

    if (endTime - startTime > 10) {
      GNS.getLogger().warning("Long latency HandleUpdate " + (endTime - startTime));
    }

  }

  private static ArrayList<ColumnField> dnsField = new ArrayList<ColumnField>();

  private static ArrayList<ColumnField> getDNSPacketFields() {
    synchronized (dnsField) {
      if (dnsField.size() == 0) {
        dnsField.add(NameRecord.ACTIVE_NAMESERVERS);
        dnsField.add(NameRecord.TIME_TO_LIVE);
      }
      return dnsField;
    }
  }

  private void handleDNSPacket() throws IOException, JSONException, InvalidKeyException,
          InvalidKeySpecException, NoSuchAlgorithmException, SignatureException {
    GNS.getLogger().fine("NS " + NameServer.getNodeID() + " received DNS lookup request: " + incomingJSON);
    DNSPacket dnsPacket = new DNSPacket(incomingJSON);

    // the only dns reponses we should see are coming in respone to SiteToSiteQueryHandler requests
    if (!dnsPacket.isQuery()) {
      // handle the special case of queries that were sent between name servers
      SiteToSiteQueryHandler.handleDNSResponsePacket(dnsPacket);
      return;
    }
    // First we do signature and ACL checks
    String guid = dnsPacket.getGuid();
    String field = dnsPacket.getKey().getName();
    String reader = dnsPacket.getAccessor();
    String signature = dnsPacket.getSignature();
    String message = dnsPacket.getMessage();
    // Check the signature and access
    NSResponseCode errorCode = NSResponseCode.NO_ERROR;
    if (reader != null) { // reader will be null for internal system reads
      errorCode = signatureAndACLCheck(guid, field, reader, signature, message, MetaDataTypeName.READ_WHITELIST);
    }
    // return an error packet if one of the checks doesn't pass
    if (errorCode.isAnError()) {
      dnsPacket.getHeader().setQRCode(DNSRecordType.RESPONSE);
      dnsPacket.getHeader().setResponseCode(errorCode);
      dnsPacket.setResponder(NameServer.getNodeID());
      GNS.getLogger().fine("Sending to " + dnsPacket.getSenderId() + " this error packet " + dnsPacket.toJSONObjectForErrorResponse());
      NameServer.returnToSender(dnsPacket.toJSONObjectForErrorResponse(), dnsPacket.getSenderId());
    } else {
      // All signature and ACL checks passed see if we can find the field to return;
      NameRecord nameRecord = null;
      // Try to look up the value in the database
      try {
        if (Defs.ALLFIELDS.equals(dnsPacket.getKey().getName())) {
          // need everything so just grab all the fields
          nameRecord = NameRecord.getNameRecord(NameServer.getRecordMap(), guid);
        } else {
          // otherwise grab a few system fields we need plus the field the user wanted
          nameRecord = NameRecord.getNameRecordMultiField(NameServer.getRecordMap(), guid, getDNSPacketFields(), field);
        }
      } catch (RecordNotFoundException e) {
        GNS.getLogger().fine("Record not found for name: " + guid + " Key = " + field);
      }
      // Now we either have a name record with stuff it in or a null one
      // Time to send something back to the client
      dnsPacket = checkAndMakeResponsePacket(dnsPacket, nameRecord);
      //GNS.getLogger().info("NS " + NameServer.getNodeID() + " returning DNS response: " + dnsPacket.toJSONObject());
      NameServer.returnToSender(dnsPacket.toJSONObject(), dnsPacket.getSenderId());
    }
  }

  private NSResponseCode signatureAndACLCheck(String guid, String field, String reader, String signature, String message, MetaDataTypeName access)
          throws InvalidKeyException, InvalidKeySpecException, SignatureException, NoSuchAlgorithmException {
    GuidInfo guidInfo, readerGuidInfo;
    if ((guidInfo = NSAccountAccess.lookupGuidInfo(guid)) == null) {
      GNS.getLogger().fine("Name " + guid + " key = " + field + ": BAD_GUID_ERROR");
      return NSResponseCode.BAD_GUID_ERROR;
    }
    if (reader.equals(guid)) {
      readerGuidInfo = guidInfo;
    } else if ((readerGuidInfo = NSAccountAccess.lookupGuidInfo(reader, true)) == null) {
      GNS.getLogger().fine("Name " + guid + " key = " + field + ": BAD_ACCESOR_ERROR");
      return NSResponseCode.BAD_ACCESOR_ERROR;
    }
    // unsigned case, must be world readable
    if (signature == null) {
      if (!NSAccessSupport.fieldAccessibleByEveryone(access, guidInfo.getGuid(), field)) {
        GNS.getLogger().fine("Name " + guid + " key = " + field + ": ACCESS_ERROR");
        return NSResponseCode.ACCESS_ERROR;
      }
      // signed case, check signature and access
    } else if (signature != null) {
      if (!NSAccessSupport.verifySignature(readerGuidInfo, signature, message)) {
        GNS.getLogger().fine("Name " + guid + " key = " + field + ": SIGNATURE_ERROR");
        return NSResponseCode.SIGNATURE_ERROR;
      } else if (!NSAccessSupport.verifyAccess(access, guidInfo, field, readerGuidInfo)) {
        GNS.getLogger().fine("Name " + guid + " key = " + field + ": ACCESS_ERROR");
        return NSResponseCode.ACCESS_ERROR;
      }
    }
    return NSResponseCode.NO_ERROR;
  }

  /**
   * Handles the normal case of returning a valid record plus 
   * a few different cases of the record not being found.
   * 
   * @param dnsPacket
   * @param nameRecord
   * @return 
   */
  private DNSPacket checkAndMakeResponsePacket(DNSPacket dnsPacket, NameRecord nameRecord) {
    dnsPacket.getHeader().setQRCode(DNSRecordType.RESPONSE);
    dnsPacket.setResponder(NameServer.getNodeID());
    // change it to a response packet
    String guid = dnsPacket.getGuid();
    String key = dnsPacket.getKey().getName();
    try {
      // Normative case... NameRecord was found and this server is one
      // of the active servers of the record
      if (nameRecord != null && nameRecord.containsActiveNameServer(NameServer.getNodeID())) {
        // how can we find a nameRecord if the guid is null?
        if (guid != null) {
          dnsPacket.setActiveNameServers(nameRecord.getActiveNameServers());
          //Generate the response packet
          // assume no error... change it below if there is an error
          dnsPacket.getHeader().setResponseCode(NSResponseCode.NO_ERROR);
          dnsPacket.setTTL(nameRecord.getTimeToLive());
          // Either returning one value or a bunch
          if (nameRecord.containsKey(key)) {
            dnsPacket.setSingleReturnValue(nameRecord.getKey(key));
            //GNS.getLogger().info("NS sending DNS lookup response: Name = " + guid);
          } else if (Defs.ALLFIELDS.equals(key)) {
            dnsPacket.setRecordValue(nameRecord.getValuesMap());
            //GNS.getLogger().info("NS sending multiple value DNS lookup response: Name = " + guid);
            // or we don't actually have the field
          } else { // send error msg.
            //GNS.getLogger().info("Record doesn't contain field: " + key + " name  = " + guid);
            dnsPacket.getHeader().setResponseCode(NSResponseCode.ERROR);
          }
          // For some reason the Guid of the packet is null
        } else { // send error msg.
          GNS.getLogger().fine("GUID of query is NULL!");
          dnsPacket.getHeader().setResponseCode(NSResponseCode.ERROR);
        }
        // we're not the correct active name server so tell the client that
      } else { // send invalid error msg.
        dnsPacket.getHeader().setResponseCode(NSResponseCode.ERROR_INVALID_ACTIVE_NAMESERVER);
        if (nameRecord == null) {
          GNS.getLogger().fine("Invalid actives. Name = " + guid);
        } else {
          GNS.getLogger().fine("Invalid actives. Name = " + guid + " Actives = " + nameRecord.getActiveNameServers());
        }
      }
    } catch (FieldNotFoundException e) {
      if (StartNameServer.debugMode) {
        GNS.getLogger().severe("Field not found exception: " + e.getMessage());
      }
      dnsPacket.getHeader().setResponseCode(NSResponseCode.ERROR);
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

    boolean sendError = false;
    try {
      ReplicaControllerRecord rcRecord = ReplicaControllerRecord.getNameRecordPrimaryMultiField(
              NameServer.getReplicaController(), packet.getName(), ReplicaControllerRecord.MARKED_FOR_REMOVAL,
              ReplicaControllerRecord.ACTIVE_NAMESERVERS);
      if (rcRecord.isMarkedForRemoval()) {
        sendError = true;
      } else { // send reply to client
        packet.setActiveNameServers(rcRecord.getActiveNameservers());
        NameServer.returnToSender(packet.toJSONObject(), packet.getLNSID());
        if (StartNameServer.debugMode) {
          GNS.getLogger().fine("Sent actives for " + packet.getName() + " Actives = " + rcRecord.getActiveNameservers());
        }
      }
    } catch (RecordNotFoundException e) {
      sendError = true;
    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field not found exception. " + e.getMessage());
      e.printStackTrace();
      return;
    }

    if (sendError) {
      packet.setActiveNameServers(null);
      NameServer.returnToSender(packet.toJSONObject(), packet.getLNSID());
      if (StartNameServer.debugMode) {
        GNS.getLogger().fine("Error: Record does not exist for " + packet.getName());
      }
    }

  }

  private void handleNameServerLoadPacket() throws JSONException {
    NameServerLoadPacket nsLoad = new NameServerLoadPacket(incomingJSON);
//    nsLoad.setLoadValue(NameServer.loadMonitor.getAverage());
//    NameServer.returnToSender(nsLoad.toJSONObject(), nsLoad.getLnsID());
  }
  /**************End of private methods in ClientRequestWorker**********************************/
}
