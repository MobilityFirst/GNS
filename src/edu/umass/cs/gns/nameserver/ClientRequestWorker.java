package edu.umass.cs.gns.nameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
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
  static ConcurrentHashMap<Integer, UpdateStatus> addInProgress = new ConcurrentHashMap<Integer, UpdateStatus>();

  public ClientRequestWorker(JSONObject json, Packet.PacketType packetType) {
    this.packetType = packetType;
    this.incomingJSON = json;
  }

  public static void handleIncomingPacket(JSONObject json, Packet.PacketType packetType) {
//      Random r = new Random();
//      NameServer.timer.schedule(new ClientRequestWorker(json), r.nextInt(500));

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
    ReplicaControllerRecord rcRecord = NameServer.getNameRecordPrimaryLazy(packet.getName());
    if (rcRecord!= null) {
      GNS.getLogger().fine(" Name Add Complete: " + packet.getName());
      rcRecord.setAdded();
    }
    else {
      GNS.getLogger().severe(" Error: Exception: Name does not exist: " + packet.getName());
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
    ReplicaControllerRecord rcRecord = NameServer.getNameRecordPrimary(name);

    // Abhigyan:
    // if GUID exists in DB:
    //       if deleted OR ReplicaControllerRecord.isAdded() == true:
    //            we will send an error to client saying record exist
    //       if add is not complete, i.e., ReplicaControllerRecord.isAdded() == false:
    //            we will add the record assuming that the same user is trying to add the record

    if (rcRecord != null) {     // && nameRecord.containsKey(nameRecordKey.getName())
      // we send back a confirmation with a failure flag
      if (rcRecord.isAdded() ||  rcRecord.isMarkedForRemoval()) {
      GNS.getLogger().info(" ADD (ns " + NameServer.nodeID+ ") : Record already exists");


      ConfirmUpdateLNSPacket confirmPacket = new ConfirmUpdateLNSPacket(
              NameServer.nodeID, false, addRecordPacket);
      JSONObject jsonConfirm = confirmPacket.toJSONObject();
      NameServer.tcpTransport.sendToID(addRecordPacket.getLocalNameServerID(), jsonConfirm);
      return;
      }

    }

    // prepare confirm packet, and create an update status object
    ConfirmUpdateLNSPacket confirmPacket = new ConfirmUpdateLNSPacket(NameServer.nodeID, true, addRecordPacket);

    Set<Integer> primaryNameServers = HashFunction.getPrimaryReplicas(addRecordPacket.getName());
    UpdateStatus status = new UpdateStatus(addRecordPacket.getLocalNameServerID(), primaryNameServers, confirmPacket);
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
    valuesMap.put(nameRecordKey.getName(), new QueryResultValue(value));
    rcRecord = new ReplicaControllerRecord(name);
    GNS.getLogger().fine(" Replica controller record created for name: " + rcRecord.getName());
//      GNS.getLogger().fine(" Full record: "  + rcRecord);
    if (NameServer.getNameRecordPrimary(name) == null)
      NameServer.addNameRecordPrimary(rcRecord);
    else
      NameServer.updateNameRecordPrimary(rcRecord);
    ReplicaController.handleNameRecordAddAtPrimary(rcRecord, valuesMap);

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

    ReplicaControllerRecord rcRecord = NameServer.getNameRecordPrimary(name);//NameServer.getNameRecord(name);

    if (rcRecord == null) {
      // create and add the record

      rcRecord = new ReplicaControllerRecord(name);
      NameServer.addNameRecordPrimary(rcRecord);
//      NameServer.addNameRecord(record);
    }
    ValuesMap valuesMap = new ValuesMap();
    valuesMap.put(addRecordPacket.getRecordKey().getName(), new QueryResultValue(addRecordPacket.getValue()));
    ReplicaController.handleNameRecordAddAtPrimary(rcRecord, valuesMap);

    ConfirmAddNSPacket pkt = new ConfirmAddNSPacket(addRecordPacket.getLNSRequestID(), NameServer.nodeID);
//    NameServer.tcpTransport.sendToID( pkt.toJSONObject(), addRecordPacket.getLocalNameServerID(), PortType.PERSISTENT_TCP_PORT);
    NameServer.tcpTransport.sendToID(addRecordPacket.getLocalNameServerID(), pkt.toJSONObject());
  }


  private void handleConfirmAddNS() throws JSONException, IOException {
//    GNS.getLogger().info("here asdf");
    ConfirmAddNSPacket packet = new ConfirmAddNSPacket(incomingJSON);
    UpdateStatus status = addInProgress.get(packet.getPacketID());
    if (status == null) return;

    status.addNameServerResponded(packet.getNameServerID());
    if (status.haveMajorityNSSentResponse() == false) return;

    status = addInProgress.remove(packet.getPacketID());
    if (status == null) return;


    JSONObject jsonConfirm = status.getConfirmUpdateLNSPacket().toJSONObject();
    GNS.getLogger().info("Sending ADD REQUEST CONFIRM (ns " + NameServer.nodeID+ ") : to "
            +  + status.getConfirmUpdateLNSPacket().getLocalNameServerId());
    GNS.getLogger().info("Sending ADD REQUEST CONFIRM to LNS " + jsonConfirm);
    NameServer.tcpTransport.sendToID(status.getLocalNameServerID(), jsonConfirm);

    // confirm to every one that add is complete
    AddCompletePacket pkt2 = new AddCompletePacket(status.getConfirmUpdateLNSPacket().getName());
//    NameServer.tcpTransport.sendToAll(pkt2.toJSONObject(),status.getAllNameServers(),PortType.PERSISTENT_TCP_PORT);
    NameServer.tcpTransport.sendToIDs(status.getAllNameServers(), pkt2.toJSONObject());
    GNS.getLogger().info("Sending AddCompletePacket to all NS" + jsonConfirm);

  }


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

  private void handleUpdateAddressLNS() throws JSONException, IOException {
    long t0 = System.currentTimeMillis();
    if (StartNameServer.debugMode) {
      GNS.getLogger().fine(" Recvd Update Address from LNS: " + incomingJSON);
    }

    UpdateAddressPacket updatePacket = new UpdateAddressPacket(incomingJSON);

    // IF this is an UPSERT operation
    if (updatePacket.getOperation().isUpsert()) {
      // this must be primary
      ReplicaControllerRecord nameRecordPrimary = NameServer.getNameRecordPrimaryLazy(updatePacket.getName());
      //ReplicaControllerRecord nameRecordPrimary = NameServer.getNameRecordPrimaryLazy(updatePacket.getName());
      if (nameRecordPrimary!= null && nameRecordPrimary.isMarkedForRemoval()) {
        // if name record is deleted, no further operation for this GUID.
        // send failure to client
        ConfirmUpdateLNSPacket failConfirmPacket =
                ConfirmUpdateLNSPacket.createFailPacket(updatePacket, NameServer.nodeID);
        NameServer.tcpTransport.sendToID(updatePacket.getLocalNameServerId(), failConfirmPacket.toJSONObject());
        if (StartNameServer.debugMode)  GNS.getLogger().fine(" UPSERT-FAILED because name record deleted already\t" + updatePacket.getName()
                + "\t" + NameServer.nodeID + "\t" + updatePacket.getLocalNameServerId() + "\t" + updatePacket.getSequenceNumber());
      }
      else if (nameRecordPrimary == null ) {
        // do an INSERT (AKA ADD) operation

        AddRecordPacket addRecordPacket = new AddRecordPacket(updatePacket.getRequestID(), updatePacket.getName(),
                updatePacket.getRecordKey(), updatePacket.getUpdateValue(), updatePacket.getLocalNameServerId());
        addRecordPacket.setLNSRequestID(updatePacket.getLNSRequestID());
        incomingJSON = addRecordPacket.toJSONObject();
        handleAddRecordLNSPacket();
        if (StartNameServer.debugMode) {
          GNS.getLogger().fine(" NS processing UPSERT changed to ADD: "
                  + incomingJSON);
        }
        // Name record was found.. do the update
      } else {
        int activeID = -1;
        Set<Integer> activeNS = nameRecordPrimary.copyActiveNameServers();
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
//          if (updatePacket.getOperation().equals(UpdateOperation.APPEND_OR_CREATE)) {
//            updatePacket.setOperation(UpdateOperation.APPEND);
//          } else if (updatePacket.getOperation().equals(UpdateOperation.REPLACE_ALL_OR_CREATE)) {
//            updatePacket.setOperation(UpdateOperation.REPLACE_ALL);
//          }
          if (StartNameServer.debugMode) {
            GNS.getLogger().fine("UPSERT forwarded as UPDATE to active: " + activeID);
          }
//          NameServer.tcpTransport.sendToID(updatePacket.toJSONObject(), activeID, PortType.PERSISTENT_TCP_PORT);
          NameServer.tcpTransport.sendToID(activeID, updatePacket.toJSONObject());
          // could not find activeNS for this name
        } else {
          // send error to LNS
          ConfirmUpdateLNSPacket failConfirmPacket =
                  ConfirmUpdateLNSPacket.createFailPacket(updatePacket, NameServer.nodeID);
          NameServer.tcpTransport.sendToID(updatePacket.getLocalNameServerId(),failConfirmPacket.toJSONObject());
//          NSListenerUDP.udpTransport.sendPacket(failConfirmPacket.toJSONObject(),
//                  updatePacket.getLocalNameServerId(), GNS.PortType.LNS_UDP_PORT);
          String msg = " UPSERT-FAILED\t" + updatePacket.getName()
                  + "\t" + NameServer.nodeID + "\t" + updatePacket.getLocalNameServerId() + "\t" + updatePacket.getSequenceNumber();
          if (StartNameServer.debugMode) {
            GNS.getLogger().fine(msg);
          }
        }
      }
      // END OF UPSERT HANDLING
    } else {
      // HANDLE NON-UPSERT CASE

      //NameRecord nameRecord = NameServer.getNameRecord(updatePacket.getName());
      NameRecord nameRecord = NameServer.getNameRecordLazy(updatePacket.getName());
      //NameRecord nameRecord = DBNameRecord.getNameRecord(updatePacket.getName());

      if (nameRecord == null || !nameRecord.containsActiveNameServer(NameServer.nodeID)) {
        ConfirmUpdateLNSPacket failConfirmPacket =
                ConfirmUpdateLNSPacket.createFailPacket(updatePacket, NameServer.nodeID);
        // inform LNS of failed request
        NameServer.tcpTransport.sendToID(updatePacket.getLocalNameServerId(),failConfirmPacket.toJSONObject());
//        NSListenerUDP.udpTransport.sendPacket(failConfirmPacket.toJSONObject(),
//                updatePacket.getLocalNameServerId(), GNS.PortType.LNS_UDP_PORT);
        String msg = " UpdateRequest-InvalidNameServer\t" + updatePacket.getName()
                + "\t" + NameServer.nodeID + "\t" + updatePacket.getLocalNameServerId() + "\t" + updatePacket.getSequenceNumber();
        if (StartNameServer.debugMode) {
          GNS.getLogger().fine(msg);
        }
      } else {
        if (StartNameServer.debugMode) {
          GNS.getLogger().fine(" PAXOS PROPOSAL: propose update request: " + updatePacket.toString());
        }

        // Save request info to send confirmation to LNS later.
        updatePacket.setNameServerId(NameServer.nodeID); // to check that I had proposed this message
        int updateID = incrementUpdateID();
        updatePacket.setNSRequestID(updateID); // to check the request ID.
        updatePacket.setType(Packet.PacketType.UPDATE_ADDRESS_NS); //
        ConfirmUpdateLNSPacket confirmPacket = ConfirmUpdateLNSPacket.createSuccessPacket(updatePacket,
                NameServer.nodeID, nameRecord.copyActiveNameServers().size());
        proposedUpdates.put(updateID, confirmPacket);
        proposedUpdatesTime.put(updateID, System.currentTimeMillis());
        if (StartNameServer.debugMode) {
          GNS.getLogger().fine(" Update Packet : " + updatePacket);
        }
        // Propose to paxos
        String activePaxosID = nameRecord.getActivePaxosID();
        if (StartNameServer.debugMode) {
          GNS.getLogger().fine(" Update proposed to paxosID = " + activePaxosID);
        }
        PaxosManager.propose(activePaxosID, new RequestPacket(updatePacket.getType().getInt(),
                updatePacket.toString(), PaxosPacketType.REQUEST, false));
        if (StartNameServer.debugMode) {
          GNS.getLogger().fine(" Update Packet Type : " + updatePacket.getType().getInt());
        }
      }
      long t1 = System.currentTimeMillis();
      if (t1 - t0 > 50) {
        if (StartNameServer.debugMode) {
          GNS.getLogger().severe("UpdateLongDelay = " + (t1 - t0) + " in handleUpdateAddressNS UpdatePacket = "
                  + updatePacket + " NameRecord = " + nameRecord);
        }
      }
    }
  }

  private void handleUpdateAddressNS() throws JSONException, IOException {

    if (StartNameServer.debugMode) {
      GNS.getLogger().fine("PAXOS DECISION: Update Confirmed... going LAZY  " + incomingJSON);
    }
    UpdateAddressPacket updatePacket = new UpdateAddressPacket(incomingJSON);
    //NameRecord nameRecord = NameServer.getNameRecord(updatePacket.getName());
    NameRecord nameRecord = NameServer.getNameRecordLazy(updatePacket.getName());
    //NameRecord nameRecord = DBNameRecord.getNameRecord(updatePacket.getName());


//    long t0 = System.currentTimeMillis();
    if (nameRecord != null) {
      // Apply update
      GNS.getLogger().fine("NAME RECORD is: " + nameRecord.toString());
      boolean result = nameRecord.updateField(updatePacket.getRecordKey().getName(), updatePacket.getUpdateValue(),
              updatePacket.getOldValue(), updatePacket.getOperation());
      if (!result) { // update failed
        if (StartNameServer.debugMode) {
          GNS.getLogger().fine("Update operation failed " + incomingJSON);
        }
        if (updatePacket.getNameServerId() == NameServer.nodeID) { //if this node proposed this update
          // send error message to client
          ConfirmUpdateLNSPacket confirmUpdateLNSPacket = proposedUpdates.remove(updatePacket.getNSRequestID());

          confirmUpdateLNSPacket.convertToFailPacket();

          NameServer.tcpTransport.sendToID(updatePacket.getLocalNameServerId(),confirmUpdateLNSPacket.toJSONObject());
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

        // if I had proposed this update, increment update count
        nameRecord.incrementUpdateRequest();

        //
        ConfirmUpdateLNSPacket confirmPacket = proposedUpdates.remove(updatePacket.getNSRequestID());
        Long t_1 = proposedUpdatesTime.remove(updatePacket.getNSRequestID());
//        if (t_1 != null && t0 - t_1 > 50) {
//          if (StartNameServer.debugMode) {
//            GNS.getLogger().severe("UpdateLongDelay = " + (t0 - t_1) + " in between Paxos propose & decision. UpdatePacket = "
//                    + updatePacket + " NameRecord = " + nameRecord);
//          }
//        }

        if (confirmPacket != null) {
          // send confirmation to
          NameServer.tcpTransport.sendToID(confirmPacket.getLocalNameServerId(),confirmPacket.toJSONObject());
//          NSListenerUDP.udpTransport.sendPacket(confirmPacket.toJSONObject(),
//                  confirmPacket.getLocalNameServerId(), GNS.PortType.LNS_UDP_PORT);
          if (StartNameServer.debugMode) {
            GNS.getLogger().fine("NS Sent confirmation to local name server. Sent packet: " + confirmPacket.toJSONObject());
          }
        }
      }
      //DBNameRecord.updateNameRecord(nameRecord);
      NameServer.updateNameRecord(nameRecord);
      if (StartNameServer.debugMode) {
        GNS.getLogger().fine("Saved name record to database.");
      }
    }
//    long t1 = System.currentTimeMillis();
//    if (t1 - t0 > 50) {
//      if (StartNameServer.debugMode) {
//        GNS.getLogger().severe("UpdateLongDelay = " + (t1 - t0) + " in handleUpdateAddressLNS UpdatePacket = "
//                + updatePacket + "NameRecord = " + nameRecord);
//      }
//    }
  }

  private static ConcurrentHashMap<Integer, ConfirmUpdateLNSPacket> proposedUpdates = new ConcurrentHashMap<Integer, ConfirmUpdateLNSPacket>();

  private static ConcurrentHashMap<Integer, Long> proposedUpdatesTime = new ConcurrentHashMap<Integer, Long>();


  private void handleDNSPacket() throws IOException, JSONException {

    if (StartNameServer.debugMode) {
      GNS.getLogger().fine("NS recvd DNS lookup request: " + incomingJSON);
    }
//    long requestRecvdTime = System.currentTimeMillis();
//    long t1 = System.currentTimeMillis();

//    InetAddress address = InetAddress.getByName(Transport.getReturnAddress(incomingJSON));    //Sender's address
//    int port = Transport.getReturnPort(incomingJSON);                //Sender's port

//    long t2 = System.currentTimeMillis();

    DNSPacket dnsPacket = new DNSPacket(incomingJSON);
    int sender = dnsPacket.getSender();

//    long t3 = System.currentTimeMillis();
    NameRecord nameRecord = NameServer.getNameRecord(dnsPacket.getQname());
    //NameRecord nameRecord = DBNameRecord.getNameRecord(dnsPacket.getQname());
//    long t4 = System.currentTimeMillis();
//    long t5 = 0;
//    long t6 = 0;
//    long t7 = 0;
    // check if this is current set of ACTIVES (not primary!).
    if (nameRecord != null && nameRecord.containsActiveNameServer(NameServer.nodeID)) {

//            dnsPacket.setTTL(nameRecord);
      dnsPacket = NameServer.makeResponseFromRecord(dnsPacket, nameRecord);
//      t5 = System.currentTimeMillis();
      JSONObject outgoingJSON = dnsPacket.toJSONObject();
//      NameServer.tcpTransport.sendToID()
      NameServer.tcpTransport.sendToID(sender,outgoingJSON);
//      NSListenerUDP.udpTransport.sendPacket(outgoingJSON, address, port);
//      t7 = System.currentTimeMillis();
      if (StartNameServer.debugMode) {
        GNS.getLogger().fine("NS sent DNS lookup response: Name = " + dnsPacket.getQname());
      }
//      NameServer.updateNameRecord(nameRecord);
      //DBNameRecord.updateNameRecord(nameRecord);


    } else { // send error msg.
      if (StartNameServer.debugMode) {
        GNS.getLogger().fine("Invalid actives. Name: " + dnsPacket.getQname());
      }

      dnsPacket.getHeader().setRcode(DNSRecordType.RCODE_ERROR_INVALID_ACTIVE_NAMESERVER);
      dnsPacket.getHeader().setQr(DNSRecordType.RESPONSE);
      NameServer.tcpTransport.sendToID(sender,dnsPacket.toJSONObject());
    }

//    long responseTime = System.currentTimeMillis() - requestRecvdTime;
    // TODO update average response time for load balancing
//    if (responseTime > 100) {
//      if (StartNameServer.debugMode) {
//        GNS.getLogger().severe("respTime\t" + responseTime + "\t"
//                + "\tt2-t1\t" + (t2 - t1) + "\tt3-t2\t" + (t3 - t2) + "\tt4-t3\t" + (t4 - t3) + "\tt5-t4\t" + (t5 - t4)
//                + "\tt6-t5\t" + (t6 - t5) + "\tt7-t6\t" + (t7 - t6));
//      }
//    }

  }

  /**
   * @throws JSONException
   */
  private void handleRequestActivesPacket() throws JSONException, IOException {
    if (StartNameServer.debugMode) GNS.getLogger().fine("NS recvd request actives packet " + incomingJSON);

    RequestActivesPacket packet = new RequestActivesPacket(incomingJSON);
    if (StartNameServer.debugMode) GNS.getLogger().fine("Name = " + packet.getName());
    //ReplicaControllerRecord nameRecordPrimary = NameServer.getNameRecordPrimaryLazy(packet.getName());
    ReplicaControllerRecord rcRecord = NameServer.getNameRecordPrimaryLazy(packet.getName());
    if (StartNameServer.debugMode) GNS.getLogger().fine("Values: " + rcRecord);
//    GNS.getLogger().fine("Values: " + rcRecord.isMarkedForRemoval());
//    GNS.getLogger().fine("Values: " + rcRecord.isPrimaryReplica());
    if (rcRecord != null && rcRecord.isMarkedForRemoval() == false && rcRecord.isPrimaryReplica()) {
      packet.setActiveNameServers(rcRecord.copyActiveNameServers());
      NameServer.tcpTransport.sendToID(packet.getLNSID(), packet.toJSONObject());
      if (StartNameServer.debugMode) {
        GNS.getLogger().fine("Sent actives for " + packet.getName() //+ " " + packet.getRecordKey()
                + " Actives = " + rcRecord.copyActiveNameServers());
      }
    } else {
      // if active == null, then name record does not exist.
      packet.setActiveNameServers(null);
      NameServer.tcpTransport.sendToID(packet.getLNSID(), packet.toJSONObject());
      if (StartNameServer.debugMode) {
        GNS.getLogger().fine("Error: Record does not exist for " + packet.getName());
      }
//            NameServer.isPrimaryNameServer(packet.getName(), packet.getRecordKey());
    }
  }
}


class UpdateStatus{
  private int localNameServerID;
  private Set<Integer> allNameServers;
  private Set<Integer> nameServersResponded;

  private ConfirmUpdateLNSPacket confirmUpdate;

  public UpdateStatus(int localNameServerID, Set<Integer> allNameServers,
                      ConfirmUpdateLNSPacket confirmUpdate) {
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

  public void addNameServerResponded(int nameServerID) {
    nameServersResponded.add(nameServerID);
  }

  public boolean haveMajorityNSSentResponse() {
    GNS.getLogger().fine("All ns size:" + allNameServers.size());
    GNS.getLogger().fine("Responded ns size:" + nameServersResponded.size());
    if (allNameServers.size() == 0) return true;
    if (nameServersResponded.size() * 2 > allNameServers.size()) return true;
    return false;
  }

}
