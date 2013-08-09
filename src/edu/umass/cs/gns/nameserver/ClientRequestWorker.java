package edu.umass.cs.gns.nameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.GNS.PortType;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaController;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaControllerRecord;
import edu.umass.cs.gns.packet.AddRecordPacket;
import edu.umass.cs.gns.packet.ConfirmUpdateLNSPacket;
import edu.umass.cs.gns.packet.DNSPacket;
import edu.umass.cs.gns.packet.DNSRecordType;
import edu.umass.cs.gns.packet.Packet;
import edu.umass.cs.gns.packet.QueryResultValue;
import edu.umass.cs.gns.packet.RequestActivesPacket;
import edu.umass.cs.gns.packet.Transport;
import edu.umass.cs.gns.packet.UpdateAddressPacket;
import edu.umass.cs.gns.packet.UpdateOperation;
import edu.umass.cs.gns.packet.paxospacket.PaxosPacketType;
import edu.umass.cs.gns.packet.paxospacket.RequestPacket;
import org.json.JSONException;
import org.json.JSONObject;
import edu.umass.cs.gns.paxos.PaxosManager;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Handle client requests - ADD/REMOVE/LOOKUP/UPDATE + REQUESTACTIVES
 *
 * @author abhigyan
 */
public class ClientRequestWorker extends TimerTask {

  JSONObject incomingJSON;

  public ClientRequestWorker(JSONObject json) {
    this.incomingJSON = json;
  }

  public static void handleIncomingPacket(JSONObject json) {
//      Random r = new Random();
//      NameServer.timer.schedule(new ClientRequestWorker(json), r.nextInt(500));
    NameServer.executorService.submit(new ClientRequestWorker(json));
  }

  @Override
  public void run() {
    try {
      switch (Packet.getPacketType(incomingJSON)) {
        // ADD
        case ADD_RECORD_LNS:
          // receive from LNS, and send to other primaries
          handleAddRecordLNSPacket();
          break;
        case ADD_RECORD_NS:
          // receive from primary which received the request
          handleAddRecordNS();
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
        case DNS:
          // LOOKUP
          handleDNSPacket();
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

  private void handleAddRecordLNSPacket() throws JSONException, IOException {
    AddRecordPacket addRecordPacket;
    String name;
    NameRecordKey nameRecordKey;
    ArrayList<String> value;
    addRecordPacket = new AddRecordPacket(incomingJSON);
    name = addRecordPacket.getName();
    nameRecordKey = addRecordPacket.getRecordKey();
    value = addRecordPacket.getValue();
    GNS.getLogger().info(
            "NSListenerUpdate ADD FROM LNS (ns " + NameServer.nodeID
                    + ") : " + name + "/" + nameRecordKey.toString() + ", "
                    + value);
    ReplicaControllerRecord nameRecord = NameServer.getNameRecordPrimary(name);
    //NameServer.getNameRecord(name);

    // assuming this is a primary name server for this "name".

    // if name record is already created by this name.
    if (nameRecord != null) {     // && nameRecord.containsKey(nameRecordKey.getName())
      // if the name record already exists and already contains the key 
      // we send back a confirmation with a failure flag

      GNS.getLogger().info("NSListenerUpdate ADD (ns " + NameServer.nodeID+ ") : Record already exists");

      ConfirmUpdateLNSPacket confirmPacket = new ConfirmUpdateLNSPacket(
              NameServer.nodeID, false, addRecordPacket);
      JSONObject jsonConfirm = confirmPacket.toJSONObject();
      NSListenerUDP.udpTransport.sendPacket(jsonConfirm,
              addRecordPacket.getLocalNameServerID(),
              PortType.LNS_UPDATE_PORT);

    } else {
      // change the packet type
      addRecordPacket.setType(Packet.PacketType.ADD_RECORD_NS);
      // recreate the json object
      JSONObject outgoingJSON = addRecordPacket.toJSONObject();
      Set<Integer> primaryNameServers = addRecordPacket
              .getPrimaryNameServers();
      GNS.getLogger().info(
              "NSListenerUpdate ADD FROM LNS (ns " + NameServer.nodeID
                      + ") : " + name + "/" + nameRecordKey.toString() + ", "
                      + value + " - SENDING TO OTHER PRIMARIES: " + primaryNameServers.toString());
      // send to all the other primaries except us
      NameServer.tcpTransport.sendToAll(outgoingJSON, primaryNameServers,
              PortType.STATS_PORT, NameServer.nodeID);
      ValuesMap valuesMap = new ValuesMap();
      valuesMap.put(nameRecordKey.getName(), new QueryResultValue(value));
      nameRecord = new ReplicaControllerRecord(name);
      NameServer.addNameRecordPrimary(nameRecord);
      ReplicaController.handleNameRecordAddAtPrimary(nameRecord, valuesMap);
      GNS.getLogger().info("");
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
      ConfirmUpdateLNSPacket confirmPacket = new ConfirmUpdateLNSPacket(
              NameServer.nodeID, true, addRecordPacket);
      JSONObject jsonConfirm = confirmPacket.toJSONObject();
      GNS.getLogger().info(
              "NSListenerUpdate CONFIRM (ns " + NameServer.nodeID
                      + ") : to "
                      + addRecordPacket.getLocalNameServerID() + ":"
                      + GNS.PortType.LNS_UPDATE_PORT + " : "
                      + jsonConfirm.toString());

      NSListenerUDP.udpTransport.sendPacket(jsonConfirm,
              addRecordPacket.getLocalNameServerID(),
              GNS.PortType.LNS_UPDATE_PORT);
    }
  }

  private void handleAddRecordNS() throws JSONException, UnknownHostException {

    AddRecordPacket addRecordPacket;
    String name;
    NameRecordKey nameRecordKey;
    ArrayList<String> value;
    addRecordPacket = new AddRecordPacket(incomingJSON);
    name = addRecordPacket.getName();
    nameRecordKey = addRecordPacket.getRecordKey();
    value = addRecordPacket.getValue();
    if (StartNameServer.debugMode) {
      GNS.getLogger().info("NSListenerUpdate ADD FROM NS (ns " + NameServer.nodeID + ") : "
              + name + "/" + nameRecordKey.toString() + ", " + value);
    }

    ReplicaControllerRecord nameRecordPrimary = NameServer.getNameRecordPrimary(name);//NameServer.getNameRecord(name);
    if (nameRecordPrimary == null) {
      // create and add the record

      nameRecordPrimary = new ReplicaControllerRecord(name);
      NameServer.addNameRecordPrimary(nameRecordPrimary);
//      NameServer.addNameRecord(record);
    }
    ValuesMap valuesMap = new ValuesMap();
    valuesMap.put(addRecordPacket.getRecordKey().getName(), new QueryResultValue(addRecordPacket.getValue()));
    ReplicaController.handleNameRecordAddAtPrimary(nameRecordPrimary, valuesMap);


  }
  /**
   * ID assigned to updates received from LNS. The next update from a LNS will be assigned id = updateIDCount + 1;
   */
  private static Integer updateIDcount = 0;

  static int incrementUpdateID() {
    synchronized (updateIDcount) {
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
      ReplicaControllerRecord nameRecordPrimary = NameServer.getNameRecordPrimary(updatePacket.getName());
      //ReplicaControllerRecord nameRecordPrimary = NameServer.getNameRecordPrimaryLazy(updatePacket.getName());
      if (nameRecordPrimary!= null && nameRecordPrimary.isMarkedForRemoval()) {
      // if name record is deleted, no further operation for this GUID.
        // send failure to client
        ConfirmUpdateLNSPacket failConfirmPacket =
                ConfirmUpdateLNSPacket.createFailPacket(updatePacket, NameServer.nodeID);
        NSListenerUDP.udpTransport.sendPacket(failConfirmPacket.toJSONObject(),
                updatePacket.getLocalNameServerId(), GNS.PortType.LNS_UPDATE_PORT);
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
          if (updatePacket.getOperation().equals(UpdateOperation.APPEND_OR_CREATE)) {
            updatePacket.setOperation(UpdateOperation.APPEND);
          } else if (updatePacket.getOperation().equals(UpdateOperation.REPLACE_ALL_OR_CREATE)) {
            updatePacket.setOperation(UpdateOperation.REPLACE_ALL);
          }
          if (StartNameServer.debugMode) {
            GNS.getLogger().fine("UPSERT forwarded as UPDATE to active: " + activeID);
          }
          NameServer.tcpTransport.sendToID(updatePacket.toJSONObject(), activeID, PortType.STATS_PORT);
          // could not find activeNS for this name
        } else {
          // send error to LNS
          ConfirmUpdateLNSPacket failConfirmPacket =
                  ConfirmUpdateLNSPacket.createFailPacket(updatePacket, NameServer.nodeID);
          NSListenerUDP.udpTransport.sendPacket(failConfirmPacket.toJSONObject(),
                  updatePacket.getLocalNameServerId(), GNS.PortType.LNS_UPDATE_PORT);
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
        NSListenerUDP.udpTransport.sendPacket(failConfirmPacket.toJSONObject(),
                updatePacket.getLocalNameServerId(), GNS.PortType.LNS_UPDATE_PORT);
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

  private void handleUpdateAddressNS() throws JSONException {

    if (StartNameServer.debugMode) {
      GNS.getLogger().fine("PAXOS DECISION: Update Confirmed... going LAZY  " + incomingJSON);
    }
    UpdateAddressPacket updatePacket = new UpdateAddressPacket(incomingJSON);
    //NameRecord nameRecord = NameServer.getNameRecord(updatePacket.getName());
    NameRecord nameRecord = NameServer.getNameRecordLazy(updatePacket.getName());
    //NameRecord nameRecord = DBNameRecord.getNameRecord(updatePacket.getName());


    long t0 = System.currentTimeMillis();
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


          NSListenerUDP.udpTransport.sendPacket(confirmUpdateLNSPacket.toJSONObject(),
                  updatePacket.getLocalNameServerId(), GNS.PortType.LNS_UPDATE_PORT);
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
        if (t_1 != null && t0 - t_1 > 50) {
          if (StartNameServer.debugMode) {
            GNS.getLogger().severe("UpdateLongDelay = " + (t0 - t_1) + " in between Paxos propose & decision. UpdatePacket = "
                    + updatePacket + " NameRecord = " + nameRecord);
          }
        }

        if (confirmPacket != null) {
          // send confirmation to
          NSListenerUDP.udpTransport.sendPacket(confirmPacket.toJSONObject(),
                  confirmPacket.getLocalNameServerId(), GNS.PortType.LNS_UPDATE_PORT);
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
    long t1 = System.currentTimeMillis();
    if (t1 - t0 > 50) {
      if (StartNameServer.debugMode) {
        GNS.getLogger().severe("UpdateLongDelay = " + (t1 - t0) + " in handleUpdateAddressLNS UpdatePacket = "
                + updatePacket + "NameRecord = " + nameRecord);
      }
    }
  }
  private static ConcurrentHashMap<Integer, ConfirmUpdateLNSPacket> proposedUpdates = new ConcurrentHashMap<Integer, ConfirmUpdateLNSPacket>();
  private static ConcurrentHashMap<Integer, Long> proposedUpdatesTime = new ConcurrentHashMap<Integer, Long>();

  private void handleDNSPacket() throws UnknownHostException, JSONException {

    if (StartNameServer.debugMode) {
      GNS.getLogger().fine("NS " + NameServer.nodeID + " RECVD DNS PACKET: " + incomingJSON);
    }
    long requestRecvdTime = System.currentTimeMillis();
    long t1 = System.currentTimeMillis();

    InetAddress address = InetAddress.getByName(Transport.getReturnAddress(incomingJSON));    //Sender's address
    int port = Transport.getReturnPort(incomingJSON);                //Sender's port

    long t2 = System.currentTimeMillis();

    DNSPacket dnsPacket = new DNSPacket(incomingJSON);

    long t3 = System.currentTimeMillis();
    NameRecord nameRecord = NameServer.getNameRecord(dnsPacket.getQname());
    //NameRecord nameRecord = DBNameRecord.getNameRecord(dnsPacket.getQname());
    long t4 = System.currentTimeMillis();
    long t5 = 0;
    long t6 = 0;
    long t7 = 0;
    // check if this is current set of ACTIVES (not primary!).
    if (nameRecord != null && nameRecord.containsActiveNameServer(NameServer.nodeID)) {

//            dnsPacket.setTTL(nameRecord);
      dnsPacket = NameServer.makeResponseFromRecord(dnsPacket, nameRecord);
      t5 = System.currentTimeMillis();
      JSONObject outgoingJSON = dnsPacket.toJSONObject();
      NSListenerUDP.udpTransport.sendPacket(outgoingJSON, address, port);
      t7 = System.currentTimeMillis();
      if (StartNameServer.debugMode) {
        GNS.getLogger().fine("NS SENT DNS PACKET: " + outgoingJSON);
      }
      NameServer.updateNameRecord(nameRecord);
      //DBNameRecord.updateNameRecord(nameRecord);


    } else { // send error msg.
      if (StartNameServer.debugMode) {
        GNS.getLogger().fine("Invalid Active");
      }

      dnsPacket.getHeader().setRcode(DNSRecordType.RCODE_ERROR_INVALID_ACTIVE_NAMESERVER);
      dnsPacket.getHeader().setQr(DNSRecordType.RESPONSE);
      NSListenerUDP.udpTransport.sendPacket(dnsPacket.toJSONObject(), address, port);
    }

    long responseTime = System.currentTimeMillis() - requestRecvdTime;
    if (responseTime > 100) {
      if (StartNameServer.debugMode) {
        GNS.getLogger().severe("respTime\t" + responseTime + "\t"
                + "\tt2-t1\t" + (t2 - t1) + "\tt3-t2\t" + (t3 - t2) + "\tt4-t3\t" + (t4 - t3) + "\tt5-t4\t" + (t5 - t4)
                + "\tt6-t5\t" + (t6 - t5) + "\tt7-t6\t" + (t7 - t6));
      }
    }

  }

  /**
   * @throws JSONException
   */
  private void handleRequestActivesPacket() throws JSONException {
    if (StartNameServer.debugMode) {
      GNS.getLogger().fine("NS RECVD REQUEST ACTIVES PACKET." + incomingJSON);
    }
    RequestActivesPacket packet = new RequestActivesPacket(incomingJSON);
    //ReplicaControllerRecord nameRecordPrimary = NameServer.getNameRecordPrimaryLazy(packet.getName());
    ReplicaControllerRecord nameRecordPrimary = NameServer.getNameRecordPrimary(packet.getName());
    if (nameRecordPrimary != null && nameRecordPrimary.isMarkedForRemoval() == false && nameRecordPrimary.isPrimaryReplica()) {
      packet.setActiveNameServers(nameRecordPrimary.copyActiveNameServers());
      NSListenerUDP.udpTransport.sendPacket(packet.toJSONObject(), packet.getLNSID(), PortType.LNS_UPDATE_PORT);
      if (StartNameServer.debugMode) {
        GNS.getLogger().fine("SENT ACTIVES FOR " + packet.getName() //+ " " + packet.getRecordKey()
                + " Actives = " + nameRecordPrimary.copyActiveNameServers());
      }
    } else {
      // if active == null, then name record does not exist.
      packet.setActiveNameServers(null);
      NSListenerUDP.udpTransport.sendPacket(packet.toJSONObject(), packet.getLNSID(), PortType.LNS_UPDATE_PORT);
      if (StartNameServer.debugMode) {
        GNS.getLogger().fine("NAME RECORD DOES NOT Exist for. " + packet.getName() // + " " + packet.getRecordKey()
        );
      }
//            NameServer.isPrimaryNameServer(packet.getName(), packet.getRecordKey());
    }
  }
}
