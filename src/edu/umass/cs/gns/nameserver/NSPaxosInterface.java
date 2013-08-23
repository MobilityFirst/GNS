package edu.umass.cs.gns.nameserver;

import edu.umass.cs.gns.nameserver.replicacontroller.ComputeNewActivesTask;
import edu.umass.cs.gns.nameserver.replicacontroller.ListenerNameRecordStats;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaController;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaControllerRecord;
import edu.umass.cs.gns.packet.Packet;
import edu.umass.cs.gns.packet.paxospacket.FailureDetectionPacket;
import edu.umass.cs.gns.packet.paxospacket.RequestPacket;
import edu.umass.cs.gns.paxos.PaxosInterface;
import edu.umass.cs.gns.paxos.PaxosManager;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Created with IntelliJ IDEA.
 * User: abhigyan
 * Date: 6/29/13
 * Time: 8:34 PM
 * To change this template use File | Settings | File Templates.
 */
public class NSPaxosInterface implements PaxosInterface {

//    @Override
    public void proposeRequestToPaxos(String paxosID, RequestPacket requestPacket) {
        PaxosManager.propose(paxosID, requestPacket);
    }

    @Override
    public void handlePaxosDecision(String paxosID, RequestPacket req) {
        try {
            // messages decided for paxos between primaries

            if (req.clientID == Packet.PacketType.NEW_ACTIVE_PROPOSE.getInt()) {
                ComputeNewActivesTask.applyNewActivesProposed(req.value);
            }
            else if (req.clientID == Packet.PacketType.NAME_RECORD_STATS_RESPONSE.getInt()) {
                ListenerNameRecordStats.applyNameRecordStatsPacket(req.value);
            }
            else if (req.clientID == Packet.PacketType.NAMESERVER_SELECTION.getInt()) {
                ListenerNameRecordStats.applyNameServerSelectionPacket(req.value);
            }
            else if (req.clientID == Packet.PacketType.NEW_ACTIVE_START_CONFIRM_TO_PRIMARY.getInt()) {
                ReplicaController.newActiveStartedWriteToNameRecord(req.value);
            }
            else if (req.clientID == Packet.PacketType.OLD_ACTIVE_STOP_CONFIRM_TO_PRIMARY.getInt()) {
                ReplicaController.oldActiveStoppedWriteToNameRecord(req.value);
            }
            else if (req.clientID  == Packet.PacketType.REMOVE_RECORD_LNS.getInt()) {
                ReplicaController.applyMarkedForRemoval(req.value);
            }
            else if (req.clientID == Packet.PacketType.PRIMARY_PAXOS_STOP.getInt()) {
                ReplicaController.applyStopPrimaryPaxos(req.value);
            }

            // messages decided in to paxos between actives
            else if (req.clientID == Packet.PacketType.ACTIVE_PAXOS_STOP.getInt()) {
                // current paxos instance stopped
                ListenerReplicationPaxos.handleIncomingPacket(new JSONObject(req.value));
            }
            else if (req.clientID == Packet.PacketType.UPDATE_ADDRESS_NS.getInt()) {
                // address update is applied
                ClientRequestWorker.handleIncomingPacket(new JSONObject(req.value), Packet.PacketType.UPDATE_ADDRESS_NS);
            }
        } catch (JSONException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }

    @Override
    public void handleFailureMessage(FailureDetectionPacket fdPacket) {

        ReplicaController.handleNodeFailure(fdPacket);
    }

  @Override
  public String getState(String paxosID) {

    if (ReplicaController.isPrimaryPaxosID(paxosID)) {
      String name = ReplicaController.getNameFromPrimaryPaxosID(paxosID);
      // read all fields of the record
      ReplicaControllerRecord record = NameServer.getNameRecordPrimary(name);
      return  (record == null) ? null: record.toString();

    }
    else {
      String name = ReplicaController.getNameFromActivePaxosID(paxosID);
      // read all fields of the record
      NameRecord record = NameServer.getNameRecord(name);
      return  (record == null) ? null: record.toString();
    }
  }

  @Override
  public void updateState(String paxosID, String state) {
    try {
      JSONObject json = new JSONObject(state);
      if (ReplicaController.isPrimaryPaxosID(paxosID)) {
        NameServer.updateNameRecordPrimary(new ReplicaControllerRecord(json));
      } else {
        NameServer.updateNameRecord(new NameRecord(json));
      }

    } catch (JSONException e) {

      e.printStackTrace();
    }
  }
}
