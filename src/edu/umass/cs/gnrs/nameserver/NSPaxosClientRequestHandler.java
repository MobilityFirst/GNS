package edu.umass.cs.gnrs.nameserver;

import edu.umass.cs.gnrs.nameserver.replicacontroller.ComputeNewActivesTask;
import edu.umass.cs.gnrs.nameserver.replicacontroller.ListenerNameRecordStats;
import edu.umass.cs.gnrs.nameserver.replicacontroller.ReplicaController;
import edu.umass.cs.gnrs.packet.Packet;
import edu.umass.cs.gnrs.packet.paxospacket.FailureDetectionPacket;
import edu.umass.cs.gnrs.packet.paxospacket.RequestPacket;
import org.json.JSONException;
import org.json.JSONObject;
import paxos.PaxosClientRequestHandler;
import paxos.PaxosManager;

/**
 * Created with IntelliJ IDEA.
 * User: abhigyan
 * Date: 6/29/13
 * Time: 8:34 PM
 * To change this template use File | Settings | File Templates.
 */
public class NSPaxosClientRequestHandler extends PaxosClientRequestHandler {

    @Override
    public void handleRequestFromClient(String paxosID, RequestPacket requestPacket) {
        PaxosManager.propose(paxosID, requestPacket);
    }

    @Override
    public void forwardDecisionToClient(String paxosID, RequestPacket req) {
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
                ReplicaController.applyRemovedRecordPacket(req.value);
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
                ClientRequestWorker.handleIncomingPacket(new JSONObject(req.value));
            }
        } catch (JSONException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }

    @Override
    public void handleFailureMessage(FailureDetectionPacket fdPacket) {

        ReplicaController.handleNodeFailure(fdPacket);
    }
}
