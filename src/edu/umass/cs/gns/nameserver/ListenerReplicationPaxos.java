package edu.umass.cs.gns.nameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaController;
import edu.umass.cs.gns.packet.NewActiveSetStartupPacket;
import edu.umass.cs.gns.packet.OldActiveSetStopPacket;
import edu.umass.cs.gns.packet.Packet;
import edu.umass.cs.gns.packet.Packet.PacketType;
import edu.umass.cs.gns.packet.paxospacket.PaxosPacketType;
import edu.umass.cs.gns.packet.paxospacket.RequestPacket;
import edu.umass.cs.gns.util.Util;
import org.json.JSONException;
import org.json.JSONObject;
import edu.umass.cs.gns.paxos.PaxosManager;

import java.io.IOException;
import java.util.HashSet;
import java.util.TimerTask;
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
}

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
                    GNS.getLogger().fine("Received msg NEW_ACTIVE_START: " + json.toString());
                    NewActiveSetStartupPacket packet = new NewActiveSetStartupPacket(json);

                    // sanity check: am I in set? otherwise quit.
                    if (!packet.getNewActiveNameServers().contains(NameServer.nodeID)) {
                        GNS.getLogger().severe("ERROR: NewActiveSetStartupPacket reached "
                                + "a non-active name server." + packet.toString());
                        break;
                    }
                    // create name server
                    NewActiveStartInfo activeStartInfo = new NewActiveStartInfo(new NewActiveSetStartupPacket(json));
                    ListenerReplicationPaxos.activeStartupInProgress.put(packet.getID(), activeStartInfo);
                    // send to all nodes, except self
                    packet.changePacketTypeToForward();
                    GNS.getLogger().fine("NEW_ACTIVE_START: forwarded msg to nodes; "
                            + packet.getNewActiveNameServers());
                    NameServer.tcpTransport.sendToAll(packet.toJSONObject(), packet.getNewActiveNameServers(),
                            GNS.PortType.STATS_PORT, NameServer.nodeID);

                    if (packet.getPreviousValue() != null  && packet.getPreviousValue().isEmpty() == false) {
                        if(StartNameServer.debugMode) GNS.getLogger().fine(packet.getName() +
                                "\tUsing Value in NewActiveSetStartupPacket To Create Name Record." + packet.getPreviousValue());
                        addNameRecord(packet, packet.getPreviousValue());
                        break;
                    }

                    // start-up paxos instance at this node.

                    CopyStateFromOldActiveTask copyTask = new CopyStateFromOldActiveTask(packet);
                    NameServer.timer.schedule(copyTask, 0, ReplicaController.TIMEOUT_INTERVAL);
                    break;

                case NEW_ACTIVE_START_FORWARD:
                    packet = new NewActiveSetStartupPacket(json);
                    if (packet.getPreviousValue() != null  && packet.getPreviousValue().isEmpty() == false) {
                        if(StartNameServer.debugMode) GNS.getLogger().fine(packet.getName()
                                + "\tUsing Value in NewActiveSetStartupPacket To Create Name Record." + packet.getPreviousValue());
                        addNameRecord(packet, packet.getPreviousValue());
                    }
                    else {
                        copyTask = new CopyStateFromOldActiveTask(packet);
                        NameServer.timer.schedule(copyTask, 0, ReplicaController.TIMEOUT_INTERVAL);
                    }
                    break;

                case NEW_ACTIVE_START_PREV_VALUE_REQUEST:
                    packet = new NewActiveSetStartupPacket(json);
                    GNS.getLogger().fine(" Received NEW_ACTIVE_START_PREV_VALUE_REQUEST at node " + NameServer.nodeID);
                    // obtain name record
                    NameRecord nameRecord = NameServer.getNameRecord(packet.getName());
                    //NameRecord nameRecord = DBNameRecord.getNameRecord(packet.getName());
                    
                    // fill query reqult value
                    //QueryResultValue value = null;
                    ValuesMap value = null;
                    if (nameRecord != null) {
                        value = nameRecord.getOldValues(packet.getOldActivePaxosID());
                    }
                    if (value == null) {
                        packet.changePreviousValueCorrect(false);
                    } else {
                        // update previous value
                        packet.changePreviousValueCorrect(true);
                        packet.changePreviousValue(value);
                    }
                    // change packet type
                    packet.changePacketTypeToPreviousValueResponse();
                    GNS.getLogger().fine(" NEW_ACTIVE_START_PREV_VALUE_REQUEST reply sent to: " + packet.getSendingActive());
                    // reply to sending active
                    NameServer.tcpTransport.sendToID(packet.toJSONObject(), packet.getSendingActive(), GNS.PortType.STATS_PORT);
                    // send current value to
                    break;

                case NEW_ACTIVE_START_PREV_VALUE_RESPONSE:

                    packet = new NewActiveSetStartupPacket(json);
                    GNS.getLogger().fine(" Received NEW_ACTIVE_START_PREV_VALUE_RESPONSE at node " + NameServer.nodeID);
                    if (packet.getPreviousValueCorrect()) {
                        NewActiveSetStartupPacket originalPacket = ListenerReplicationPaxos.activeStartupPacketsReceived.remove(packet.getID());
                        if (originalPacket != null) {
                            addNameRecord(originalPacket, packet.getPreviousValue());
                        } else {
                            GNS.getLogger().fine(" NewActiveSetStartupPacket not found for response.");
                        }
                    } else {
                        GNS.getLogger().fine(" Old Active did not return previous value.");
                    }
                    break;
                case NEW_ACTIVE_START_RESPONSE:
                    packet = new NewActiveSetStartupPacket(json);
                    NewActiveStartInfo info = ListenerReplicationPaxos.activeStartupInProgress.get(packet.getID());
                    GNS.getLogger().fine("NEW_ACTIVE_START: received confirmation from "
                            + "node: " + packet.getSendingActive());
                    if (info != null) {
                        info.receivedResponseFromActive(packet.getSendingActive());
                        if (info.haveMajorityActivesResponded()) {
                            GNS.getLogger().fine("NEW_ACTIVE_START: received confirmation from majority. ");
                            info.packet.changePacketTypeToConfirmation();
                            NameServer.tcpTransport.sendToID(info.packet.toJSONObject(),
                                    info.packet.getSendingPrimary(), GNS.PortType.STATS_PORT);
                            ListenerReplicationPaxos.activeStartupInProgress.remove(packet.getID());
                        }
                    }
                    break;
                case OLD_ACTIVE_STOP:
                    OldActiveSetStopPacket oldActiveStopPacket = new OldActiveSetStopPacket(json);
                    GNS.getLogger().fine("Received Old Active Stop Packet: " + json);
                    String paxosID = oldActiveStopPacket.getPaxosIDToBeStopped();
                    // if this is current active:
                    NameRecord nameRecord1 = NameServer.getNameRecord(oldActiveStopPacket.getName());
                    //NameRecord nameRecord1 = DBNameRecord.getNameRecord(oldActiveStopPacket.getName());


//                if (nameRecord == null) {
//                    sendOldActiveStopConfirmationToPrimary(oldActiveStopPacket);
//                    return;
//                }

                    GNS.getLogger().fine("NAME RECORD NOW: " + nameRecord1);
                    int paxosStatus = nameRecord1.getPaxosStatus(paxosID);
                    GNS.getLogger().fine("PaxosIDtoBeStopped = " + paxosID + " PaxosStatus = " + paxosStatus);
                    if (paxosStatus == 1) { // this paxos ID is current active
                        // propose STOP command for this paxos instance
                        // Change Packet Type in oldActiveStop: This will help paxos identify that
                        // this is a stop packet. See: PaxosManager.isStopCommand()
                        oldActiveStopPacket.changePacketTypeToPaxosStop();
                        // Put client ID = PAXOS_STOP.getInt() so that PaxosManager can route decision
                        // to this class.
                        PaxosManager.propose(paxosID, new RequestPacket(PacketType.ACTIVE_PAXOS_STOP.getInt(),
                                oldActiveStopPacket.toString(), PaxosPacketType.REQUEST, true));
                        GNS.getLogger().fine("PAXOS PROPOSE: STOP Current Active Set. Paxos ID = " + paxosID);
                    } else if (paxosStatus == 2) { // this is the old paxos ID
                        // send confirmation to primary that this paxos ID is stopped.
                        sendOldActiveStopConfirmationToPrimary(oldActiveStopPacket);
                    } else {
                        GNS.getLogger().fine("PAXOS ID Neither current nor old. Ignore msg = " + paxosID);
                    }

                    break;
                case ACTIVE_PAXOS_STOP:
                    // STOP command is performed by paxos instance replica.
                    oldActiveStopPacket = new OldActiveSetStopPacket(json);
                    GNS.getLogger().fine("PAXOS DECISION: Old Active Stopped: " + oldActiveStopPacket);
                    paxosID = oldActiveStopPacket.getPaxosIDToBeStopped();
                    NameRecord nameRecord2 = NameServer.getNameRecord(oldActiveStopPacket.getName());
                    //NameRecord nameRecord2 = DBNameRecord.getNameRecord(oldActiveStopPacket.getName());
                    if (nameRecord2 !=null) {
                        nameRecord2.handleCurrentActiveStop(paxosID);
                        sendOldActiveStopConfirmationToPrimary(oldActiveStopPacket);
                        //          PaxosManager.deletePaxosInstance(paxosID);
                        NameServer.updateNameRecord(nameRecord2);
                        //DBNameRecord.updateNameRecord(nameRecord2);
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

    /**
     *
     * @param oldActiveStopPacket
     * @throws JSONException
     * @throws IOException
     */
    private void sendOldActiveStopConfirmationToPrimary(OldActiveSetStopPacket oldActiveStopPacket)
            throws IOException, JSONException {

        // confirm to primary name server that this set of actives has stopped
        if (oldActiveStopPacket.getActiveReceiver() == NameServer.nodeID) {
            // the active node who received this node, sends confirmation to primary
            // confirm to primary
            oldActiveStopPacket.changePacketTypeToConfirm();
            NameServer.tcpTransport.sendToID(oldActiveStopPacket.toJSONObject(),
                    oldActiveStopPacket.getPrimarySender(),
                    GNS.PortType.STATS_PORT);
            GNS.getLogger().fine("OLD ACTIVE STOP: Name Record Updated. Sent confirmation to primary."
                    + " OldPaxosID = " + oldActiveStopPacket.getPaxosIDToBeStopped());
        } else {
            // other nodes do nothing.
            GNS.getLogger().fine("OLD ACTIVE STOP: Name Record Updated. OldPaxosID = "
                    + oldActiveStopPacket.getPaxosIDToBeStopped());
        }
    }

    private  void addNameRecord(NewActiveSetStartupPacket originalPacket, ValuesMap previousValue)
            throws  JSONException, IOException{
        try {


       NameRecord nameRecord = NameServer.getNameRecord(originalPacket.getName());
       //NameRecord nameRecord = DBNameRecord.getNameRecord(originalPacket.getName());

        if (nameRecord == null) {
            nameRecord = new NameRecord(originalPacket.getName());

            nameRecord.handleNewActiveStart(originalPacket.getNewActiveNameServers(),
                    originalPacket.getNewActivePaxosID(), previousValue);
            // first add name record, then create paxos instance for it.

            NameServer.addNameRecord(nameRecord);

            //DBNameRecord.addNameRecord(nameRecord);
            GNS.getLogger().fine(" NAME RECORD ADDED AT ACTIVE NODE: "
                    + "name record = " + nameRecord);
        } else {
            nameRecord.handleNewActiveStart(originalPacket.getNewActiveNameServers(),
                    originalPacket.getNewActivePaxosID(), previousValue);
            NameServer.updateNameRecord(nameRecord);
            //DBNameRecord.updateNameRecord(nameRecord);
            GNS.getLogger().fine(" NAME RECORD UPDATED AT ACTIVE  NODE. "
                    + "name record = " + nameRecord);
        }
        // put the previous value obtained in the name record.
        GNS.getLogger().fine(" NEW_ACTIVE_START_PREV_VALUE_RESPONSE. "
                + "Name Record Value: " + nameRecord);
        // fire up paxos instance
        boolean created = PaxosManager.createPaxosInstance(originalPacket.getNewActivePaxosID(),
                originalPacket.getNewActiveNameServers(), nameRecord.toString());
        if (created) {
            GNS.getLogger().fine(" PAXOS INSTANCE CREATED AT ACTIVE NAME SERVER. " + nameRecord.getName());
        } else {
            GNS.getLogger().fine(" PAXOS INSTANCE NOT CREATED. "  + nameRecord.getName());
        }
        // send reply to main active
        originalPacket.changePacketTypeToResponse();
        int sendingActive = originalPacket.getSendingActive();
        originalPacket.changeSendingActive(NameServer.nodeID);
        GNS.getLogger().fine("NEW_ACTIVE_START: replied to active sending the startup packet from node: " + sendingActive);
        NameServer.tcpTransport.sendToID(originalPacket.toJSONObject(), sendingActive,
                GNS.PortType.STATS_PORT);
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
//			NameServer.tcpTransport.sendToID(packet2.toJSONObject(), oldActive, GNRS.PortType.STATS_PORT);
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
        if (sent == false && packet.getNewActiveNameServers().size() < activesResponded.size() * 2) {
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
            GNS.getLogger().fine(" NEW_ACTIVE_START_FORWARD received packet: " + packet.toJSONObject());
            if (oldActivesQueried.size() == 0) {
                ListenerReplicationPaxos.activeStartupPacketsReceived.put(packet.getID(), packet);
            }

            if (!ListenerReplicationPaxos.activeStartupPacketsReceived.containsKey(packet.getID())) {
                GNS.getLogger().fine(" COPY State from Old Active Successful! Cancel Task; Actives Queried: " + oldActivesQueried);
                this.cancel();
                return;
            }

            // make a copy
            NewActiveSetStartupPacket packet2 = new NewActiveSetStartupPacket(packet.toJSONObject());
            // select old active to send request to
            int oldActive = Util.getSmallestLatencyNS(packet.getOldActiveNameServers(), oldActivesQueried);

            if (oldActive == -1) {
                GNS.getLogger().fine(" ERROR:  No More Actives Left To Query. Cancel Task!!!");
                this.cancel();
                return;
            }
            oldActivesQueried.add(oldActive);
            GNS.getLogger().fine(" OLD ACTIVE SELECTED = : " + oldActive);
            // change packet type
            packet2.changePacketTypeToPreviousValueRequest();
            // change sending active
            packet2.changeSendingActive(NameServer.nodeID);

            try {
                NameServer.tcpTransport.sendToID(packet2.toJSONObject(), oldActive, GNS.PortType.STATS_PORT);
            } catch (IOException e) {
                GNS.getLogger().fine(" IOException here: " + e.getMessage());
                e.printStackTrace();
            } catch (JSONException e) {
                GNS.getLogger().fine(" JSONException here: " + e.getMessage());
                e.printStackTrace();
            }
            GNS.getLogger().fine(" REQUESTED VALUE from OLD ACTIVE. PACKET: " + packet2);

        } catch (JSONException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}