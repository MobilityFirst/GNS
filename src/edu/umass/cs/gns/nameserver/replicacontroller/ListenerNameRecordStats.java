package edu.umass.cs.gns.nameserver.replicacontroller;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.NSListenerUDP;
import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.packet.NameRecordStatsPacket;
import edu.umass.cs.gns.packet.NameServerSelectionPacket;
import edu.umass.cs.gns.packet.Packet;
import edu.umass.cs.gns.packet.Packet.PacketType;
import edu.umass.cs.gns.packet.paxospacket.PaxosPacketType;
import edu.umass.cs.gns.packet.paxospacket.RequestPacket;
import edu.umass.cs.gns.paxos.PaxosManager;
import edu.umass.cs.gns.statusdisplay.StatusClient;
import org.json.JSONException;
import org.json.JSONObject;

public class ListenerNameRecordStats extends Thread {


    public static void handleIncomingPacket(JSONObject json) throws JSONException {
        switch (Packet.getPacketType(json)) {
            case NAME_RECORD_STATS_RESPONSE:
                handleNameRecordStatsPacket(json);
                break;
            case NAMESERVER_SELECTION:
                handleNameserverSelectionPacket(json);
                break;
            default:
                break;
        }
    }

    public static void handleNameserverSelectionPacket(JSONObject incomingJSON) throws JSONException {
        String msg = "NS: received  NAMESERVER_SELECTION " + incomingJSON.toString();
        if (StartNameServer.debugMode) GNS.getLogger().fine(msg);
//        GNS.getStatLogger().fine(msg);
//		if (StartNameSer)
        NameServerSelectionPacket selectionPacket = new NameServerSelectionPacket(incomingJSON);
        // Send ACK to local name server immediately, that vote is received.
        NSListenerUDP.udpTransport.sendPacket(incomingJSON, selectionPacket.getLocalnameserverID(),
                GNS.PortType.LNS_UPDATE_PORT);

        //Add vote for the name record if your the primary name server for this name record
        //ReplicaControllerRecord nameRecordPrimary = NameServer.getNameRecordPrimaryLazy(selectionPacket.getName());
        ReplicaControllerRecord nameRecordPrimary = NameServer.getNameRecordPrimary(selectionPacket.getName());
        if (nameRecordPrimary != null && nameRecordPrimary.isMarkedForRemoval() == false && nameRecordPrimary.isPrimaryReplica()) {
            RequestPacket request = new RequestPacket(PacketType.NAMESERVER_SELECTION.getInt(),
                    selectionPacket.toString(),
                    PaxosPacketType.REQUEST, false);
            PaxosManager.propose(ReplicaController.getPrimaryPaxosID(nameRecordPrimary), request);
            if (StartNameServer.debugMode) GNS.getLogger().fine("PAXOS PROPOSAL: NameSever Vote: " + incomingJSON.toString());
        }
    }


    private static void handleNameRecordStatsPacket(JSONObject json) {
        if (StartNameServer.debugMode) GNS.getLogger().fine("ListenerNameRecordStats: received " + json.toString());
        NameRecordStatsPacket statsPacket;
        try {
            statsPacket = new NameRecordStatsPacket(json);
        } catch (JSONException e) {
            e.printStackTrace();
            return;
        }
       
         //ReplicaControllerRecord nameRecordPrimary = NameServer.getNameRecordPrimaryLazy(statsPacket.getName());
        ReplicaControllerRecord nameRecordPrimary = NameServer.getNameRecordPrimary(statsPacket.getName());
        if (nameRecordPrimary != null && nameRecordPrimary.isMarkedForRemoval() == false && nameRecordPrimary.isPrimaryReplica()) {
            // Propose to paxos.
            String paxosID = ReplicaController.getPrimaryPaxosID(nameRecordPrimary);
            RequestPacket requestPacket = new RequestPacket(PacketType.NAME_RECORD_STATS_RESPONSE.getInt(),
                    statsPacket.toString(), PaxosPacketType.REQUEST, false);
            PaxosManager.propose(paxosID, requestPacket);
            if (StartNameServer.debugMode) GNS.getLogger().fine("PAXOS PROPOSAL: Stats Packet proposed. ");
        }
    }

    /**
     * Apply the decision from Paxos: Packet = NameRecordStatsPacket.
     */
    public static void applyNameRecordStatsPacket(String decision) {

        NameRecordStatsPacket statsPacket;
        try {
            statsPacket = new NameRecordStatsPacket(new JSONObject(decision));
        } catch (JSONException e) {
            e.printStackTrace();
            return;
        }
        if (StartNameServer.debugMode) GNS.getLogger().fine("PAXOS DECISION: StatsPacket for name " + statsPacket.getName()
                + " Decision: " + decision);
        ReplicaControllerRecord nameRecordPrimary = NameServer.getNameRecordPrimary(statsPacket.getName());
        if (nameRecordPrimary != null && nameRecordPrimary.isMarkedForRemoval() == false && nameRecordPrimary.isPrimaryReplica())
        {
            // Record access frequency from the name server
            nameRecordPrimary.addNameServerStats(statsPacket.getActiveNameServerId(),
                    statsPacket.getReadFrequency(), statsPacket.getWriteFrequency());
            NameServer.updateNameRecordPrimary(nameRecordPrimary);
            StatusClient.sendStatus(NameServer.nodeID, "Updating stats: " + statsPacket.getName()
                    // + " / " + statsPacket.getRecordKey().getName()
                    + ", r = " + statsPacket.getReadFrequency() + ", w = " + statsPacket.getWriteFrequency());
        }
        else  {
            if (StartNameServer.debugMode) GNS.getLogger().severe(" Name Record Does Not Exist At Active for Packet " + statsPacket);
        }
    }

    public static void applyNameServerSelectionPacket(String decision) {

        try {
            NameServerSelectionPacket selectionPacket = new NameServerSelectionPacket(new JSONObject(decision));
            if (StartNameServer.debugMode) GNS.getLogger().fine("PAXOS DECISION: Name Sever Vote: " + selectionPacket.toString());
            ReplicaControllerRecord nameRecordPrimary = NameServer.getNameRecordPrimary(selectionPacket.getName());

            if (nameRecordPrimary!=null && nameRecordPrimary.isMarkedForRemoval() == false && nameRecordPrimary.isPrimaryReplica()) {
                nameRecordPrimary.addReplicaSelectionVote(selectionPacket.getNameserverID(), selectionPacket.getVote());
                NameServer.updateNameRecordPrimary(nameRecordPrimary);
            }
            else {
                if (StartNameServer.debugMode)GNS.getLogger().severe(" Name Record Does Not Exist At Active for Packet " + selectionPacket);
            }

        } catch (JSONException e) {
            e.printStackTrace();
        }

    }
}
//=======
//	/*************************************************************
//	 * Starts executing this thread.
//	 ************************************************************/
//	@Override
//	public void run() {
//		GNRS.getLogger().info("NS Node " + NameServer.nodeID + " starting Name Record Stats Server on port " 
//				+ ConfigFileInfo.getStatsPort(NameServer.nodeID));
//		int numberMessages = 0;
//		while (true) {
//			try {
//				JSONObject json = transport.readPacket();
//				numberMessages++;
//        if (numberMessages%1000 == 0) {
//        	System.out.println("ListenerNameRecordStats\t" + NameServer.nodeID + "\t" + numberMessages);
//        }
//        
//				GNRS.getLogger().fine("ListenerNameRecordStats: received " + json.toString());
//				GNRS.getStatLogger().fine("ListenerNameRecordStats: received " + json.toString());
//				NameRecordStatsPacket statsPacket = new NameRecordStatsPacket(json);
//
//				if (NameServer.isPrimaryNameServer(statsPacket.getName(), statsPacket.getRecordKey())) {
//					//Record access frequency from the name server
//					NameRecord nameRecord = NameServer.getNameRecord(statsPacket.getName(), statsPacket.getRecordKey());
//					if (nameRecord != null) {
//						nameRecord.addNameServerStats(statsPacket.getActiveNameServerId(), statsPacket.getReadFrequency(), statsPacket.getWriteFrequency());
//						NameServer.updateNameRecord(nameRecord);
//						StatusClient.sendStatus(NameServer.nodeID, "Updating stats: " + statsPacket.getName() + " / " + statsPacket.getRecordKey().name()
//								+ ", r = " +  statsPacket.getReadFrequency() + ", w = " + statsPacket.getWriteFrequency());
//					}
//				}
//				//				socket.close();
//			} catch (Exception e) {
//				e.printStackTrace();
//>>>>>>> .r485
//
//				if (NameServer.isPrimaryNameServer(statsPacket.getName(), statsPacket.getRecordKey())) {
//					//Record access frequency from the name server
//					NameRecord nameRecord = NameServer.getNameRecord(statsPacket.getName(), statsPacket.getRecordKey());
//					if (nameRecord != null) {
//						nameRecord.addNameServerStats(statsPacket.getActiveNameServerId(), statsPacket.getReadFrequency(), statsPacket.getWriteFrequency());
//						NameServer.updateNameRecord(nameRecord);
//						StatusClient.sendStatus(NameServer.nodeID, "Updating stats: " + statsPacket.getName() + " / " + statsPacket.getRecordKey().getName()
//								+ ", r = " +  statsPacket.getReadFrequency() + ", w = " + statsPacket.getWriteFrequency());
//					}
//				}
//				//				socket.close();
//			} catch (Exception e) {
//				e.printStackTrace();
//>>>>>>> .r547
//	/** Socket over which name record stats packet arrive **/
//	public static Transport transport;
//	/*************************************************************
//	 * Constructs a new ListenerNameRecordStats that handles all
//	 * packets related to name record stats.
//	 * @throws IOException
//	 ************************************************************/
//	public ListenerNameRecordStats() throws IOException {
//		super("ListenerNameRecordStats");
//		transport = new Transport(NameServer.nodeID, 
//				ConfigFileInfo.getStatsPort(NameServer.nodeID), NameServer.timer);
//	}
//
//	/*************************************************************
//	 * Starts executing this thread.
//	 ************************************************************/
//	@Override
//	public void run() {
//		GNRS.getLogger().info("NS Node " + NameServer.nodeID + " starting Name Record Stats Server on port " 
//				+ ConfigFileInfo.getStatsPort(NameServer.nodeID));
//		int numberMessages = 0;
//		while (true) {
//			try {
//				JSONObject json = transport.readPacket();
//				numberMessages++;
//        if (numberMessages%1000 == 0) {
//        	System.out.println("ListenerNameRecordStats\t" + NameServer.nodeID + "\t" + numberMessages);
//        }
//        
//				GNRS.getLogger().fine("ListenerNameRecordStats: received " + json.toString());
//				GNRS.getStatLogger().fine("ListenerNameRecordStats: received " + json.toString());
//				NameRecordStatsPacket statsPacket = new NameRecordStatsPacket(json);
//
//				if (NameServer.isPrimaryNameServer(statsPacket.name, statsPacket.recordKey)) {
//					//Record access frequency from the name server
//					NameRecord nameRecord = NameServer.getNameRecord(statsPacket.name, statsPacket.recordKey);
//					if (nameRecord != null) {
//						nameRecord.addNameServerStats(statsPacket.activeNameServerId, statsPacket.readFrequency, statsPacket.writeFrequency);
//						NameServer.updateNameRecord(nameRecord);
//						StatusClient.sendStatus(NameServer.nodeID, "Updating stats: " + statsPacket.name + " / " + statsPacket.recordKey.name()
//								+ ", r = " +  statsPacket.readFrequency + ", w = " + statsPacket.writeFrequency);
//					}
//				}
//				//				socket.close();
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
//	}

