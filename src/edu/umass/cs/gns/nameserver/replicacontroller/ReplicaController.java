package edu.umass.cs.gns.nameserver.replicacontroller;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.NSListenerUDP;
import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.nameserver.ValuesMap;
import edu.umass.cs.gns.packet.ChangeActiveStatusPacket;
import edu.umass.cs.gns.packet.ConfirmUpdateLNSPacket;
import edu.umass.cs.gns.packet.NewActiveSetStartupPacket;
import edu.umass.cs.gns.packet.OldActiveSetStopPacket;
import edu.umass.cs.gns.packet.Packet;
import edu.umass.cs.gns.packet.Packet.PacketType;
import edu.umass.cs.gns.packet.RemoveRecordPacket;
import edu.umass.cs.gns.packet.paxospacket.FailureDetectionPacket;
import edu.umass.cs.gns.packet.paxospacket.PaxosPacketType;
import edu.umass.cs.gns.packet.paxospacket.RequestPacket;
import org.json.JSONException;
import org.json.JSONObject;
import edu.umass.cs.gns.paxos.FailureDetection;
import edu.umass.cs.gns.paxos.PaxosManager;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ReplicaController
{

	public static int TIMEOUT_INTERVAL = 5000;
	
	private static ConcurrentHashMap<String, RemoveRecordPacket> removeRecordRequests = new ConcurrentHashMap<String, RemoveRecordPacket>();
	
	public static void handleNameRecordAddAtPrimary(ReplicaControllerRecord recordEntry, ValuesMap valuesMap)
	{
        HashSet<Integer> primaries = recordEntry.getPrimaryNameservers();
//        if (StartNameServer.debugMode) GNS.getLogger().fine(recordEntry.getName() +
//                "\tBefore Paxos instance created for name: " + recordEntry.getName()
//                        + " Primaries: " + primaries);
		PaxosManager.createPaxosInstance(getPrimaryPaxosID(recordEntry), primaries);
//		if (StartNameServer.debugMode) GNS.getLogger().fine(recordEntry.getName()  +
//				"\tPaxos instance created for name: " + recordEntry.getName()
//						+ " Primaries: " + primaries);
		startupNewActives(recordEntry, valuesMap);
        if (StartNameServer.debugMode) GNS.getLogger().fine(recordEntry.getName()  +
                "\tStartup new actives.: " + recordEntry.getName()
                        + " Primaries: " + primaries);
	}

	
	public static void handleNameRecordRemoveRequestAtPrimary(JSONObject json) throws JSONException {
		// 1. stop current actives
		// 2. stop current primaries
		// 3. send confirmation to client.
		RemoveRecordPacket removeRecord = new RemoveRecordPacket(json);
		ReplicaControllerRecord nameRecord = NameServer.replicaController.getNameRecordPrimary(removeRecord.getName());
                //NameServer.getNameRecord(removeRecord.getName()//, removeRecord.getRecordKey()

		if (nameRecord != null && nameRecord.isPrimaryReplica()) {
			if (nameRecord.isMarkedForRemoval() == true) {
				if (StartNameServer.debugMode) GNS.getLogger().fine("Already marked for removal. Name record will " +
						"be deleted soon. So request is dropped.");
				return;
			}
			// propose this to primary paxos
            String primaryPaxosID = getPrimaryPaxosID(nameRecord);
			PaxosManager.propose(primaryPaxosID,
					new RequestPacket(removeRecord.getType().getInt(), removeRecord.toString(), PaxosPacketType.REQUEST));
			if (StartNameServer.debugMode) GNS.getLogger().fine("PAXOS PROPOSAL: Proposed mark for removal in primary paxos. Packet = " + removeRecord);
			removeRecordRequests.put(getPrimaryPaxosID(nameRecord), removeRecord);
			
		}
		else {
			GNS.getLogger().severe(" REMOVE RECORD ERROR!! Name: " + removeRecord.getName()
                                // + " Record key: " + removeRecord.getRecordKey()
            + " NAME RECORD: " + nameRecord);
            if (nameRecord != null) {
                GNS.getLogger().severe(" REMOVE RECORD PRIMARY: " + nameRecord.getPrimaryNameservers());
            }
		}
	}

    /**
     *
     * @param name
     * @return
     */
    public static String getPrimaryPaxosID(String name) {
        return  name + "-Primary";
    }

    /**
     * Return ID of the paxos instance among primary name servers for this record.
     *
     * @param nameRecord
     */
    public static String getPrimaryPaxosID(ReplicaControllerRecord nameRecord)
    {
        return getPrimaryPaxosID(nameRecord.getName());
    }

	public static void applyRemovedRecordPacket(String value) throws JSONException {
		// create a remove record object
		if (StartNameServer.debugMode) GNS.getLogger().fine("PAXOS DECISION remove record packet accepted by paxos: " + value);
		RemoveRecordPacket removeRecord = new RemoveRecordPacket(new JSONObject(value));
		// 
		ReplicaControllerRecord nameRecord = NameServer.replicaController.getNameRecordPrimary(removeRecord.getName());

		if (nameRecord != null &&  nameRecord.isPrimaryReplica()) {
			if (nameRecord.isMarkedForRemoval() == true) {
				if (StartNameServer.debugMode) GNS.getLogger().fine("PAXOS DECISION  not applied. Already marked for removal. Name record will " +
						"be deleted soon. So request is dropped.");
				return;
			}
			nameRecord.setMarkedForRemoval();
            NameServer.replicaController.updateNameRecordPrimary(nameRecord);
			stopRunningActiveToRemoveNameRecord(nameRecord);

		}
		else {
			if (StartNameServer.debugMode) GNS.getLogger().fine("ERROR: remove record message reached non-primary name server or record not found.");
		}
	}
	
	static void stopRunningActiveToRemoveNameRecord(ReplicaControllerRecord nameRecord) {

		if (StartNameServer.debugMode) GNS.getLogger().fine("PAXOS DECISON applied. Name Record marked for removal.");
		ReplicaControllerRecord.ACTIVE_STATE stage = nameRecord.getNewActiveTransitionStage();
		if (StartNameServer.debugMode) GNS.getLogger().fine("ACTIVE Transition currently in stage = " + stage);
		switch (stage) {
		case ACTIVE_RUNNING:
			createTaskToStopCurrentActives(nameRecord);
			
			break;
		default:
			break;
		}

//		else if (stage == 2) { // old active stopped, new active yet to run
//			// when new active runs, send message to stop it.
//			
//		}
//		else if (stage == 1) { // old active not yet stopped
//			// wait for old active to stop. that process must be in progress.
//		}
			
	}
	
	public static void startupNewActives(ReplicaControllerRecord nameRecord, ValuesMap initialValue)
	{
		// this method will schedule a timer task to startup active replicas.
		StartupActiveSetTask startupTask = new StartupActiveSetTask(
				nameRecord.getName(),
				nameRecord.getOldActiveNameservers(),
				nameRecord.copyActiveNameServers(),
				nameRecord.getActivePaxosID(), nameRecord.getOldActivePaxosID(), initialValue);
		// scheduled
		NameServer.timer.schedule(startupTask, 0, TIMEOUT_INTERVAL);
	}
	

	/**
	 * Create a task to stop old actives from this name record.
	 * @param nameRecord
	 */
	public static void stopOldActives(ReplicaControllerRecord nameRecord)
	{
		// this method will schedule a timer task to startup active replicas.
		StopActiveSetTask task = new StopActiveSetTask(nameRecord.getName(), 
                                // nameRecord.getRecordKey(), 
				nameRecord.getOldActiveNameservers(), nameRecord.copyActiveNameServers(), 
				nameRecord.getOldActivePaxosID());
		NameServer.timer.schedule(task, 0, TIMEOUT_INTERVAL);
	}

	/**
	 * Return ID of the paxos instance among active name servers of this record.
	 * 
	 * @param nameRecord
	 */
	public static String getActivePaxosID(ReplicaControllerRecord nameRecord)
	{
		Random r = new Random();
		return nameRecord.getName() // + "-" + nameRecord.getRecordKey().getName()
				+ "-Active-" + r.nextInt();
	}



	public static void handleIncomingPacket(JSONObject json)
	{

		try
		{
			switch (Packet.getPacketType(json))
			{
			case NEW_ACTIVE_START_CONFIRM_TO_PRIMARY:
				NewActiveSetStartupPacket packet = new NewActiveSetStartupPacket(
						json);
				newActiveStartupReceivedConfirmationFromActive(packet);
				break;
			case OLD_ACTIVE_STOP_CONFIRM_TO_PRIMARY:
				OldActiveSetStopPacket oldPacket = new OldActiveSetStopPacket(
						json);
				oldActiveStoppedReceivedConfirmationFromActive(oldPacket);
				break;
			default:
				break;
			}
		} catch (JSONException e)
		{
			if (StartNameServer.debugMode) GNS.getLogger().fine("JSON Exception here.");
			e.printStackTrace();
		} catch (Exception e)
        {
            if (StartNameServer.debugMode) GNS.getLogger().fine("Exception in Replication controller .... ." + e.getMessage());
            e.printStackTrace();
        }
	}

	public static void newActiveStartupReceivedConfirmationFromActive(
			NewActiveSetStartupPacket packet)
	{
		if (StartNameServer.debugMode) GNS.getLogger().fine(
				"NEW_ACTIVE_START: Received confirmation at primary. "
						+ packet.getName());
		ReplicaControllerRecord nameRecord = NameServer.replicaController.getNameRecordPrimary(packet.getName());
//                NameServer.getNameRecord(packet.getName()//,packet.getRecordKey()
//                        );
		if (nameRecord != null) {

        }
		String paxosID = getPrimaryPaxosID(nameRecord);

		ChangeActiveStatusPacket proposePacket = new ChangeActiveStatusPacket(
				packet.getNewActivePaxosID(), nameRecord.getName(),
				//nameRecord.getRecordKey(),
				PacketType.NEW_ACTIVE_START_CONFIRM_TO_PRIMARY);

		PaxosManager.propose(paxosID, new RequestPacket(
				PacketType.NEW_ACTIVE_START_CONFIRM_TO_PRIMARY.getInt(),
				proposePacket.toString(), PaxosPacketType.REQUEST));
		if (StartNameServer.debugMode) GNS.getLogger().fine(
				"PAXOS PROPOSAL: New Active Started for Name: "
						+ nameRecord.getName() + " Paxos ID = "
						+ nameRecord.getActivePaxosID() + " New Actives: "
						+ packet.getNewActiveNameServers());

		//
		// write to name record object using Primary-paxos that newActive is
		// running
		// no more events.

	}

	public static void oldActiveStoppedReceivedConfirmationFromActive(
			OldActiveSetStopPacket packet)
	{
		// write to name record object using Primary-paxos that oldActive is
		// stopped
		// schedule new active startup event: StartupReplicaSetTask
		
		if (StartNameServer.debugMode) GNS.getLogger().fine(
				"OLD ACTIVE STOP: Received confirmation at primary. Name = "
						+ packet.getName());
		ReplicaControllerRecord nameRecord = NameServer.replicaController.getNameRecordPrimary(packet.getName());
//                NameServer.getNameRecord(packet.getName()//,packet.getRecordKey()
//                        );

		String paxosID = getPrimaryPaxosID(nameRecord);
		
		if (nameRecord.isMarkedForRemoval() == true) {
			PaxosManager.propose(paxosID, 
					new RequestPacket(PacketType.PRIMARY_PAXOS_STOP.getInt(), 
							packet.toString(), PaxosPacketType.REQUEST));
			if (StartNameServer.debugMode) GNS.getLogger().fine("PAXOS PROPOSAL PROPOSED STOP COMMAND because " +
					"name record is marked for removal: " + packet.toString());
			return;
		}
		
		ChangeActiveStatusPacket proposePacket = new ChangeActiveStatusPacket(
				packet.getPaxosIDToBeStopped(), nameRecord.getName(),
				//nameRecord.getRecordKey(),
				PacketType.OLD_ACTIVE_STOP_CONFIRM_TO_PRIMARY);

		PaxosManager.propose(paxosID, new RequestPacket(
				PacketType.OLD_ACTIVE_STOP_CONFIRM_TO_PRIMARY.getInt(),
				proposePacket.toString(), PaxosPacketType.REQUEST));
		if (StartNameServer.debugMode) GNS.getLogger().fine(
				"PAXOS PROPOSAL: Old Active Stopped for  Name: "
						+ nameRecord.getName() + " Old Paxos ID = "
						+ nameRecord.getOldActivePaxosID());
	}

	public static void newActiveStartedWriteToNameRecord(String decision)
			throws JSONException
	{
		if (StartNameServer.debugMode) GNS.getLogger().fine(
				"PAXOS DECISION: new active started. write to nameRecord: "
						+ decision);
		ChangeActiveStatusPacket packet = new ChangeActiveStatusPacket(
				new JSONObject(decision));
		ReplicaControllerRecord nameRecord = NameServer.replicaController.getNameRecordPrimary(packet.getName());
		
		if (nameRecord != null)
		{
			if (nameRecord.setNewActiveRunning(packet.getPaxosID()))
			{
				if (StartNameServer.debugMode) GNS.getLogger().fine("New Active paxos running for name : "
					+ nameRecord.getName() + " Paxos ID: "+ packet.getPaxosID());
			} else
			{
				if (StartNameServer.debugMode) GNS.getLogger()
						.fine("IGNORE MSG: NEW Active PAXOS ID NOT FOUND while setting "
								+ "it to inactive. Already received msg before. Paxos ID = "
								+ packet.getPaxosID());
			}
			
			if (nameRecord.isMarkedForRemoval() == true) {
				createTaskToStopCurrentActives(nameRecord);
			}
            NameServer.replicaController.updateNameRecordPrimary(nameRecord);
			
		} else
		{
			if (StartNameServer.debugMode) GNS.getLogger().severe(
					"ERROR: NAME RECORD NOT FOUND AT PRIMARY NAME SERVER !!!!");
		}
	}
	
	public static void oldActiveStoppedWriteToNameRecord(String decision)
			throws JSONException
	{
		ChangeActiveStatusPacket packet = new ChangeActiveStatusPacket(
				new JSONObject(decision));
		ReplicaControllerRecord nameRecord = NameServer.replicaController.getNameRecordPrimary(packet.getName());

		if (StartNameServer.debugMode) GNS.getLogger().fine(
				"PAXOS DECISION: old active stoppped. write to nameRecord: "
						+ decision);
		if (nameRecord != null)
		{
			if (nameRecord.setOldActiveStopped(packet.getPaxosID()))
			{
				if (StartNameServer.debugMode) GNS.getLogger().fine(
						"OLD Active paxos stopped. Name: "
								+ nameRecord.getName() + " Old Paxos ID: "
								+ packet.getPaxosID());
                NameServer.replicaController.updateNameRecordPrimary(nameRecord);
				startupNewActives(nameRecord, null);
			} else
			{
				if (StartNameServer.debugMode) GNS.getLogger().fine(
						"INGORE MSG: OLD PAXOS ID NOT FOUND IN NAME RECORD "
								+ "while setting it to inactive: "
								+ packet.getPaxosID());
			}
		} else
		{
			if (StartNameServer.debugMode) GNS.getLogger().severe(
					"ERROR: NAME RECORD NOT FOUND AT PRIMARY NAME SERVER !!!!");
		}
	}
	
	private static void createTaskToStopCurrentActives(ReplicaControllerRecord nameRecord) {
		// UPDATE NAME RECORD AFTER YOU CALL THIS //
		nameRecord.updateActiveNameServers(nameRecord.copyActiveNameServers(), getActivePaxosID(nameRecord));
        NameServer.replicaController.updateNameRecordPrimary(nameRecord);
//        NameServer.updateNameRecord(nameRecord);

		// create task to stop currently running active.
		StopActiveSetTask stopTask = new StopActiveSetTask(nameRecord.getName(), 
				//nameRecord.getRecordKey(), 
                        nameRecord.getOldActiveNameservers(), 
				nameRecord.getOldActiveNameservers(), nameRecord.getOldActivePaxosID());
		NameServer.timer.schedule(stopTask, 0, TIMEOUT_INTERVAL);
	}
	
	public static void applyStopPrimaryPaxos(String value) throws JSONException
	{
		if (StartNameServer.debugMode) GNS.getLogger().fine("PAXOS DECISION stop primary paxos decision received.");
		OldActiveSetStopPacket packet = new OldActiveSetStopPacket(new JSONObject(value));
        String paxosID = getPrimaryPaxosID(packet.getName()//, packet.getRecordKey()
                );
//		String name = packet.getName();
		RemoveRecordPacket removeRecordPacket = removeRecordRequests.remove(paxosID);

        NameServer.replicaController.removeNameRecord(packet.getName());
//        NameServer.removeNameRecord(packet.getName());

//		PaxosManager.deletePaxosInstance(paxosID);

		if (StartNameServer.debugMode) GNS.getLogger().fine("REMOVED RECORD FROM DB");
		if (removeRecordPacket != null) {
			ConfirmUpdateLNSPacket confirmPacket = new ConfirmUpdateLNSPacket(NameServer.nodeID, 
					true, removeRecordPacket);
			NSListenerUDP.udpTransport.sendPacket(confirmPacket.toJSONObject(), 
					confirmPacket.getLocalNameServerId(), GNS.PortType.LNS_UPDATE_PORT);
			if (StartNameServer.debugMode) GNS.getLogger().fine("REMOVE RECORD SENT RESPONSE TO LNS");
		}
	}
	
	
	private static void handlePrimaryFailureForNameRecord(ReplicaControllerRecord nameRecord, int failedNode) {
		// if this node is primary

		if (nameRecord.containsPrimaryNameserver(NameServer.nodeID) && nameRecord.containsPrimaryNameserver(failedNode)) {
			if (StartNameServer.debugMode) GNS.getLogger().fine("NODE FAILURE: This node is not a primary name server.: ");
		}
		

		ReplicaControllerRecord.ACTIVE_STATE stage = nameRecord.getNewActiveTransitionStage();
        if (StartNameServer.debugMode) {
            GNS.getLogger().fine("Handling node failure for name record: " + nameRecord.getName() +
                    " Failed Node: " + failedNode + " STAGE = " + stage);
        }
		switch (stage) {
		case ACTIVE_RUNNING:
			if (nameRecord.isMarkedForRemoval()) stopRunningActiveToRemoveNameRecord(nameRecord);
			break;
		case OLD_ACTIVE_RUNNING:
			// stop old actives, since we do not know whether old active is stopped or not.
			// if isMarkedForRemoval() == true: then
			// 			(1) we will make sure that old active has stopped
			//			(2) then remove name record
			stopOldActives(nameRecord);
			if (StartNameServer.debugMode) GNS.getLogger().fine(" Started the old actives task. upon failure of node");
			break;
		case NO_ACTIVE_RUNNING:
			// start to run new active replicas, since we do not know whether new active is running or not.
			// if isMarkedForRemoval() == true: then 
			//			(1) make sure new active has started 
			//			(2) next we will stop new active 
			//			(3) then remove name record 
			startupNewActives(nameRecord, null);
			break;
		default:
			break;
		}
	}
	
	public static void handleNodeFailure(FailureDetectionPacket fdPacket) {
        if (fdPacket.status == true) return;
        int failedNode = fdPacket.responderNodeID;
        GNS.getLogger().fine(" Failed Node Detected: replication controller working. " + failedNode);

//		if (node fails then what happens)
		Set<ReplicaControllerRecord> nameRecords = NameServer.replicaController.getAllPrimaryNameRecords();
		for (ReplicaControllerRecord record: nameRecords) {
			// if both this node & failed node are primaries.
			if (record.containsPrimaryNameserver(NameServer.nodeID) && 
					record.containsPrimaryNameserver(failedNode)) {
                GNS.getLogger().fine(" Handing Failure for Name: " + record.getName()
                        //+ " Key: " + record.getRecordKey() 
                        + " NAME RECORD: " + record);
                handlePrimaryFailureForNameRecord(record, failedNode);
			}
		}
	}

	public static boolean isSmallestPrimaryRunning(
			Set<Integer> primaryNameServer)
	{
		int smallestNSUp = -1;
		for (Integer primaryNS : primaryNameServer)
		{
			if (FailureDetection.isNodeUp(primaryNS))
			{
				if (smallestNSUp == -1 || primaryNS < smallestNSUp)
				{
					smallestNSUp = primaryNS;
				}
			}
		}
		if (smallestNSUp == NameServer.nodeID)
			return true;
		else
			return false;
	}


}
