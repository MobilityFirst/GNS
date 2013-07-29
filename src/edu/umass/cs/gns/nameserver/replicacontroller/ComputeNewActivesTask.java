package edu.umass.cs.gns.nameserver.replicacontroller;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.packet.NewActiveProposalPacket;
import edu.umass.cs.gns.packet.Packet.PacketType;
import edu.umass.cs.gns.packet.paxospacket.PaxosPacketType;
import edu.umass.cs.gns.packet.paxospacket.RequestPacket;
import edu.umass.cs.gns.replicationframework.BeehiveReplication;
import edu.umass.cs.gns.util.ConfigFileInfo;
import org.json.JSONException;
import org.json.JSONObject;
import paxos.PaxosManager;

import java.util.Set;
import java.util.TimerTask;

/**
 * Computes new actives for all name records for which this node is primary name server.
 * @author abhigyan
 *
 */
public class ComputeNewActivesTask extends TimerTask
{
	
	static int replicationRound = 0;
	
	@Override
	public void run()
	{
		replicationRound++;

		if (StartNameServer.debugMode) GNS.getLogger().fine("ComputeNewActives: " + replicationRound);
		
		//Iterate through the NameRecord and check if any changes need to
		//be made to the active name server set
		Set<ReplicaControllerRecord> nameRecords = DBReplicaController.getAllPrimaryNameRecords();
		if (StartNameServer.debugMode) GNS.getLogger().fine("\tComputeNewActives\tNumberOfNameRecords\t" + nameRecords.size());

		int count = 0;
		for (ReplicaControllerRecord nameRecord : nameRecords) {
			if (nameRecord.isMarkedForRemoval() == true) {
				continue;
			}
			count++;
			if (StartNameServer.debugMode) GNS.getLogger().fine("\tComputeNewActives\t" + nameRecord.getName() + "\tCount\t" + count + "\tRound\t" + replicationRound);
			
			if (!nameRecord.getPrimaryNameservers().contains(NameServer.nodeID) || 
					!ReplicaController.isSmallestPrimaryRunning(nameRecord.getPrimaryNameservers()))
					continue;
			
			if (StartNameServer.debugMode) GNS.getLogger().fine("I am the smallest primary NS up, I will select new actives.");
			
			Set<Integer> oldActiveNameServers = nameRecord.copyActiveNameServers();
			Set<Integer> newActiveNameServers = getNewActiveNameServers(nameRecord, 
					nameRecord.copyActiveNameServers(), replicationRound);

			// TODO: uncomment this line
//			if (!isActiveSetUnchanged(oldActiveNameServers, newActiveNameServers)) {
				if (StartNameServer.debugMode) GNS.getLogger().fine("\tComputeNewActives\t" + nameRecord.getName() + "\tCount\t" + count + "\tRound\t" + replicationRound + "\tUpadingOtherActives");
				
				String newActivePaxosID = ReplicaController.getActivePaxosID(nameRecord);
				NewActiveProposalPacket activePropose = new NewActiveProposalPacket(nameRecord.getName(), 
					//nameRecord.getRecordKey(), 
                                        NameServer.nodeID, newActiveNameServers, newActivePaxosID);
				String paxosID = ReplicaController.getPrimaryPaxosID(nameRecord);
				RequestPacket requestPacket = new RequestPacket(PacketType.NEW_ACTIVE_PROPOSE.getInt(), activePropose.toString(), 
						PaxosPacketType.REQUEST);
				PaxosManager.propose(paxosID, requestPacket);
				if (StartNameServer.debugMode) GNS.getLogger().fine("PAXOS PROPOSAL: Proposal done.");
//			}
//			else {
//				if (StartNameServer.debugMode) GNRS.getLogger().fine("Old and new active name servers are same. No Operation.");
//			}
		}
		
	}



	
	/**
	 * Calculates new set of active name servers depending on replication framework. 
	 * @param nameRecord
	 */
	private Set<Integer> getNewActiveNameServers(ReplicaControllerRecord nameRecord,
			Set<Integer> oldActiveNameServers, int count) {
		
		Set<Integer> newActiveNameServers = null;
		
		int numReplica = numberOfReplica(nameRecord);
		
		// used for beehive. 
		if (StartNameServer.beehiveReplication) {
			numReplica = BeehiveReplication.numActiveNameServers(nameRecord.getName()) - 3;
		}
		
		//Get a new set of active name servers for this record
		newActiveNameServers = NameServer.replicationFramework.newActiveReplica(nameRecord, numReplica, count);
		
		if (StartNameServer.debugMode) GNS.getLogger().fine("ComputeNewActives: Round:" + count + " Name:" + nameRecord.getName()
				+ " OldActive:" + oldActiveNameServers.toString() + " NumberReplica:" + numReplica
				+ " NewReplica:" + newActiveNameServers.toString());
		
		GNS.getStatLogger().info("ComputeNewActives: Round:" + count + " Name:" + nameRecord.getName()
				+ " OldActive:" + oldActiveNameServers.toString() + " NumberReplica:" + numReplica
				+ " NewReplica:" + newActiveNameServers.toString());
		//		}
		return newActiveNameServers;
	}






	/**
	 * ***********************************************************
	 * Returns the size of active replicas set that should exist for this name record. 
	 * The size of the active replica set
	 * depends on the lookup and update rate of this name record.
	 *
	 * @param nameRecord Name record
	 ***********************************************************
	 */
	private static int numberOfReplica(ReplicaControllerRecord nameRecord) {
		double update = 0;
		double lookup = 0;
		
		update = nameRecord.getWriteStats_Paxos();
		lookup = nameRecord.getReadStats_Paxos();
		
		DBReplicaController.updateNameRecordPrimary(nameRecord);
		
		int replicaCount = 0;
		if (update == 0 && lookup == 0) {
			// no requests seen, replicate at minimum number of locations.
			replicaCount = StartNameServer.minReplica;
		}
		else if (update == 0) {
			// no updates, replicate everywhere.
			replicaCount = ConfigFileInfo.getNumberOfNameServers();
		}
		else {
			replicaCount = StrictMath.round(StrictMath.round(
					(lookup / (update * StartNameServer.normalizingConstant) + StartNameServer.minReplica)));
			
			if (replicaCount > ConfigFileInfo.getNumberOfNameServers()) {
				replicaCount =  ConfigFileInfo.getNumberOfNameServers();
			}
		}
			
		// put in here for DNS experiments.
		if (replicaCount > StartNameServer.maxReplica) replicaCount = StartNameServer.maxReplica;
		
		GNS.getStatLogger().info("\tComputeNewActives-ReplicaCount\tName\t"
		+ nameRecord.getName() +"\tLookup\t" + lookup + "\tUpdate\t" + update + 
		"\tReplicaCount\t" + replicaCount);
		
		return replicaCount;
	}
	
	/**
	 * Apply the decision from paxos. Packet = NewActiveProposalPacket.
	 * @param decision
	 */
	public static void applyNewActivesProposed(String decision) {
		
		try {
			NewActiveProposalPacket activeProposalPacket = new NewActiveProposalPacket(new JSONObject(decision));
			ReplicaControllerRecord nameRecord = DBReplicaController.getNameRecordPrimary(activeProposalPacket.getName());
            if (nameRecord == null) {
                if (StartNameServer.debugMode) GNS.getLogger().severe("ERROR: PAXOS DECISION: " +
                        "BUT PRIMARY NAME RECORD DELETED Name = " + activeProposalPacket.getName());
                return;
            }
			
			if (StartNameServer.debugMode) GNS.getLogger().fine("PAXOS DECISION: Name = " + activeProposalPacket.getName()
					+ " Actives: " + activeProposalPacket.getProposedActiveNameServers() 
					+ " DECISION: "+ decision );
			if (nameRecord.isMarkedForRemoval()) {
				if (StartNameServer.debugMode) GNS.getLogger().fine("PAXOS DECISION NOT APPLIED: actives not changed because namerecord is markedForRemoval ");
				return;
			}
			
			if (nameRecord.isActiveRunning() == false) {
				if (StartNameServer.debugMode) GNS.getLogger().fine("PAXOS DECISION NOT APPLIED. Because most recently " +
						"proposed active name servers is not yet running: " + nameRecord.copyActiveNameServers());
				return;
			}
			
			// All primaries will apply this decision.
			
			nameRecord.updateActiveNameServers(activeProposalPacket.getProposedActiveNameServers(), 
					activeProposalPacket.getPaxosID());
			
			if (StartNameServer.debugMode) GNS.getLogger().fine("Name Record Now: = " + nameRecord.toString());
//			nameRecord.replaceActiveNameServers(activeProposalPacket.getProposedActiveNameServers());
			// Update Database.
            DBReplicaController.updateNameRecordPrimary(nameRecord);
//			NameServer.updateNameRecord(nameRecord);
			// TODO: 2-3 update database operations, reduce them.
			
			// Step 1 is done. New Actives are chosen.
			
			// Step 2: stop old paxos and write to primaries.
			// if I have proposed this change, I will inform actives of this change.
			if (activeProposalPacket.getProposingNode() == NameServer.nodeID) {
				if (StartNameServer.debugMode) GNS.getLogger().fine("PAXOS : Stop oldActiveSet now: Name = "
						+ activeProposalPacket.getName() + " Decision = " + decision);
				ReplicaController.stopOldActives(nameRecord);
				
				// we're using timer instead of executor service because we need repeat execution.
			}
			else {
				// This keeps the name record at primary in sync.
				numberOfReplica(nameRecord);
			}
			
		} catch (JSONException e) {
			if (StartNameServer.debugMode) GNS.getLogger().severe("JSON Exception in " +e.getMessage()) ;
		} 
//		catch (IOException e) {
//			if (StartNameServer.debugMode) GNRS.getLogger().severe("IO exception " + e.getMessage());
//			e.printStackTrace();
//		}
	}
	
//	/**
//	 * Updates name record at old/new actives (excluding primaries).
//	 * Primaries would be updated when Paxos completes.
//	 * @param nameRecord
//	 * @param count
//	 * @param oldActiveNameServers
//	 * @param newActiveNameServers
//	 * @throws JSONException
//	 * @throws IOException
//	 */
//	private static void updateOtherNameServers2(NameRecord nameRecord, int count,
//			Set<Integer> oldActiveNameServers, Set<Integer> newActiveNameServers)
//					throws JSONException, IOException{
//
//		//Set of active nameserver ids where name record should be replicated
//		Set<Integer> idReplicateRecords = new HashSet<Integer>();
//		//Set of active nameserver ids where name record should be removed
//		Set<Integer> idRemoveRecords = new HashSet<Integer>();
//		//Set of active nameservers that need the updated active nameserver set
//		Set<Integer> idUpdateRecords = new HashSet<Integer>();
//
//		//Add the new active name servers to the name record
//		for (Integer activeNameServerId : newActiveNameServers) {
//			if (!oldActiveNameServers.contains(activeNameServerId)) {
////				nameRecord.addActiveNameserver(activeNameServerId);
//				idReplicateRecords.add(activeNameServerId);
//			} else {
//				idUpdateRecords.add(activeNameServerId);
//			}
//		}
//
//		if (StartNameServer.debugMode) GNS.getLogger().fine("ComputeNewActives: " + count + " Name:" + nameRecord.getName()
//				+ " ReplicateRecord:" + idReplicateRecords.toString());
//		if (StartNameServer.debugMode) GNS.getLogger().fine("ComputeNewActives: " + count + " Name:" + nameRecord.getName()
//				+ " UpdateRecord:" + idUpdateRecords.toString());
//
//		//Remove old active nameservers that are not part of the new active nameserver set.
//		for (Integer activeNameServerId : oldActiveNameServers) {
//			if (!newActiveNameServers.contains(activeNameServerId)) {
////				nameRecord.removeActiveNameserver(activeNameServerId);
//				idRemoveRecords.add(activeNameServerId);
//			}
//		}
//
//		if (StartNameServer.debugMode) GNS.getLogger().fine("ComputeNewActives: " + count + " Name:" + nameRecord.getName()
//				+ " RemoveRecord:" + idRemoveRecords.toString());
//
//		//Since primary name servers always maintain the name record information,
//		//we avoid sending them RemoveRecordPacket or ReplicateRecordPacket
//		Set<Integer> excludePrimaryNS = nameRecord.getPrimaryNameservers();
//
//		if (idReplicateRecords.size() != 0) {
//			//Send name record information to the new active nameservers
//			ReplicateRecordPacket recordPacket = new ReplicateRecordPacket(nameRecord, NameServer.nodeID);
//			NameServer.tcpTransport.sendToAll(recordPacket.toJSONObject(), idReplicateRecords,
//					GNS.PortType.STATS_PORT, excludePrimaryNS);
//
//			StatusClient.sendTrafficStatus(NameServer.nodeID, idReplicateRecords, excludePrimaryNS,
//					GNS.PortType.STATS_PORT, recordPacket.getType());
//		}
//
//		if (idRemoveRecords.size() != 0) {
//			//Remove name record information from old active name servers
////			RemoveRecordPacket removePacket = new RemoveRecordPacket(nameRecord.getRecordKey(),
////					nameRecord.getName(), NameServer.nodeID);
//			RemoveReplicationRecordPacket removePacket = new RemoveReplicationRecordPacket(
//					//nameRecord.getRecordKey(),
//                                nameRecord.getName(), NameServer.nodeID);
//			NameServer.tcpTransport.sendToAll(removePacket.toJSONObject(), idRemoveRecords,
//					GNS.PortType.STATS_PORT, excludePrimaryNS);
//
//			StatusClient.sendTrafficStatus(NameServer.nodeID, idRemoveRecords, excludePrimaryNS,
//					GNS.PortType.STATS_PORT, removePacket.getType(), nameRecord.getName()
//                                //, nameRecord.getRecordKey()
//                                );
//		}
//
//		//Inform other name servers about the changes made to the active name server set.
//		//This includes the primary name server as well.
////		for (Integer primaryNSId : nameRecord.getPrimaryNameservers()) {
////			idUpdateRecords.add(primaryNSId);
////		}
//
//		if (idUpdateRecords.size() != 0 && (idReplicateRecords.size() != 0 || idRemoveRecords.size() != 0)) {
//			ActiveNSUpdatePacket updatePacket = new ActiveNSUpdatePacket(NameServer.nodeID,
//					//nameRecord.getRecordKey(),
//                                nameRecord.getName(), idReplicateRecords, idRemoveRecords);
//
//			NameServer.tcpTransport.sendToAll(updatePacket.toJSONObject(),
//					idUpdateRecords, GNS.PortType.STATS_PORT, excludePrimaryNS);
//
//			StatusClient.sendTrafficStatus(NameServer.nodeID, idUpdateRecords, excludePrimaryNS,
//					GNS.PortType.STATS_PORT, Packet.PacketType.ACTIVE_NAMESERVER_UPDATE,
//					nameRecord.getName()//, nameRecord.getRecordKey()
//                                );
//		}
//
//		if (StartNameServer.debugMode) GNS.getLogger().fine("\tActivesMatching\t" + nameRecord.copyActiveNameServers()
//				+ "\t" + newActiveNameServers);
////		newActiveNameServers
//
//		if (StartNameServer.debugMode) GNS.getLogger().info("Replication\t" + count + "\t" + nameRecord.getName()
//				+ "\t" + newActiveNameServers.size()
//				+ "\t" + nameRecord.getTotalReadFrequency() + "\t" + nameRecord.getTotalWriteFrequency()
//				+ "\t" + nameRecord.getReadAvg() + "\t" + nameRecord.getWriteAvg()
//				+ "\t" + nameRecord.getMovingAverageLookupString()
//				+ "\t" + nameRecord.getMovingAverageUpdateString()
//				+ "\t" + System.currentTimeMillis()
//				+ "\t" + newActiveNameServers.toString()
//				+ "\t" + idReplicateRecords.toString()
//				+ "\t" + idRemoveRecords.toString()
//				+ "\t" + idUpdateRecords.toString()
//				+ "\t" + nameRecord.copyActiveNameServers().toString()
//				+ "\t" + nameRecord.getPrimaryNameservers().toString());
//	}

	
}
