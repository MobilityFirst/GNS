package edu.umass.cs.gns.nameserver.replicacontroller;

import edu.umass.cs.gns.database.BasicRecordCursor;
import edu.umass.cs.gns.database.ColumnField;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.ReplicationFrameworkType;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.packet.NewActiveProposalPacket;
import edu.umass.cs.gns.packet.Packet;
import edu.umass.cs.gns.packet.paxospacket.PaxosPacketType;
import edu.umass.cs.gns.packet.paxospacket.RequestPacket;
import edu.umass.cs.gns.paxos.PaxosManager;
import edu.umass.cs.gns.replicationframework.BeehiveReplication;
import edu.umass.cs.gns.test.FailureScenario;
import edu.umass.cs.gns.util.ConfigFileInfo;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.*;

/**
 * Computes new actives for all name records for which this node is primary name server.
 * @author abhigyan
 *
 */
public class ComputeNewActivesTask extends TimerTask {

  static int replicationRound = 0;

  @Override
  public void run() {
    replicationRound++;

    if (replicationRound > 1) return;

    GNS.getLogger().severe("ComputeNewActives started: " + replicationRound);

    ArrayList<ColumnField> readFields = new ArrayList<ColumnField>();
    readFields.add(ReplicaControllerRecord.MARKED_FOR_REMOVAL);
    readFields.add(ReplicaControllerRecord.PRIMARY_NAMESERVERS);
    readFields.add(ReplicaControllerRecord.ACTIVE_NAMESERVERS);

    readFields.add(ReplicaControllerRecord.PREV_TOTAL_READ);
    readFields.add(ReplicaControllerRecord.PREV_TOTAL_WRITE);
    readFields.add(ReplicaControllerRecord.MOV_AVG_READ);
    readFields.add(ReplicaControllerRecord.MOV_AVG_WRITE);
    readFields.add(ReplicaControllerRecord.VOTES_MAP);


    int count = 0;

//    Object iterator = NameServer.replicaController.getIterator(ReplicaControllerRecord.NAME,readFields);
//    if (StartNameServer.debugMode) GNS.getLogger().info("Got iterator : " + replicationRound);



//      while (true) {
//        count++;
//        HashMap<Field,Object> hashMap = NameServer.replicaController.next(iterator, ReplicaControllerRecord.NAME, readFields);
////        if (StartNameServer.debugMode) GNS.getLogger().infor("Got next: " + count);
//        if (hashMap == null) {
////          if (StartNameServer.debugMode) GNS.getLogger().infor("BREAK!! ");
//          break;
//        }
//        ReplicaControllerRecord rcRecord = new ReplicaControllerRecord(hashMap);

    //Iterate through the rcRecord and check if any changes need to
    //be made to the active name server set
//      Set<ReplicaControllerRecord> rcRecords = NameServer.getAllPrimaryNameRecords();
//      if (StartNameServer.debugMode) GNS.getLogger().info("\tComputeNewActivesStart\tNumberOfrcRecords\t" + rcRecords.size());
//
//      for (ReplicaControllerRecord rcRecord : rcRecords) {
    try {
      HashSet<String> namesConsidered = new HashSet<String>();
      GNS.getLogger().severe("ComputeNewActives before getting iterator ... ");
      BasicRecordCursor iterator = NameServer.replicaController.getIterator(ReplicaControllerRecord.NAME, readFields);
      GNS.getLogger().severe("ComputeNewActives started iterating. ");
      long t0 = System.currentTimeMillis();
      while (iterator.hasNext()) {
        count++;
        if (count % 10000 == 0) {
          GNS.getLogger().severe("ComputeNewActives iterated over " + count + " names.");
        }

        HashMap<ColumnField, Object> hashMap = iterator.nextHashMap();
        ReplicaControllerRecord rcRecord = new ReplicaControllerRecord(hashMap);

        if (StartNameServer.experimentMode &&  Integer.parseInt(rcRecord.getName()) >=
                StartNameServer.regularWorkloadSize + StartNameServer.mobileWorkloadSize) continue;

        if (rcRecord.isMarkedForRemoval()) {
          continue;
        }

        if (StartNameServer.debugMode) {
          GNS.getLogger().fine("\tComputeNewActivesConsidering\t" + rcRecord.getName() + "\tCount\t" + count +
                  "\tRound\t" + replicationRound);
        }

        if (!rcRecord.getPrimaryNameservers().contains(NameServer.nodeID)
                || !ReplicaController.isSmallestNodeRunning(rcRecord.getName(), rcRecord.getPrimaryNameservers())) {
          rcRecord.recomputeAverageReadWriteRate(); // this will keep moving average calculation updated.
          continue;
        }

        namesConsidered.add(rcRecord.getName());
//        try {
//          Thread.sleep(10); // sleep between successive names so as to keep traffic smooth
//        } catch (InterruptedException e) {
//          e.printStackTrace();
//        }
      }
      long t1 = System.currentTimeMillis();
      GNS.getLogger().severe(" ComputeNewActives NamesConsidered " + namesConsidered.size() + "\tDuration = " +
              (t1 - t0) + "ms");


      if (StartNameServer.experimentMode && StartNameServer.quitAfterTimeSec >= 0) {
        Thread t = new Thread() {
          @Override
          public void run() {
            Random r = new Random();
            long quitAfterTimeMillis = 500 + r.nextInt(3000);
            GNS.getLogger().severe("Sleeping for " + quitAfterTimeMillis + " milli sec before quitting ...");
            try {
              Thread.sleep(quitAfterTimeMillis);
            } catch (InterruptedException e) {
              e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
            GNS.getLogger().severe("SYSTEM EXIT.");
            System.exit(2);
          }
        };
        t.start();
      }
      t0 = System.currentTimeMillis();
      int nameCount = 0;
      for (String name : namesConsidered) {
        ReplicaControllerRecord rcRecord = NameServer.getNameRecordPrimaryMultiField(name, readFields);
        if (StartNameServer.debugMode) {
          GNS.getLogger().fine("I will select new actives for name = " + rcRecord.getName());
        }

        Set<Integer> oldActiveNameServers = rcRecord.getActiveNameservers();
        Set<Integer> newActiveNameServers = getNewActiveNameServers(rcRecord, rcRecord.getActiveNameservers(), replicationRound);
//        if (isActiveSetModified(oldActiveNameServers, newActiveNameServers)) {
          nameCount++;
          if (StartNameServer.debugMode) {
            GNS.getLogger().fine("\tComputeNewActives\t" + rcRecord.getName()
                    + "\tCount\t" + count + "\tRound\t" + replicationRound + "\tUpdatingOtherActives");
          }

          String newActivePaxosID = ReplicaController.getActivePaxosID(rcRecord);
          NewActiveProposalPacket activePropose = new NewActiveProposalPacket(rcRecord.getName(), NameServer.nodeID,
                  newActiveNameServers, newActivePaxosID);
          String paxosID = ReplicaController.getPrimaryPaxosID(rcRecord);
          boolean isStop = false;
          RequestPacket requestPacket = new RequestPacket(Packet.PacketType.NEW_ACTIVE_PROPOSE.getInt(), activePropose.toString(),
                  PaxosPacketType.REQUEST, isStop);

          if (StartNameServer.debugMode) GNS.getLogger().info("Proposal to paxosID: "  + paxosID);
          String x = NameServer.paxosManager.propose(paxosID, requestPacket);

          if (StartNameServer.debugMode) {
            GNS.getLogger().fine("PAXOS PROPOSAL: Proposal done. Response: " + x);
          }
          try {
            Thread.sleep(5); // sleep between successive names so as to keep traffic smooth
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
//        } else {
//          if (StartNameServer.debugMode) {
//            GNS.getLogger().fine("Old and new active name servers are same. No Operation.");
//          }
//        }

      }
      t1 = System.currentTimeMillis();
      GNS.getLogger().severe(" Compute New Actives Summary. Total Names = " + namesConsidered.size() +
              " Group Change Names = " + nameCount + "\tDuration = " + (t1 - t0));
    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field Not Found Exception: " + e.getMessage());
      e.printStackTrace();
    } catch (Exception e) {
      GNS.getLogger().severe("Exception Exception Exception " + e.getMessage());
      e.printStackTrace();
    }

    GNS.getLogger().fine("Reached end of code ... ");



  }

  /**
   * Returns true if the set of new actives is identical to the set of old actives. False otherwise.
   * @param oldActives
   * @param newActives
   * @return
   */
  private boolean isActiveSetModified(Set<Integer> oldActives, Set<Integer> newActives) {
    if (oldActives.size() != newActives.size()) {
      return true;
    }
    for (int x : oldActives) {
      if (newActives.contains(x) == false) {
        return true;
      }
    }
    return false;
  }

  /**
   * Calculates new set of active name servers depending on replication framework.
   * @param rcRecord
   */
  private Set<Integer> getNewActiveNameServers_test(ReplicaControllerRecord rcRecord,
          Set<Integer> oldActiveNameServers, int count) throws FieldNotFoundException {
    Random r = new Random();
    Set<Integer> newActiveNameServers = new HashSet<Integer>();
    while (newActiveNameServers.size() < StartNameServer.minReplica) {
      int ns = r.nextInt(ConfigFileInfo.getNumberOfNameServers());
      newActiveNameServers.add(ns);
    }
    return newActiveNameServers;
  }

  /**
   * Calculates new set of active name servers depending on replication framework.
   * @param rcRecord
   */
  private Set<Integer> getNewActiveNameServers(ReplicaControllerRecord rcRecord, Set<Integer> oldActiveNameServers,
          int count) throws FieldNotFoundException {

    Set<Integer> newActiveNameServers;

    int numReplica = numberOfReplica(rcRecord);

    // used for beehive.
    if (StartNameServer.replicationFramework == ReplicationFrameworkType.BEEHIVE) {
      numReplica = BeehiveReplication.numActiveNameServers(rcRecord.getName()) - 3;
    }

    //Get a new set of active name servers for this record
    newActiveNameServers = NameServer.replicationFramework.newActiveReplica(rcRecord, numReplica, count);


    if (StartNameServer.debugMode) {
      GNS.getLogger().fine("ComputeNewActives: Round:" + count + " Name:" + rcRecord.getName()
              + " OldActive:" + oldActiveNameServers.toString() + " NumberReplica:" + numReplica
              + " NewReplica:" + newActiveNameServers.toString());
    }


    GNS.getStatLogger().info("ComputeNewActives: Round:" + count + " Name:" + rcRecord.getName()
            + " OldActive:" + oldActiveNameServers.toString() + " NumberReplica:" + numReplica
            + " NewReplica:" + newActiveNameServers.toString());
    return newActiveNameServers;
  }

  /**
   * ***********************************************************
   * Returns the size of active replicas set that should exist for this name record.
   * The size of the active replica set
   * depends on the lookup and update rate of this name record.
   *
   * @param rcRecord Name record
   ***********************************************************
   */
  private static int numberOfReplica(ReplicaControllerRecord rcRecord) throws FieldNotFoundException {
    double[] readWrites = rcRecord.recomputeAverageReadWriteRate();
    double lookup = readWrites[0];
    double update = readWrites[1];

//		update = rcRecord.getWriteStats_Paxos();
//		lookup = rcRecord.getReadStats_Paxos();

//    NameServer.updateNameRecordPrimary(rcRecord);

    int replicaCount = 0;
    if (update == 0 && lookup == 0) {
      // no requests seen, replicate at minimum number of locations.
      replicaCount = StartNameServer.minReplica;
    } else if (update == 0) {
      // no updates, replicate everywhere.
      replicaCount = ConfigFileInfo.getNumberOfNameServers();
    } else {
      replicaCount = StrictMath.round(StrictMath.round(
              (lookup / (update * StartNameServer.normalizingConstant) + StartNameServer.minReplica)));

      if (replicaCount > ConfigFileInfo.getNumberOfNameServers()) {
        replicaCount = ConfigFileInfo.getNumberOfNameServers();
      }
    }

    // put in here for DNS experiments.
    if (replicaCount > StartNameServer.maxReplica) {
      replicaCount = StartNameServer.maxReplica;
    }

    GNS.getStatLogger().info("\tComputeNewActives-ReplicaCount\tName\t"
            + rcRecord.getName() + "\tLookup\t" + lookup + "\tUpdate\t" + update
            + "\tReplicaCount\t" + replicaCount);

    return replicaCount;
  }
  private static ArrayList<ColumnField> readFieldsApplyNewActivesProposed = new ArrayList<ColumnField>();

  private static ArrayList<ColumnField> getReadFieldsApplyNewActivesProposed() {
    synchronized (readFieldsApplyNewActivesProposed) {
      if (readFieldsApplyNewActivesProposed.size() > 0) {
        return readFieldsApplyNewActivesProposed;
      }

      readFieldsApplyNewActivesProposed.add(ReplicaControllerRecord.MARKED_FOR_REMOVAL);
      readFieldsApplyNewActivesProposed.add(ReplicaControllerRecord.ACTIVE_NAMESERVERS_RUNNING);
      readFieldsApplyNewActivesProposed.add(ReplicaControllerRecord.ACTIVE_NAMESERVERS);
      readFieldsApplyNewActivesProposed.add(ReplicaControllerRecord.ACTIVE_PAXOS_ID);
      return readFieldsApplyNewActivesProposed;
    }
  }
  static boolean expFlag = true;


  /**
   * Apply the decision from paxos. Packet = NewActiveProposalPacket.
   * @param decision
   */
  public static void applyNewActivesProposed(String decision) {

    if (StartNameServer.experimentMode) StartNameServer.checkFailure(FailureScenario.applyNewActivesProposed);

    try {
      NewActiveProposalPacket activeProposalPacket = new NewActiveProposalPacket(new JSONObject(decision));

      ReplicaControllerRecord rcRecordPrimary = NameServer.getNameRecordPrimaryMultiField(activeProposalPacket.getName(), getReadFieldsApplyNewActivesProposed());

      if (rcRecordPrimary == null) {
        if (StartNameServer.debugMode) {
          GNS.getLogger().warning("ERROR: PAXOS DECISION: "
                  + "BUT PRIMARY NAME RECORD DELETED Name = " + activeProposalPacket.getName());
        }
        return;
      }

      if (StartNameServer.debugMode) {
        GNS.getLogger().fine("PAXOS DECISION: Name = " + activeProposalPacket.getName()
                + " Actives: " + activeProposalPacket.getProposedActiveNameServers() + " DECISION: " + decision);
      }

      if (rcRecordPrimary.isMarkedForRemoval()) {
        if (StartNameServer.debugMode) {
          GNS.getLogger().fine("PAXOS DECISION NOT APPLIED: actives not changed because rcRecord is markedForRemoval ");
        }

        return;
      }

      if (rcRecordPrimary.isActiveRunning() == false) {
        if (StartNameServer.debugMode) {
          GNS.getLogger().fine("PAXOS DECISION NOT APPLIED. Because most recently "
                  + "proposed active name servers is not yet running: " + rcRecordPrimary.getActiveNameservers());
        }

        return;
      }
      if (rcRecordPrimary.getActivePaxosID().equals(activeProposalPacket.getPaxosID())) {
        if (StartNameServer.debugMode) GNS.getLogger().info("PAXOS DECISION NOT APPLIED. Old and new paxosIDs are same"
                + rcRecordPrimary.getActivePaxosID());
        return;
      }

      if (ReplicaController.groupChangeStartTimes.containsKey(rcRecordPrimary.getName())) {
        GNS.getLogger().warning("Exception: group change not completed and new group change in progress. " + rcRecordPrimary.getName());
      }

      // All primaries will apply this decision.
      rcRecordPrimary.updateActiveNameServers(activeProposalPacket.getProposedActiveNameServers(),
              activeProposalPacket.getPaxosID());

      if (StartNameServer.debugMode) {
        GNS.getLogger().fine("Name Record Now: = " + rcRecordPrimary.toString());
      }

      // Step 1 complete: New actives are chosen.

      // Step 2: stop old paxos and write to primaries.
      if (activeProposalPacket.getProposingNode() == NameServer.nodeID || // if I have proposed this change, I will start actives group change process
              NameServer.paxosManager.isNodeUp(activeProposalPacket.getProposingNode()) == false) { // else if proposing node has failed, then also I will start group change
        ReplicaController.updateGroupChangeProgress(activeProposalPacket.getName(), ReplicaController.STOP_SENT);
        ReplicaController.groupChangeStartTimes.put(rcRecordPrimary.getName(), System.currentTimeMillis());
        if (StartNameServer.debugMode) {
          GNS.getLogger().fine("PAXOS : Stop oldActiveSet now: Name = "
                  + activeProposalPacket.getName() + " Decision = " + decision);
        }
        StopActiveSetTask stopTask = new StopActiveSetTask(activeProposalPacket.getName(),
                rcRecordPrimary.getOldActiveNameservers(), rcRecordPrimary.getOldActivePaxosID());
        NameServer.timer.schedule(stopTask, 0, ReplicaController.RC_TIMEOUT_MILLIS);
      }

    } catch (JSONException e) {
      if (StartNameServer.debugMode) {
        GNS.getLogger().severe("JSON Exception in " + e.getMessage());
      }
    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Unexpected Error: New Actives Accepted. " + e.getMessage());
      e.printStackTrace();
    } catch (RecordNotFoundException e) {
      GNS.getLogger().severe("Unexpected Error: New Actives Accepted But Record Not Exists. " + e.getMessage());
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }
//	/**
//	 * Updates name record at old/new actives (excluding primaries).
//	 * Primaries would be updated when Paxos completes.
//	 * @param rcRecord
//	 * @param count
//	 * @param oldActiveNameServers
//	 * @param newActiveNameServers
//	 * @throws JSONException
//	 * @throws IOException
//	 */
//	private static void updateOtherNameServers2(rcRecord nameRecord, int count,
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
//		if (StartNameServer.debugMode) GNS.getLogger().info("ComputeNewActives: " + count + " Name:" + nameRecord.getName()
//				+ " ReplicateRecord:" + idReplicateRecords.toString());
//		if (StartNameServer.debugMode) GNS.getLogger().info("ComputeNewActives: " + count + " Name:" + nameRecord.getName()
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
//		if (StartNameServer.debugMode) GNS.getLogger().info("ComputeNewActives: " + count + " Name:" + nameRecord.getName()
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
//					GNS.PortType.PERSISTENT_TCP_PORT, excludePrimaryNS);
//
//			StatusClient.sendTrafficStatus(NameServer.nodeID, idReplicateRecords, excludePrimaryNS,
//					GNS.PortType.PERSISTENT_TCP_PORT, recordPacket.getType());
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
//					GNS.PortType.PERSISTENT_TCP_PORT, excludePrimaryNS);
//
//			StatusClient.sendTrafficStatus(NameServer.nodeID, idRemoveRecords, excludePrimaryNS,
//					GNS.PortType.PERSISTENT_TCP_PORT, removePacket.getType(), nameRecord.getName()
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
//					idUpdateRecords, GNS.PortType.PERSISTENT_TCP_PORT, excludePrimaryNS);
//
//			StatusClient.sendTrafficStatus(NameServer.nodeID, idUpdateRecords, excludePrimaryNS,
//					GNS.PortType.PERSISTENT_TCP_PORT, Packet.PacketType.ACTIVE_NAMESERVER_UPDATE,
//					nameRecord.getName()//, nameRecord.getRecordKey()
//                                );
//		}
//
//		if (StartNameServer.debugMode) GNS.getLogger().info("\tActivesMatching\t" + nameRecord.copyActiveNameServers()
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
