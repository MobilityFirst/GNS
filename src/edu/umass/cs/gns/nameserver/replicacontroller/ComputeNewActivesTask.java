package edu.umass.cs.gns.nameserver.replicacontroller;

import edu.umass.cs.gns.database.BasicRecordCursor;
import edu.umass.cs.gns.database.ColumnField;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.ReplicationFrameworkType;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.nameserver.recordmap.ReplicaControllerRecord;
import edu.umass.cs.gns.packet.NewActiveProposalPacket;
import edu.umass.cs.gns.packet.Packet;
import edu.umass.cs.gns.paxos.paxospacket.PaxosPacketType;
import edu.umass.cs.gns.paxos.paxospacket.RequestPacket;
import edu.umass.cs.gns.replicationframework.BeehiveReplication;
import edu.umass.cs.gns.test.FailureScenario;
import edu.umass.cs.gns.util.ConfigFileInfo;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.*;

/**
 *
 * For all names for which this node is a primary name server, this class periodically checks if the set of
 * active replicas needs to be changed depending on several factors: (1) rate of lookups (2) rates of updates
 * (3) which local name servers are receiving requests for a name.
 *
 * If it determines that the set of actives need to change, it starts the process by proposing a new set of
 * actives to other replica controllers.  Once replica controllers (aka primaries) have agreed upon a new set of
 * actives, they make this change in the database and start the process of informing old and new set of actives.
 *
 * For each name there will be multiple nodes who will be replica contollers. However, only one of these
 * nodes initiates the process of changing the set of active replica. Which node will do so is determined by:
 * <code>isSmallestNodeRunning</code> in <code>ReplicaController</code>.
 *
 * This class is implemented as a timer task which is periodically executed (once every <code>analysisInterval</code>.
 *
 * Note: 'primaries' and 'replica controllers' refer to same things in the code.
 * Note: we refer to the process of changing the set of active replicas as 'group change' at some places.
 *
 * If we start group change for a large number of names in a short interval, a large fraction of the node's resources
 * might be used in doing group change, which leaves less resources for actually executing client requests.
 * To limit the resource used in doing group changes, we sleep for a small interval (e.g. 10 ms) between successive
 * group change events.
 *
 * @author abhigyan
 * @deprecated
 */
public class ComputeNewActivesTask extends TimerTask {

  private static int replicationRound = 0;

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

    try {
      HashSet<String> namesConsidered = new HashSet<String>();
      GNS.getLogger().info("ComputeNewActives before getting iterator ... ");
      BasicRecordCursor iterator = NameServer.getReplicaController().getIterator(ReplicaControllerRecord.NAME, readFields);
      GNS.getLogger().info("ComputeNewActives started iterating. ");
      long t0 = System.currentTimeMillis();
      while (iterator.hasNext()) {
        count++;
        if (count % 10000 == 0) {
          GNS.getLogger().info("ComputeNewActives iterated over " + count + " names.");
        }

        HashMap<ColumnField, Object> hashMap = iterator.nextHashMap();
        ReplicaControllerRecord rcRecord = new ReplicaControllerRecord(NameServer.getReplicaController(), hashMap);

        if (StartNameServer.experimentMode &&  Integer.parseInt(rcRecord.getName()) >=
                StartNameServer.regularWorkloadSize + StartNameServer.mobileWorkloadSize) continue;

        if (rcRecord.isMarkedForRemoval()) {
          continue;
        }

        if (StartNameServer.debugMode) {
          GNS.getLogger().fine("\tComputeNewActivesConsidering\t" + rcRecord.getName() + "\tCount\t" + count +
                  "\tRound\t" + replicationRound);
        }

        if (!rcRecord.getPrimaryNameservers().contains(NameServer.getNodeID())
                || !ReplicaController.isSmallestNodeRunning(rcRecord.getName(), rcRecord.getPrimaryNameservers())) {
          rcRecord.recomputeAverageReadWriteRate(); // this will keep moving average calculation updated.
          continue;
        }

        namesConsidered.add(rcRecord.getName());

      }
      long t1 = System.currentTimeMillis();
      GNS.getLogger().info(" ComputeNewActives NamesConsidered " + namesConsidered.size() + "\tDuration = " +
              (t1 - t0) + "ms");

      t0 = System.currentTimeMillis();
      int nameCount = 0;
      for (String name : namesConsidered) {
        ReplicaControllerRecord rcRecord = ReplicaControllerRecord.getNameRecordPrimaryMultiField(NameServer.getReplicaController(), name, readFields);
        if (StartNameServer.debugMode) {
          GNS.getLogger().fine("I will select new actives for name = " + rcRecord.getName());
        }
        Set<Integer> oldActiveNameServers = rcRecord.getActiveNameservers();
        Set<Integer> newActiveNameServers = getNewActiveNameServers(rcRecord, rcRecord.getActiveNameservers(), replicationRound);
        if (isActiveSetModified(oldActiveNameServers, newActiveNameServers)) {
          nameCount++;
          if (StartNameServer.debugMode) {
            GNS.getLogger().fine("\tComputeNewActives\t" + rcRecord.getName()
                    + "\tCount\t" + count + "\tRound\t" + replicationRound + "\tUpdatingOtherActives");
          }

          String newActivePaxosID = ReplicaController.getActivePaxosID(rcRecord);
          NewActiveProposalPacket activePropose = new NewActiveProposalPacket(rcRecord.getName(), NameServer.getNodeID(),
                  newActiveNameServers, newActivePaxosID);
          String paxosID = ReplicaController.getPrimaryPaxosID(rcRecord);
          boolean isStop = false;
          RequestPacket requestPacket = new RequestPacket(Packet.PacketType.NEW_ACTIVE_PROPOSE.getInt(), activePropose.toString(),
                  PaxosPacketType.REQUEST, isStop);

          if (StartNameServer.debugMode) GNS.getLogger().info("Proposal to paxosID: "  + paxosID);
          String x = NameServer.getPaxosManager().propose(paxosID, requestPacket);

          if (StartNameServer.debugMode) {
            GNS.getLogger().fine("PAXOS PROPOSAL: Proposal done. Response: " + x);
          }
          try {
            Thread.sleep(5); // sleep between successive names so we do not start a large number of group changes
            // at the same time and a large fraction of the system resources are used in just doing group changes.
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        } else {
          if (StartNameServer.debugMode) {
            GNS.getLogger().fine("Old and new active name servers are same. No Operation.");
          }
        }

      }
      t1 = System.currentTimeMillis();
      GNS.getLogger().info(" Compute New Actives Summary. Total Names = " + namesConsidered.size() +
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
   * Apply the decision from paxos. Packet = NewActiveProposalPacket.
   * @param decision
   */
  public static void applyNewActivesProposed(String decision) {

    if (StartNameServer.experimentMode) StartNameServer.checkFailure(FailureScenario.applyNewActivesProposed);

    try {
      NewActiveProposalPacket activeProposalPacket = new NewActiveProposalPacket(new JSONObject(decision));

      ReplicaControllerRecord rcRecordPrimary = ReplicaControllerRecord.getNameRecordPrimaryMultiField(
              NameServer.getReplicaController(), activeProposalPacket.getName(), getReadFieldsApplyNewActivesProposed());

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
      if (activeProposalPacket.getProposingNode() == NameServer.getNodeID() || // if I have proposed this change, I will start actives group change process
              NameServer.getPaxosManager().isNodeUp(activeProposalPacket.getProposingNode()) == false) { // else if proposing node has failed, then also I will start group change
        GroupChangeProgress.updateGroupChangeProgress(activeProposalPacket.getName(), GroupChangeProgress.GROUP_CHANGE_START);
        ReplicaController.groupChangeStartTimes.put(rcRecordPrimary.getName(), System.currentTimeMillis());
        if (StartNameServer.debugMode) {
          GNS.getLogger().fine("PAXOS : Stop oldActiveSet now: Name = "
                  + activeProposalPacket.getName() + " Decision = " + decision);
        }
        StopActiveSetTask stopTask = new StopActiveSetTask(activeProposalPacket.getName(),
                rcRecordPrimary.getOldActiveNameservers(), rcRecordPrimary.getOldActivePaxosID());
        NameServer.getTimer().schedule(stopTask, 0, ReplicaController.RC_TIMEOUT_MILLIS);
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



  /************************ Methods below are private methods ************************/

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
    newActiveNameServers = NameServer.getReplicationFramework().newActiveReplica(rcRecord, numReplica, count);


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


}
