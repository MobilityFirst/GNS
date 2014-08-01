package edu.umass.cs.gns.reconfigurator;


import edu.umass.cs.gns.database.ColumnField;
import edu.umass.cs.gns.exceptions.FailedDBOperationException;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.MessagingTask;
import edu.umass.cs.gns.nio.NIOTransport;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.packet.*;
import edu.umass.cs.gns.nsdesign.recordmap.BasicRecordMap;
import edu.umass.cs.gns.nsdesign.recordmap.ReplicaControllerRecord;
import edu.umass.cs.gns.util.GroupChangeIdentifier;
import edu.umass.cs.gns.util.UniqueIDHashMap;
import edu.umass.cs.gns.util.Util;

import org.json.JSONException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * @author V. Arun
 * Based on code created by abhigyan on 2/27/14.
 * 
 * Implements group changes in the active replica set of a name. A group change consists of these steps:
 * (1) A replica controller, called manager, proposing to change active replica set and all replica controllers agreeing to it.
 * (2) The manager replica controller sends request to a current active replicas to stop.
 * (3) That active replica confirming to the manager that current active replicas have stopped.
 * (4) The manager informs one of the new active replicas to start functioning, who in turn inform others.
 * (5) That active replica confirming to the manager that majority of new active replicas have started functioning.
 * (6) Manager proposes an update to replica controllers that group change is completed and all replica controllers agreeing to it.
 * (7) Last step is to clean up state at old active replicas, for which manage informs all old active replicas to clean
 * up state for this name record.
 *
 * <p>
 * We ensure that there is only one group change for a name at any time. To this end, when replica controllers
 * agree on a group change, they update a variable indicating that group change is progress; replica controllers
 * do not agree to any other group change unless this group change is marked as completed. After group change is
 * completed, they again update this variable to indicate that group change has completed. This second update is important
 * because the next group change cannot start unless this update is done.</p>
 *
 * <p>
 * If a manager does get a response from either old or new active replicas, it retries sending those messages.
 * We expect that the request are successful on a few retries. Otherwise, the manager gives up, and group change is left
 * incomplete at that stage.</p>
 *
 * <p>
 * Also refer to documentation in activeReconfiguration/GroupChange on how active replicas implement group change.</p>
 *
 *
 * 
 */
public class GroupChange {

	private static Logger log = NIOTransport.LOCAL_LOGGER ? Logger.getLogger(Add.class.getName()) : GNS.getLogger();

	/**
	 * These are set of fields that different methods will read from database. We try to keep variable names
	 * similar to method names. If method name changes, update variable names accordingly.
	 */
	private static ArrayList<ColumnField> executeNewActivesProposedFields = new ArrayList<ColumnField>();

	static {
		executeNewActivesProposedFields.add(ReplicaControllerRecord.MARKED_FOR_REMOVAL);
		executeNewActivesProposedFields.add(ReplicaControllerRecord.ACTIVE_NAMESERVERS_RUNNING);
		executeNewActivesProposedFields.add(ReplicaControllerRecord.ACTIVE_NAMESERVERS);
		executeNewActivesProposedFields.add(ReplicaControllerRecord.ACTIVE_VERSION);
	}

	private static ArrayList<ColumnField> startActiveSetFields = new ArrayList<ColumnField>();

	static {
		startActiveSetFields.add(ReplicaControllerRecord.OLD_ACTIVE_VERSION);
		startActiveSetFields.add(ReplicaControllerRecord.ACTIVE_VERSION);
		startActiveSetFields.add(ReplicaControllerRecord.OLD_ACTIVE_NAMESERVERS);
		startActiveSetFields.add(ReplicaControllerRecord.ACTIVE_NAMESERVERS_RUNNING);
		startActiveSetFields.add(ReplicaControllerRecord.ACTIVE_NAMESERVERS);
		startActiveSetFields.add(ReplicaControllerRecord.PRIMARY_NAMESERVERS);
	}

	private static ArrayList<ColumnField> newActiveStartedFields = new ArrayList<ColumnField>();

	static {
		newActiveStartedFields.add(ReplicaControllerRecord.MARKED_FOR_REMOVAL);
		newActiveStartedFields.add(ReplicaControllerRecord.ACTIVE_VERSION);
		newActiveStartedFields.add(ReplicaControllerRecord.ACTIVE_NAMESERVERS);
		newActiveStartedFields.add(ReplicaControllerRecord.ACTIVE_NAMESERVERS_RUNNING);
	}

	/**
	 * OBJECT USED ONLY DURING TESTING!
	 * Hash map stores info about group changes that are requested by local name server.
	 * This is used only for collecting statistics regarding group change.
	 */
	private final static ConcurrentHashMap<GroupChangeIdentifier, Integer> TESTTrackGroupChange =
			new ConcurrentHashMap<GroupChangeIdentifier, Integer>();

	/**
	 * After replica controllers agree on changing the set of active replicas, this method updates the database to
	 * indicate a group change for this name is in progress.
	 * 
	 * Arun: This method also seems to be stopping old actives.
	 *
	 * @param activeProposalPacket Actives proposed to primary replicas
	 */
	public static void executeNewActivesProposed(NewActiveProposalPacket activeProposalPacket, BasicRecordMap DB, int rcID, 
			boolean recovery, RCProtocolTask[] protocolTasks /*return value*/) {

		try {
			ReplicaControllerRecord rcRecord = ReplicaControllerRecord.getNameRecordPrimaryMultiField(
					DB, activeProposalPacket.getName(), executeNewActivesProposedFields);
			if(earlyReturnCheck(rcRecord, rcID, activeProposalPacket)) return;

			// All replica controllers will locally apply this decision.
			log.fine("Node "+rcID+ " DECISION: Name = " + activeProposalPacket.getName() + " Actives: "
					+ activeProposalPacket.getProposedActiveNameServers());
			rcRecord.updateActiveNameServers(activeProposalPacket.getProposedActiveNameServers(),
					activeProposalPacket.getVersion());
			log.fine("Node "+rcID+" name record after update: = " + rcRecord.toString());

			if(recovery) return;

			// for some kinda testing
			if (activeProposalPacket.getProposingNode() == rcID && activeProposalPacket.getLnsId() != -1) {
				TESTTrackGroupChange.put(new GroupChangeIdentifier(activeProposalPacket.getName(), 
						activeProposalPacket.getVersion()), activeProposalPacket.getLnsId());
			}

			stopOldActives(activeProposalPacket, rcID, rcRecord, protocolTasks /*return value*/);

		} catch (FieldNotFoundException e) {
			log.severe("Unexpected Error: New Actives Accepted. " + e.getMessage());
			e.printStackTrace();
		} catch (RecordNotFoundException e) {
			// this could happen in rare cases when remove request for a name arrives at the same time as a group change
			log.warning("GROUP CHANGE DECISION: BUT PRIMARY NAME RECORD DELETED Name = " + activeProposalPacket.getName());
		} catch (FailedDBOperationException e) {
			log.severe("Unexpected Error!" + e.getMessage());
			e.printStackTrace();
		}
	}
	private static void stopOldActives(NewActiveProposalPacket activeProposalPacket, int rcID, 
			ReplicaControllerRecord rcRecord, Runnable[] protocolTasks /*return value*/) 
					throws FieldNotFoundException {
		if (activeProposalPacket.getProposingNode() == rcID) {// proposer RC initiates stopping of old actives
			GNS.getStatLogger().info("\tGroupChange\tname" + rcRecord.getName() + "\tversion\t"
					+ activeProposalPacket.getVersion() + "\tNewActives\t" + activeProposalPacket.getProposedActiveNameServers() + "\t");
			// todo could use failure detector here
			// if NameServer.getManager().isNodeUp(activeProposalPacket.getProposingNode()) == false
			// then proposing node has failed, so I will start group change
			if (Config.debuggingEnabled) log.fine("Node "+rcID+" : stop oldActiveSet name = " + activeProposalPacket.getName());
			StopActiveSetTask stopTask = new StopActiveSetTask(activeProposalPacket.getName(),
					rcRecord.getOldActiveNameservers(), rcRecord.getOldActiveVersion(),
					Packet.PacketType.OLD_ACTIVE_STOP, activeProposalPacket);
			assert(protocolTasks.length>0);
			protocolTasks[0] = stopTask; // return value
		}
	}
	private static boolean earlyReturnCheck(ReplicaControllerRecord rcRecord, int rcID, NewActiveProposalPacket activeProposalPacket) 
			throws FieldNotFoundException {
		if (rcRecord == null) return true;

		log.fine("Node "+rcID+ " read record: " + rcRecord);
		if (rcRecord.isMarkedForRemoval()) { // could happen when remove request for a name arrives during a group change
			log.fine("Node "+rcID +" DECISION NOT APPLIED: actives not changed because rcRecord is markedForRemoval ");
			return true;
		}
		if (!rcRecord.isActiveRunning()) {
			// this means another group change is already in progress. this can happen if another node proposed a group
			// change at same time OR  previous group change did not complete successfully.
			log.warning("Node "+rcID + " DECISION NOT APPLIED. Because most recently "
					+ "proposed active name servers is not yet running: " + rcRecord.getActiveNameservers());
			return true;
		}
		if (rcRecord.getActiveVersion() == activeProposalPacket.getVersion()) {
			log.info("Node "+rcID + " DECISION NOT APPLIED. Old and new versions are same" + rcRecord.getActiveVersion());
			return true;
		}
		return false;
	}

	/**
	 * Old set of active replicas have stopped, and therefore new set of active replicas can be started.
	 */
	public static void handleOldActiveStop(OldActiveSetStopPacket oldActiveSetStop, BasicRecordMap DB, 
			int rcID, UniqueIDHashMap ongoingStops, RCProtocolTask[] protocolTasks/*return value*/) throws FailedDBOperationException {
		if (ongoingStops.remove(oldActiveSetStop.getRequestID()) != null) {
			createStartActiveSetTask(oldActiveSetStop, DB, rcID, protocolTasks/*return value*/);
		} else {
			log.info("Confirmation is received previously or is excessively delayed. " + oldActiveSetStop);
		}
	}

	/**
	 * Old actives have stopped, create a task to start new actives for name.
	 */
	private static void createStartActiveSetTask(OldActiveSetStopPacket packet, BasicRecordMap DB, int rcID, 
			RCProtocolTask[] protocolTasks/*return value*/) throws FailedDBOperationException {
		try {
      ReplicaControllerRecord rcRecord = ReplicaControllerRecord.getNameRecordPrimaryMultiField(
              DB, packet.getName(), startActiveSetFields);

      if (Config.debuggingEnabled) {
				log.info("Old active stopped. write to nameRecord: " + packet.getName());
			}
			if (!rcRecord.isActiveRunning()) {
				if (Config.debuggingEnabled) {
					log.info("OLD Active  stopped. Name: " + rcRecord.getName() + " Old Version: "
							+ packet.getVersion());
				}
				StartActiveSetTask startupTask = new StartActiveSetTask(rcRecord.getName(),
						rcRecord.getOldActiveNameservers(), rcRecord.getActiveNameservers(), rcRecord.getActiveVersion(),
						rcRecord.getOldActiveVersion(), null);
				assert(protocolTasks.length>0);
				protocolTasks[0] = startupTask; // return value
			} else {
				log.info("IGNORE MSG: GROUP CHANGE ALREADY COMPLETE. " + packet.getVersion());
			}
		} catch (RecordNotFoundException e) {
			log.severe("Name record not found. This case should not happen. " + e.getMessage());
			e.printStackTrace();
		} catch (FieldNotFoundException e) {
			log.severe("Field not found exception. " + e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * ReplicaController has received message from an active that a majority of new actives have been informed.
	 * This method proposes a request to update replica controllers that new actives are running, and group change
	 * is complete.
	 */
	public static MessagingTask[] handleNewActiveStartConfirmMessage(NewActiveSetStartupPacket packet, int rcID, 
			UniqueIDHashMap ongoingStarts, RCProtocolTask[] protocolTasks) throws JSONException, IOException {
		MessagingTask notifyRC = null, notifyOldActives = null;
		if (Config.debuggingEnabled) log.info("NEW_ACTIVE_START: Received confirmation at primary. " + packet.getName());

		Object removed = ongoingStarts.remove(packet.getUniqueID());
		if (removed != null) {
			// inform old actives to delete state
			OldActiveSetStopPacket oldActiveSetStopPacket = new OldActiveSetStopPacket(packet.getName(), 0,
					rcID, -1, packet.getOldActiveVersion(), Packet.PacketType.DELETE_OLD_ACTIVE_STATE);
			notifyOldActives = new MessagingTask(Util.setToIntArray(packet.getOldActiveNameServers()), oldActiveSetStopPacket.toJSONObject());

			// inform self of group change completion
			GroupChangeCompletePacket proposePacket = new GroupChangeCompletePacket(packet.getNewActiveVersion(), packet.getName());
			notifyRC = new MessagingTask(rcID, proposePacket.toJSONObject());

			if (Config.debuggingEnabled)  log.info(" Propose group change complete message for coordination: " + packet.getName()
					+ " Version = " + packet.getNewActiveNameServers());
			/* This task is created to check if replica controllers have updated their database, if not, the request is 
			 * re-proposed for coordination.
			 * 
			 * FIXME: Arun: This seems problematic. What if some replica controllers are down? Shouldn't retransmission by
			 * the "client" initiating this request take care of this? Who is the "client" initiating a group change? 
			 */
			WriteActiveNameServersRunningTask writeTask = new WriteActiveNameServersRunningTask(packet.getName(),
					packet.getNewActiveVersion());
			assert(protocolTasks.length>0);
			protocolTasks[0] = writeTask; 
		} else {
			log.info("Confirmation is received previously or is excessively delayed. " + packet);
		}
		return MessagingTask.toArray(notifyOldActives, notifyRC);
	}

	/**
	 * Updates <code>ReplicaControllerRecord</code> database to indicate group change has completed.
	 * Executes the result of update proposed by <code>handleNewActiveStartConfirmMessage</code>.
	 */
	public static MessagingTask[] executeActiveNameServersRunning(GroupChangeCompletePacket packet,
			BasicRecordMap DB, int rcID, boolean recovery)
					throws JSONException {
		MessagingTask replyToLNS = null;
		if (Config.debuggingEnabled) log.info("Node "+rcID+": new active started; writing to database: " + packet);
		try {
			ReplicaControllerRecord rcRecord = ReplicaControllerRecord.getNameRecordPrimaryMultiField(
					DB, packet.getName(), newActiveStartedFields);

			log.info("Node "+rcID + " group change complete. name = " + rcRecord.getName() + " Version "
					+ rcRecord.getActiveVersion());
			if (rcRecord.setNewActiveRunning(packet.getVersion())) {
				if (Config.debuggingEnabled) log.info("Node "+rcID+" new active running: " + packet.getName() + " : " + 
						packet.getVersion());
			} else {
				log.info("Node "+rcID+" IGNORE MSG: NEW Active Version NOT FOUND while setting it to inactive. " +
						"Already received msg before. Version = " + packet.getVersion());
			}

			if(recovery) return null; 
			
			if (TESTTrackGroupChange.size() > 0) {
				GroupChangeIdentifier gci = new GroupChangeIdentifier(packet.getName(), packet.getVersion());
				Integer lnsID = TESTTrackGroupChange.remove(gci);
				log.info("Node "+rcID+" after group change: Send confirmation to LNS  " + lnsID);
				if (lnsID != null) {
					log.info("Node "+rcID+ " after group change: Send confirmation to LNS  " + lnsID);
					replyToLNS = new MessagingTask(lnsID, packet.toJSONObject());
				}
			}
		} catch (RecordNotFoundException e) {
			log.severe("Record does not exist !! Should not happen. " + packet.getName());
			e.printStackTrace();
		} catch (FieldNotFoundException e) {
			log.severe("Field not found exception. " + e.getMessage() + "\tName\t" + packet.getName());
			e.printStackTrace();
		} catch (FailedDBOperationException e) {
			log.severe("Failed update exception. " + e.getMessage() + "\tName\t" + packet.getName());
			e.printStackTrace();
		}
		return replyToLNS.toArray();
	}
}


