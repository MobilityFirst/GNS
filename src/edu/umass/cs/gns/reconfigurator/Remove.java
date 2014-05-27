/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved
 */
package edu.umass.cs.gns.reconfigurator;

import edu.umass.cs.gns.database.ColumnField;
import edu.umass.cs.gns.exceptions.FailedUpdateException;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.MessagingTask;
import edu.umass.cs.gns.nio.NIOTransport;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.packet.ConfirmUpdatePacket;
import edu.umass.cs.gns.nsdesign.packet.OldActiveSetStopPacket;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.nsdesign.packet.RemoveRecordPacket;
import edu.umass.cs.gns.nsdesign.recordmap.BasicRecordMap;
import edu.umass.cs.gns.nsdesign.recordmap.ReplicaControllerRecord;
import edu.umass.cs.gns.util.NSResponseCode;
import edu.umass.cs.gns.util.UniqueIDHashMap;

import org.json.JSONException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Logger;

/**
 * @author V. Arun
 * Based on code created by abhigyan on 2/27/14.
 * 
 * A replica controller executes these methods to remove a name from GNS. If replica controllers are replicated
 * and use coordination, then methods in this class will be executed after the coordination among them is complete.
 * <p>
 * Like an add request, remove request is first received by a replica controller, but also needs to be executed at
 * active replicas. A replica controller makes two database operations to remove a record. First operation updates
 * record to say the name is going to be removed. After this, ReplicaController waits from confirmation from active
 * replicas of that name that they have removed the record. Second operation then finally removes the record at replica
 * controllers.
 * <p>
 * We need two operations to remove a record because the set of active replicas for a name can change.
 * After first operation is over, we do not allow any changes to the set of active replicas for a name. For the
 * remove request to complete, the current set of active replicas after the first operation must remove the record
 * from database. Replica controllers finally remove the record after the current active replicas have removed the record.
 * <p>
 */
public class Remove {

	private static Logger log = NIOTransport.LOCAL_LOGGER ? Logger.getLogger(Add.class.getName()) : GNS.getLogger();
	private static ArrayList<ColumnField> applyMarkedForRemovalFields = new ArrayList<ColumnField>();

	static {
		applyMarkedForRemovalFields.add(ReplicaControllerRecord.MARKED_FOR_REMOVAL);
		applyMarkedForRemovalFields.add(ReplicaControllerRecord.ACTIVE_NAMESERVERS_RUNNING);
		applyMarkedForRemovalFields.add(ReplicaControllerRecord.ACTIVE_NAMESERVERS);
		applyMarkedForRemovalFields.add(ReplicaControllerRecord.ACTIVE_VERSION);
	}

	/**
	 * Executes the first phase of remove operation, which updates record to say it is going to be removed.
	 * This method also forwards request to current active replicas to remove the record.
	 * The request is not forwarded in the special case that a group change for this name is in
	 * progress at the same time. In this case, we wait for the group change to complete before proceeding to remove the
	 * record at active replicas.
	 *
	 * @param removeRecord Packet sent by client
	 * @param rc ReplicaController calling this method
	 * @param recovery  true if we are replaying logs during recovery
	 */
	protected static MessagingTask[] executeMarkRecordForRemoval(RemoveRecordPacket removeRecord, BasicRecordMap DB, int rcID,
			boolean recovery, RCProtocolTask[] protocolTasks /*return value*/) throws JSONException, IOException {
		boolean sendError = false;
		MessagingTask replyToLNS = null;
		try {
			ReplicaControllerRecord rcRecord = ReplicaControllerRecord.getNameRecordPrimaryMultiField(DB,
					removeRecord.getName(), applyMarkedForRemovalFields);
			rcRecord.setMarkedForRemoval(); // FIXME: should return boolean value if this can fail
			// check if record marked as removed; it may not be if a group change for this name is in progress.
			if (rcRecord.isMarkedForRemoval() && removeRecord.getNameServerID() == rcID && !recovery) {
				// if marked for removal and this node received packet from client and not recover, inform actives
				if (Config.debugMode) log.info("Node "+rcID+ " marked for removal: " + removeRecord);
				assert rcRecord.isActiveRunning() : "Node "+rcID + " active not running: "+rcRecord;
				assert(protocolTasks.length>0);
				StopActiveSetTask stopActiveSetTask = new StopActiveSetTask(removeRecord.getName(),
						rcRecord.getActiveNameservers(), rcRecord.getActiveVersion(), 
						Packet.PacketType.ACTIVE_REMOVE, removeRecord);
				protocolTasks[0] = stopActiveSetTask; // return value
			} else if(!rcRecord.isMarkedForRemoval()) {
				sendError = true;
				log.info("Remove record not executed because group change for the name is in progress " + removeRecord);
			} else if (removeRecord.getNameServerID() != rcID){
				if (Config.debugMode) log.info("SKIP: remove record request does not not contain node ID " + rcRecord.getName());
			}
		} catch (FailedUpdateException e) {
			sendError = true;
			log.info("Error during update. Sent failure confirmation to client. Name = " + removeRecord.getName());
		} catch (RecordNotFoundException e) {
			sendError = true;
			log.info("Record not found. Sent failure confirmation to client. Name = " + removeRecord.getName());
		} catch (FieldNotFoundException e) {
			if (Config.debugMode) log.severe("Field Not Found Exception. " + e.getMessage());
			e.printStackTrace();
		} finally {
			if (sendError && removeRecord.getNameServerID() == rcID) {
				ConfirmUpdatePacket failPacket = new ConfirmUpdatePacket(NSResponseCode.ERROR, removeRecord);
				replyToLNS = new MessagingTask(removeRecord.getLocalNameServerID(), failPacket.toJSONObject());
			}
		}
		return !recovery ? replyToLNS.toArray() : null;
	}

	/**
	 * Actives have removed the record, so remove the requestID from the list of ongoing stop active requests.
	 */
	protected static MessagingTask handleActiveRemoveRecord(OldActiveSetStopPacket activeStop, int rcID, UniqueIDHashMap map, 
			boolean recovery) throws JSONException, IOException {
		if(recovery) return null;
		MessagingTask removeFromSelf = null;
		if (Config.debugMode) log.fine("RC handling active remove record ... " + activeStop);
		// response received for active stop request, so remove from set, which will cancel the OldActiveSetStopPacket task
		RemoveRecordPacket removePacket = (RemoveRecordPacket) map.remove(activeStop.getRequestID());
		if (Config.debugMode) log.fine("RC remove packet fetched ... " + removePacket);
		if (removePacket != null) { // response has not been already received
			removePacket.changePacketTypeToRcRemove();
			removeFromSelf = new MessagingTask(rcID, removePacket.toJSONObject());
		} else {
			log.info("Duplicate or delayed response for old active stop: " + activeStop);
		}
		return removeFromSelf;
	}

	/**
	 * Executes second phase of remove operation at replica controllers, which finally removes the record from database
	 * and sends confirmation to local name server.
	 * This method is executed after active replicas have stopped and removed the record.
	 *
	 * @param removeRecordPacket Packet sent by client
	 * @param rc ReplicaController calling this method
	 *
	 */
	protected static MessagingTask[] executeRemoveRecord(RemoveRecordPacket removeRecordPacket, BasicRecordMap DB, int rcID,
			boolean recovery) throws JSONException, FailedUpdateException, IOException {
		if (Config.debugMode) log.fine("DECISION executing remove record at RC: " + removeRecordPacket);
		DB.removeNameRecord(removeRecordPacket.getName());

		if(recovery) return null;
		// else send NO_ERROR confirmation to LNS
		MessagingTask replyToLNS = null;
		if (removeRecordPacket.getNameServerID() == rcID) { // this will be true at the replica controller who
			// first received the client's request
			ConfirmUpdatePacket confirmPacket = new ConfirmUpdatePacket(NSResponseCode.NO_ERROR, removeRecordPacket);
			replyToLNS = new MessagingTask(removeRecordPacket.getLocalNameServerID(), confirmPacket.toJSONObject());
			if (Config.debugMode) log.fine("Remove record response sent to LNS: " + removeRecordPacket.getName() + " lns "
					+ removeRecordPacket.getLocalNameServerID());
		}
		return replyToLNS.toArray();
	}
}
