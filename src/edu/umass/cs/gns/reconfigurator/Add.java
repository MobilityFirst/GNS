package edu.umass.cs.gns.reconfigurator;

import edu.umass.cs.gns.exceptions.FailedDBOperationException;
import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.MessagingTask;
import edu.umass.cs.gns.nio.NIOTransport;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.packet.AddRecordPacket;
import edu.umass.cs.gns.nsdesign.packet.ConfirmUpdatePacket;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.nsdesign.recordmap.BasicRecordMap;
import edu.umass.cs.gns.nsdesign.recordmap.ReplicaControllerRecord;
import edu.umass.cs.gns.util.NSResponseCode;
import org.json.JSONException;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * @author V. Arun
 * Based on code created by abhigyan on 2/27/14.
 * 
 * Includes code which a replica controller executes to add a name to GNS. If name servers are replicated,
 * then methods in this class will be executed after  the coordination among replica controllers at name servers
 * is complete.
 * <p/>
 * This class contains static methods that will be called by ReplicaController.
 * <p/>
 * An add request is first received by a replica controller, but it also needs to be executed at active replicas.
 * Initially, active replicas are co-located with replica controllers therefore replica controller forwards an add
 * request to the GnsReconfigurable at the same node.
 * <p/>
 * <p/>
 * 
 */
public class Add {

	private static Logger log = NIOTransport.LOCAL_LOGGER ? Logger.getLogger(Add.class.getName()) : GNS.getLogger();

	/**
	 * Adds a name in the database at this replica controller and forwards the request to the active replica at
	 * the same node. If name exists in database, it will send error message to the client.
	 *
	 * @param addRecordPacket   Packet sent by client.
	 * @param replicaController ReplicaController object calling this method.
	 */
	public static MessagingTask[] executeAddRecord(AddRecordPacket addRecordPacket, BasicRecordMap DB, int rcID,
			boolean recovery) throws JSONException, FailedDBOperationException, IOException {

		MessagingTask activeMTask=null, LNSMTask=null;  // send add at active, confirmation or error to LNS
		boolean addedAtReplicaController= false;

		if (Config.debuggingEnabled) log.fine("Executing ADD at replica controller " + addRecordPacket +
				" Local name server address = " + addRecordPacket.getLnsAddress());
		
		// FIXME: earlier version removed record if recovery. Unclear why we shouldn't just try and fail.
		//if (recovery) ReplicaControllerRecord.removeNameRecordPrimary(replicaController.getDB(), addRecordPacket.getName());

		// add at local replica controller's database
		try {
			ReplicaControllerRecord rcRecord = new ReplicaControllerRecord(DB, addRecordPacket.getName(), true);
			ReplicaControllerRecord.addNameRecordPrimary(DB, rcRecord); // only line that can throw exceptions
			addedAtReplicaController = true;
		} catch (FailedDBOperationException e) {
			// send ERROR to LNS
			ConfirmUpdatePacket confirmPkt = new ConfirmUpdatePacket(NSResponseCode.ERROR, addRecordPacket);
			if (colocatedActiveAndReplicaController(addRecordPacket, rcID)) {
				LNSMTask = new MessagingTask(addRecordPacket.getLnsAddress(), confirmPkt.toJSONObject());
			}
		} catch (RecordExistsException e) {
			if (addRecordPacket.getNameServerID() == rcID) {
				// send ERROR to LNS
				ConfirmUpdatePacket confirmPkt = new ConfirmUpdatePacket(NSResponseCode.ERROR, addRecordPacket);
				if (Config.debuggingEnabled) log.fine("Record exists. sending failure: name = " + addRecordPacket.getName() + 
						" Local name server address = " + addRecordPacket.getLnsAddress() + "Response code: " + confirmPkt);
				LNSMTask = new MessagingTask(addRecordPacket.getLnsAddress(), confirmPkt.toJSONObject());
			}
		}

		// create messages for active replica and LNS
		if(addedAtReplicaController) {
			// send ACTIVE_ADD to active replica
			addRecordPacket.setType(Packet.PacketType.ACTIVE_ADD); // change packet type
			activeMTask = new MessagingTask(rcID, addRecordPacket.toJSONObject());

			// send NO_ERROR confirmation to LNS
			if (colocatedActiveAndReplicaController(addRecordPacket, rcID)) { // FIXME: Why is this check needed?
				ConfirmUpdatePacket confirmPkt = new ConfirmUpdatePacket(NSResponseCode.NO_ERROR, addRecordPacket);
				if (Config.debuggingEnabled) log.fine("Add complete informing client. " + addRecordPacket + " LocalNameServer address = " +
						addRecordPacket.getLnsAddress() + "Response code: " + confirmPkt);
				LNSMTask = new MessagingTask(addRecordPacket.getLnsAddress(), confirmPkt.toJSONObject());
			}
		}
		
		return !recovery ? MessagingTask.toArray(activeMTask, LNSMTask) : null; // no messaging during recovery
	}
	// FIXME: separate method because this should not matter and should always return false
	private static boolean colocatedActiveAndReplicaController(AddRecordPacket addPacket, int rcID) {
		return addPacket.getNameServerID() == rcID;
	}
	
	/**
	 * Method is called after GnsReconfigurable confirms it has added the record. This methods sends confirmation
	 * to local name server that record is added.
	 */
	public static MessagingTask[] executeAddActiveConfirm(AddRecordPacket addRecordPacket)
					throws JSONException {
		// no action needed.
		return null;
	}
}
