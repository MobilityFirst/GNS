package edu.umass.cs.gns.reconfigurator;

import edu.umass.cs.gns.exceptions.FailedDBOperationException;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.MessagingTask;
import edu.umass.cs.gns.nio.NIOTransport;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.packet.RequestActivesPacket;
import edu.umass.cs.gns.nsdesign.recordmap.BasicRecordMap;
import edu.umass.cs.gns.nsdesign.recordmap.ReplicaControllerRecord;
import org.json.JSONException;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * @author V. Arun
 * Based on code created by abhigyan on 2/27/14.
 * 
 * A replica controller will execute this code on a local name server's request to lookup
 * current set of active replicas for a name.
 * <p>
 * This class contains static methods that will be called by ReplicaController.
 * <p>
 * 
 */
public class LookupActives {

	private static Logger log = NIOTransport.LOCAL_LOGGER ? Logger.getLogger(Add.class.getName()) : GNS.getLogger();

	public static MessagingTask[] executeLookupActives(RequestActivesPacket packet, BasicRecordMap DB, int rcID, boolean recovery)
          throws JSONException, IOException, FailedDBOperationException {

		if (recovery || packet.getNsID() != rcID) return null;
		MessagingTask replyToLNS = null;
		boolean isError = false;

		if(Config.debuggingEnabled) log.fine("Received Request Active Packet Name = " + packet.getName());
		try {
			ReplicaControllerRecord rcRecord = ReplicaControllerRecord.getNameRecordPrimaryMultiField(
					DB, packet.getName(), ReplicaControllerRecord.MARKED_FOR_REMOVAL,
					ReplicaControllerRecord.ACTIVE_NAMESERVERS);
			if (rcRecord.isMarkedForRemoval()) {
				isError = true;
			} else { // send reply to LNS
				packet.setActiveNameServers(rcRecord.getActiveNameservers());
				replyToLNS = new MessagingTask(packet.getLNSID(), packet.toJSONObject());
				if(Config.debuggingEnabled) log.fine("Sent actives for " + packet.getName() + " = " + rcRecord.getActiveNameservers());
			}
		} catch (RecordNotFoundException e) {
			isError = true;
		} catch (FieldNotFoundException e) {
			log.severe("Field not found exception. " + e.getMessage());
			e.printStackTrace();
		} finally {
			if(isError) { // may be set within catch block above or if record isMarkedForRemoval()
				packet.setActiveNameServers(null);
				replyToLNS = new MessagingTask(packet.getLNSID(), packet.toJSONObject());
				log.info("Error: Record does not exist for " + packet.getName());
			}
		}
		
		return replyToLNS.toArray();
	}
}
