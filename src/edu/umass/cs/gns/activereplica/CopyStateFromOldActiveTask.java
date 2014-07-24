package edu.umass.cs.gns.activereplica;

import edu.umass.cs.gns.exceptions.CancelExecutorTaskException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.MessagingTask;
import edu.umass.cs.gns.nio.NIOTransport;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.TransferableNameRecordState;
import edu.umass.cs.gns.nsdesign.packet.NewActiveSetStartupPacket;
import edu.umass.cs.gns.reconfigurator.Add;
import edu.umass.cs.gns.util.NameRecordKey;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.util.ValuesMap;
import org.json.JSONException;

import java.util.HashSet;
import java.util.logging.Logger;

/**
 * @author V. Arun
 * Based on code created by abhigyan on 3/28/14.
 */
public class CopyStateFromOldActiveTask implements ARProtocolTask {

	private ActiveReplica<?> activeReplica;

	private final NewActiveSetStartupPacket packet;

	private final HashSet<Integer> oldActivesQueried;

	private final int requestID;
	
	private static Logger log = NIOTransport.LOCAL_LOGGER ? Logger.getLogger(Add.class.getName()) : GNS.getLogger();

	protected CopyStateFromOldActiveTask(NewActiveSetStartupPacket packet) throws JSONException {
		this.oldActivesQueried = new HashSet<Integer>();
		// first, store the original packet in hash map
		this.requestID = packet.hashCode();
		activeReplica.getOngoingStateTransferRequests().put(requestID, packet);
		// next, create a copy as which we will modify
		this.packet = new NewActiveSetStartupPacket(packet.toJSONObject());
		this.packet.setUniqueID(this.requestID);  // ID assigned by this active replica.
		this.packet.changePacketTypeToPreviousValueRequest();
		this.packet.changeSendingActive(activeReplica.getNodeID());
	}
	
	public void setActiveReplica(ActiveReplica<?> ar) {
		this.activeReplica = ar;
	}

	protected static void makeFakeResponse(NewActiveSetStartupPacket packet) {
		packet.changePreviousValueCorrect(true);
		ValuesMap valuesMap = new ValuesMap();
		ResultValue rv = new ResultValue();
		rv.add("pqrst");
		valuesMap.putAsArray(NameRecordKey.EdgeRecord.getName(), rv);
		packet.changePacketTypeToPreviousValueResponse();
		packet.changePreviousValue(new TransferableNameRecordState(valuesMap, 0).toString());
	}

	private void run_alt() {
		makeFakeResponse(packet);
		try {
			activeReplica.send(new MessagingTask(activeReplica.getNodeID(), packet.toJSONObject()));
			throw new CancelExecutorTaskException();
		} catch (JSONException e) {
			e.printStackTrace();
		} catch (CancelExecutorTaskException e) {
			throw new RuntimeException();
		}
	}

	public void run() {
		if (Config.dummyGNS) {
			run_alt();
			return;
		}
		try {

			if (Config.debugMode) log.info(" NEW_ACTIVE_START_FORWARD received packet: " + packet.toJSONObject());

			if (activeReplica.getOngoingStateTransferRequests().get(requestID) == null) {
				log.info(" COPY State from Old Active Successful! Cancel Task; Actives Queried: " + oldActivesQueried);
				throw new CancelExecutorTaskException();
			}

			// select old active to send request to
			int oldActive = activeReplica.getGnsNodeConfig().getClosestServer(packet.getOldActiveNameServers(),
					oldActivesQueried);

			if (oldActive == -1) {
				// this will happen after all actives have been tried at least once.
				log.severe(" Exception ERROR:  No More Actives Left To Query. Cancel Task!!! " + packet + " Actives queried: " + oldActivesQueried);
				activeReplica.getOngoingStateTransferRequests().remove(requestID);
				throw new CancelExecutorTaskException();
			}
			oldActivesQueried.add(oldActive);
			if (Config.debugMode) log.info(" OLD ACTIVE SELECTED = : " + oldActive);

			try {
				activeReplica.send(new MessagingTask(oldActive, packet.toJSONObject()));
			} catch (JSONException e) {
				log.severe(" JSONException here: " + e.getMessage());
				e.printStackTrace();
			}
			if (Config.debugMode) log.info(" REQUESTED VALUE from OLD ACTIVE. PACKET: " + packet);

		} catch (JSONException e) {
			e.printStackTrace();
		} catch (Exception e) {
			if (e.getClass().equals(CancelExecutorTaskException.class)) {
				throw new RuntimeException(); // this is only way to cancel this task as it is run via ExecutorService
			}
			// other types of exception are not expected. so, log them.
			log.severe("Exception in Copy State from old actives task. " + e.getMessage());
			e.printStackTrace();
			throw new RuntimeException();
		}
	}
}