package edu.umass.cs.gns.localnameserver;

import java.util.HashSet;
import java.util.TimerTask;

import edu.umass.cs.gns.main.StartLocalNameServer;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.packet.RequestActivesPacket;

public class SendActivesRequestTask extends TimerTask
{

	String name;
	//NameRecordKey recordKey;
	HashSet<Integer> nameServersQueried;
	int MAX_ATTEMPTS = GNS.numPrimaryReplicas;

	public SendActivesRequestTask(String name//, NameRecordKey recordKey
                ) {
		this.name = name;
		//this.recordKey = recordKey;
		nameServersQueried = new HashSet<Integer>();
	}


	@Override
	public void run()
	{
		// check whether actives Received
		if (LocalNameServer.isValidNameserverInCache(name//, recordKey
                        )) {
			this.cancel();
			return;
		}
		// All primaries have been queried
		if (nameServersQueried.size() == GNS.numPrimaryReplicas) {
			// 
			this.cancel();
			return;
		}
		// next primary to be queried
		int primaryID = LocalNameServer.getClosestPrimaryNameServer(name, //recordKey, 
                        nameServersQueried);
		if (primaryID == -1) {
			this.cancel();
			return;
		}
		nameServersQueried.add(primaryID);
		// send packet to primary
		sendActivesRequestPacketToPrimary(name, //recordKey, 
                        primaryID);
	}


	/**
	 * Create task to request actives from primaries.
	 * @param name
	 */
	public static void requestActives(String name) {
		SendActivesRequestTask task = new SendActivesRequestTask(name);
		LocalNameServer.timer.schedule(task, 0, GNS.DEFAULT_QUERY_TIMEOUT);
	}

	/**
	 * send request to primary to send actives
	 * @param name
	 * @param primaryID
	 */
	private static void sendActivesRequestPacketToPrimary(String name, //NameRecordKey recordKey,
                int primaryID) {
		RequestActivesPacket packet = new RequestActivesPacket(name, //recordKey,
                        LocalNameServer.nodeID);
		try
		{
			LNSListener.udpTransport.sendPacket(packet.toJSONObject(), primaryID, GNS.PortType.UPDATE_PORT);
			if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Send Active Request Packet to Primary. " + primaryID);
		} catch (JSONException e)
		{
			if (StartLocalNameServer.debugMode) GNS.getLogger().fine("JSON Exception in sending packet");
			e.printStackTrace();
		}

	}

	/**
	 * Recvd reply from primary with current actives, update the cache.
	 * @param json
	 * @throws JSONException 
	 */
	public static void handleActivesRequestReply(JSONObject json) throws JSONException {
		RequestActivesPacket requestActivesPacket = new RequestActivesPacket(json);
		if (StartLocalNameServer.debugMode) GNS.getLogger().fine("RECVD request packet: " + requestActivesPacket);
        if (requestActivesPacket.getActiveNameServers() == null ||
                requestActivesPacket.getActiveNameServers().size() == 0) {
            PendingTasks.sendErrorMsgForName(requestActivesPacket.getName()//,requestActivesPacket.getRecordKey()
                    );
            return;
        }

        if (LocalNameServer.containsCacheEntry(requestActivesPacket.getName()//, requestActivesPacket.getRecordKey()
                )) {
			LocalNameServer.updateCacheEntry(requestActivesPacket);
			if (StartLocalNameServer.debugMode) GNS.getLogger().fine("LNSListenerResponse: Updating cache Name:" +
                    requestActivesPacket.getName()
					+ " Actives: " + requestActivesPacket.getActiveNameServers());
		} else {
			LocalNameServer.addCacheEntry(requestActivesPacket);
			if (StartLocalNameServer.debugMode) GNS.getLogger().fine("LNSListenerResponse: Adding to cache Name:" +
                    requestActivesPacket.getName()+ " Actives: " + requestActivesPacket.getActiveNameServers());
		}
		
		PendingTasks.runPendingRequestsForName(
				requestActivesPacket.getName()//, requestActivesPacket.getRecordKey()
                        );
		
	}


}

