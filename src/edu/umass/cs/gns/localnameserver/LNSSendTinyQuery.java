package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.packet.TinyQuery;
import edu.umass.cs.gns.util.ConfigFileInfo;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.Random;
import java.util.TimerTask;

public class LNSSendTinyQuery {
	public static Random r= new Random();
	
	public static void sendQuery(String name,int count) {
		// increment lookup request;
		LocalNameServer.incrementLookupRequest(name);

		// select name server.
		int nameServer = getNameServerID(name);
		if (StartLocalNameServer.debugMode) GNS.getLogger().fine("TINYQUERY NSFOUND " + name + " count " + count + " name server " + nameServer);
		
		// add to query info.
		logIncomingRequest(name, nameServer, System.currentTimeMillis(), count);

		// create packet
		TinyQuery query = new TinyQuery(count, LocalNameServer.nodeID, 
				name, new HashSet<Integer>(), new HashSet<Integer>());

		// send packet
		if (StartLocalNameServer.delayScheduling) {
			double latency = ConfigFileInfo.getPingLatency(nameServer) * 
					( 1 + r.nextDouble() * StartLocalNameServer.variation);
			long timerDelay = (long) latency;
			try {
				LocalNameServer.timer.schedule(new SendQueryWithDelay(query.toJSONObject(), nameServer), timerDelay);
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else {
			
			try {
				LNSListener.udpTransport.sendPacket(query.toJSONObject(), nameServer, GNS.PortType.UPDATE_PORT);
				if (StartLocalNameServer.debugMode) GNS.getLogger().fine("TINYQUERY SEND " + name + " count " + count + "\t");
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
	private static int getNameServerID(String name) {
		NameRecordKey nameRecordKey = NameRecordKey.EdgeRecord;
		int nameServerID = -1;
//		String queryStatus = null;

		if (LocalNameServer.isValidNameserverInCache(name)) {
			// Active name server information available in cache.s
			// Send a lookup query to the closest active name server.
			if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Name: " + name 
                                //+ " / " + nameRecordKey.getName()
                                + " Address invalid in cache" + "TimeAddress:" + LocalNameServer.timeSinceAddressCached(name, nameRecordKey) + "ms");
			if (StartLocalNameServer.beehiveReplication && StartLocalNameServer.loadDependentRedirection) {
				nameServerID = LocalNameServer.getLoadAwareBeehiveNameServerFromCache(name, new HashSet<Integer>());
			}
			else if (StartLocalNameServer.beehiveReplication) {
				nameServerID = LocalNameServer.getBeehiveNameServerFromCache(name, new HashSet<Integer>());
			} else if (StartLocalNameServer.loadDependentRedirection) {
				nameServerID = LocalNameServer.getBestActiveNameServerFromCache(name, new HashSet<Integer>());
			} else {
				nameServerID = LocalNameServer.getClosestActiveNameServerFromCache(name, new HashSet<Integer>());
			}
			if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Contacting closest active. Name:" + name + " / " + nameRecordKey.getName() + " ClosestNS:" + nameServerID);
		} else {
			//Send a request to the closest primary name server.
			if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Name: " + name 
                                //+ " / " + nameRecordKey.getName() 
                                + " Both address and NS invalid. " + "TimeAddress:" + LocalNameServer.timeSinceAddressCached(name, nameRecordKey) + "ms");
			if (StartLocalNameServer.beehiveReplication) {
				nameServerID = LocalNameServer.getBeehivePrimaryNameServer(name, new HashSet<Integer>());
			} else if (StartLocalNameServer.loadDependentRedirection) {
				nameServerID = LocalNameServer.getBestPrimaryNameServer(name, new HashSet<Integer>());
			} else {
				nameServerID = LocalNameServer.getClosestPrimaryNameServer(name, new HashSet<Integer>());
			}
//			queryStatus = QueryInfo.CONTACT_PRIMARY;
			//				LocalNameServer.appendQueryInfoStatus(queryId, QueryInfo.CONTACT_PRIMARY);
			if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Contacting closest primary. Name Server: " + nameServerID);
		}

		//No name server available to query. Ignore this query.
		if (nameServerID == -1) {
			if (StartLocalNameServer.debugMode) GNS.getLogger().fine("LNSListenerQuery: NO NAME SERVER FOR " + name + " / " + nameRecordKey.getName());
			//      errorResponse(incomingPacket, RecordType.RCODE_ERROR, senderAddress, senderPort);

			return -1;
		}
		return nameServerID;

	}

	private static void logIncomingRequest(String name, int nameServerID, long recvdTime, int count) {
		double ping = ConfigFileInfo.getPingLatency(nameServerID); 
		String log = "\tLookup-Query\t" + count + "\t" + name  + "\t" + LocalNameServer.nodeID 
				+ "\t" + nameServerID + "\t" + ping + "\t" + recvdTime + "\t"; 
		GNS.getStatLogger().fine(log);
	}
	
	private static int addToQueryInfo(String name, int nameServerID, long recvdTime, int count) {
		
 		int queryId = LocalNameServer.addQueryInfo(name, NameRecordKey.EdgeRecord, 
				nameServerID, recvdTime, "send-tiny-query", count, null, null, -1);
		return queryId;

	}
}


class SendQueryWithDelay extends TimerTask {
	JSONObject json;
	int nameserver;
	public SendQueryWithDelay(JSONObject json, int nameserver) {
		this.json = json;
		this.nameserver = nameserver;
	}
	@Override
	public void run() {
		// send packet
		try {
			LNSListener.udpTransport.sendPacket(json, nameserver, GNS.PortType.UPDATE_PORT);
//			if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("TINYQUERY SEND " + name + " count " + count + "\t");
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
}
