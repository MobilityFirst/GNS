package edu.umass.cs.gns.localnameserver.original;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.ReplicationFrameworkType;
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
    try {
      LocalNameServer.sendToNS(query.toJSONObject(),nameServer);
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
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
			if (StartLocalNameServer.replicationFramework == ReplicationFrameworkType.BEEHIVE && StartLocalNameServer.loadDependentRedirection) {
				nameServerID = LocalNameServer.getLoadAwareBeehiveNameServerFromCache(name, new HashSet<Integer>());
			}
			else if (StartLocalNameServer.replicationFramework == ReplicationFrameworkType.BEEHIVE) {
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
			if (StartLocalNameServer.replicationFramework == ReplicationFrameworkType.BEEHIVE) {
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
		
 		int queryId = LocalNameServer.addDNSRequestInfo(name, NameRecordKey.EdgeRecord, 
				nameServerID, recvdTime, "send-tiny-query", count, null, null, -1, 0);
		return queryId;
	}
}


/**
 * When we emulate ping latencies between LNS and NS, this task will actually send packets to NS.
 * See option StartLocalNameServer.emulatePingLatencies
 */
class SendQueryWithDelay extends TimerTask {
  /**
   * Json object to send
   */
	JSONObject json;
  /**
   * Name server to send this packet to.
   */
	int nameServer;
	public SendQueryWithDelay(JSONObject json, int nameServer) {
		this.json = json;
		this.nameServer = nameServer;
	}

	@Override
	public void run() {
    LocalNameServer.sendToNSActual(json, nameServer);
  }
	
	
}
