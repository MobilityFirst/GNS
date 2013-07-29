package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.main.StartLocalNameServer;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.packet.TinyQuery;

public class LNSRecvTinyQuery {

	public static void recvdQueryResponse(TinyQuery tinyQuery) {
		int queryID = tinyQuery.getQueryID();
		
		if (StartLocalNameServer.debugMode) GNS.getLogger().fine("TINYQUERY LNSRECVD queryid " + tinyQuery.getQueryID());
		QueryInfo queryInfo = LocalNameServer.removeQueryInfo(queryID);
		if (queryInfo != null) {
			// Log response
			queryInfo.setRecvTime(System.currentTimeMillis());
      String stats = queryInfo.getLookupStats();
      GNS.getStatLogger().info("Success-LookupRequest\t" + stats);
      
      return;
		}
		else {
			GNS.getStatLogger().fine("Failed-Lookup");
		}
	}
	
	public static void logQueryResponse(JSONObject json) throws JSONException {
		TinyQuery tinyQuery = new TinyQuery(json);
		updateCacheBasedOnTinyQueryResponse(tinyQuery);
		String log = "\tLookup-Response\t" + tinyQuery.getQueryID() + "\t" + tinyQuery.getName()  
				+ "\t" + LocalNameServer.nodeID + "\t-1\t-1\t" + System.currentTimeMillis() + "\t";
		
		GNS.getStatLogger().fine(log);
	}
	
	public static void updateCacheBasedOnTinyQueryResponse(TinyQuery tinyQuery) {
		if (LocalNameServer.containsCacheEntry(tinyQuery.getName())) {
      LocalNameServer.updateCacheEntry(tinyQuery);
      if (StartLocalNameServer.debugMode) GNS.getLogger().fine("LNSListenerResponse: Updating cache QueryID:" + tinyQuery.getQueryID());
    } else {
      LocalNameServer.addCacheEntry(tinyQuery);
      if (StartLocalNameServer.debugMode) GNS.getLogger().fine("LNSListenerResponse: Adding to cache QueryID:" + tinyQuery.getQueryID());
    }
		
	}
}
