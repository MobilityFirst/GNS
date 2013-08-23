package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.packet.*;
import edu.umass.cs.gns.statusdisplay.StatusClient;
import edu.umass.cs.gns.util.AdaptiveRetransmission;
import edu.umass.cs.gns.util.ConfigFileInfo;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.Random;
import java.util.TimerTask;
import java.util.logging.Level;

public class LNSQueryTask extends TimerTask {

  DNSPacket incomingPacket;
  InetAddress senderAddress;
  int senderPort;
  long receivedTime; // overall latency
  private int transmissionCount;
  private int lookupNumber;
  private int queryId;
  private HashSet<Integer> nameserversQueried;
  int latestNameServerQueried;
//  long totalTimerWait;
//	private long retransmission_timeout = StartLocalNameServer.queryTimeout;
  //private long maxQueryWaitTime = StartLocalNameServer.maxQueryWaitTime; // 100 seconds
  private static final boolean DISABLECACHE = false; // for testing - Westy

  LNSQueryTask(DNSPacket incomingPacket, int transmissionCount,
          InetAddress senderAddress, int senderPort, long receivedTime,
          int lookupNumber, int queryId,
          HashSet<Integer> nameserversQueried) {
    this.incomingPacket = incomingPacket;
    this.transmissionCount = transmissionCount;
    this.senderAddress = senderAddress;
    this.senderPort = senderPort;
    this.receivedTime = receivedTime;
    this.lookupNumber = lookupNumber;
    this.queryId = queryId;
    this.nameserversQueried = nameserversQueried;
    latestNameServerQueried = -1;
//    this.totalTimerWait = totalTimerWait;
  }

  @Override
  public void run() {
    // if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("Query-" + lookupNumber + "\t" + System.currentTimeMillis() + "\t"  + incomingPacket.qname + "\tTimer-running");
    // new DNS query?
    if (transmissionCount <= 1) {
        transmissionCount = 1;
      if (!checkIncomingPacketOkay()) {
        if (StartLocalNameServer.debugMode) GNS.getLogger().info("Incoming dns packet not okay. Packet:" + incomingPacket.toString());
        return;
      }
//			if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("Query-" + lookupNumber + "\t" + System.currentTimeMillis() + "\t"
//	        + incomingPacket.qname + "\tPacket-okay");
//			if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("1. Packet Okay." + incomingPacket.qrecordKey + " " + incomingPacket.qname);
      newDNSRequestBookkeeping();
//			if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("Query-" + lookupNumber + "\t" + System.currentTimeMillis() + "\t"
//	        + incomingPacket.qname + "\tDNS-bookkeeping-done");
//			if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("1.1 Bookkeeping Done." + incomingPacket.qrecordKey + " " + incomingPacket.qname);
      // if address already in cache?
      if (!DISABLECACHE) {
          CacheEntry cacheEntry = LocalNameServer.getCacheEntry(incomingPacket.getQname());
          if (cacheEntry != null) {
              QueryResultValue value = cacheEntry.getValue(incomingPacket.getQrecordKey());
              if (value != null) {
                  loggingForAddressInCache();
                  sendReplyToUser(value, cacheEntry.getTTL());
                  return;
              }
          }
      } else {
        if (StartLocalNameServer.debugMode) GNS.getLogger().warning("!!!! CACHE IS DISABLED !!!!");
      }
    }
    else  if (LocalNameServer.getQueryInfo(queryId) == null) {
    	if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Query ID does not exist. Removed due to Invalid Active NS. Query ID = " + queryId);
    	return;
    }
    
    // received DNS response for query ? 
    if (transmissionCount > 1 && LocalNameServer.hasDataToProceed(queryId)) {
      QueryInfo query = LocalNameServer.removeQueryInfo(queryId);
      if (query != null) {
        query.setRecvTime(System.currentTimeMillis());
        String stats = query.getLookupStats();
        GNS.getStatLogger().info("Success-LookupRequest\t" + stats);
        if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Query-" + lookupNumber + "\t" + System.currentTimeMillis() + "\t"
                + incomingPacket.getQname() + "\tEnd-of-processing");
        return;
      }
    }
    
    if (!DISABLECACHE) {
      // for retransmissions: is address already in cache?
        CacheEntry entry = LocalNameServer.getCacheEntry(incomingPacket.getQname());
        if (entry != null) {
            QueryResultValue value = entry.getValue(incomingPacket.getQrecordKey());
            if (value != null) {
                QueryInfo query = LocalNameServer.removeQueryInfo(queryId);
                if (query != null) {
                    query.setRecvTime(System.currentTimeMillis());
                    String stats = query.getLookupStats();
                    GNS.getStatLogger().info(stats);
                } else {
                    loggingForAddressInCache();

                }
                sendReplyToUser(value,entry.getTTL());
            }

        }
    } else {
      if (StartLocalNameServer.debugMode) GNS.getLogger().warning("!!!! CACHE IS DISABLED !!!!");
    }

    // exceeded numberOfRetransmissions ?  reply Failure to user.
    if (System.currentTimeMillis() - receivedTime > StartLocalNameServer.maxQueryWaitTime) {

      if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Transmissions exceeded. " + incomingPacket.getQrecordKey() + " " + incomingPacket.getQname());
      errorResponse(incomingPacket, DNSRecordType.RCODE_ERROR, senderAddress, senderPort);
      QueryInfo query = LocalNameServer.removeQueryInfo(queryId);

      logFailureMessage(query);
      if (query != null) {
        if (StartLocalNameServer.debugMode) GNS.getLogger().finer("3.2.1 Query Info Removed." + query.id);
      } else {
        if (StartLocalNameServer.debugMode) GNS.getLogger().fine("3.2.2 Query Info Does not exist.");
      }

      return;
    }

    if (transmissionCount < StartLocalNameServer.numberOfTransmissions) {

      int result = sendDNSLookupToNameServer();
      if (result == 2) {
        if (StartLocalNameServer.debugMode) GNS.getLogger().fine("New actives request sent. Name = " + incomingPacket.getQname());
        // 2 = new actives requested, cancel this query
        return;
      }
      if (result == 1) { // no name servers remaining
        if (StartLocalNameServer.debugMode) GNS.getLogger().fine("No name servers remaining. Previous NS Queried = " + nameserversQueried + " Trying again .. Name = " + incomingPacket.getQname());
        // clear name servers and try again
        nameserversQueried.clear();
        result = sendDNSLookupToNameServer();
        if (result == 2) {
          if (StartLocalNameServer.debugMode) GNS.getLogger().fine("New actives request sent. Name = " + incomingPacket.getQname());
          // 2 = new actives requested, cancel this query
          return;
        }
        if (result == 1) { // still no name servers
          if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Again no name servers remaining. Quit sending request. Name = " + incomingPacket.getQname());
          transmissionCount = StartLocalNameServer.numberOfTransmissions;
        }
      }
//		if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("Query-" + lookupNumber + "\t" + System.currentTimeMillis() + "\t"
//        + incomingPacket.qname + "\tDNS-lookup-scheduled");
    }
    GNS.getLogger().fine(" HECHED ...");
    long retransmissionTimeout;
    if (StartLocalNameServer.adaptiveTimeout) {
      retransmissionTimeout = AdaptiveRetransmission.getTimeoutInterval(latestNameServerQueried);
      if (StartLocalNameServer.debugMode) GNS.getLogger().info("Retransmission-Timeout\t" + LocalNameServer.nodeID + "\t" + retransmissionTimeout + "\t");
    } else {
      retransmissionTimeout = StartLocalNameServer.queryTimeout;
    }
    GNS.getLogger().fine(" asdfas ...");
    //if (StartLocalNameServer.debugMode) GNRS.getLogger().info("***RETRAN*** COUNT = " + transmissionCount + " NODE = " + LocalNameServer.nodeID + " timeout = " + retransmissionTimeout + " total timeout = " + totalTimerWait);
    LocalNameServer.timer.schedule(
            new LNSQueryTask(
            incomingPacket,
            transmissionCount + 1,
            senderAddress,
            senderPort,
            receivedTime,
            lookupNumber,
            queryId,
                    nameserversQueried),
            retransmissionTimeout);
//		if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("Query-" + lookupNumber + "\t" + System.currentTimeMillis() + "\t"
//        + incomingPacket.qname + "\tEnd-of-timer");
    // send DNS requests to NameServer
    //    boolean rescheduleTimer = 
    //    if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("4. Request sent to Name Server. " + incomingPacket.qrecordKey + " " + incomingPacket.qname);
    //    if (rescheduleTimer) {
    //      
    //      if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("5.1 Timer Rescheduled." + incomingPacket.qrecordKey + " " + incomingPacket.qname);
    //    } else {
    //      if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("5.2 Timer Not Rescheduled." + incomingPacket.qrecordKey + " " + incomingPacket.qname);
    //      LocalNameServer.removeQueryInfo(queryId);
    //    }

  }

  /**
   * Send error response to users.
   *
   * @param dnsPacket
   * @param errorCode
   * @param address
   * @param port
   * @throws JSONException
   */
  private void errorResponse(DNSPacket dnsPacket, int errorCode, InetAddress address, int port) {

    dnsPacket.getHeader().setRcode(errorCode);
    dnsPacket.getHeader().setQr(DNSRecordType.RESPONSE);
    
    try {
//      Packet.sendUDPPacket(LocalNameServer.socket, dnsPacket.questionToJSONObject(),
//              address, port);
    	LNSListener.udpTransport.sendPacket(dnsPacket.toJSONObject(), address, port);
        if (StartLocalNameServer.debugMode) GNS.getLogger().fine("error sent --> " + dnsPacket.toJSONObjectQuestion().toString());
    }  catch (JSONException e) {
      e.printStackTrace();
    }

  }

  public static JSONObject getErrorPacket(DNSPacket dnsPacket1) {
      try {
          DNSPacket dnsPacket = new DNSPacket(dnsPacket1.toJSONObjectQuestion());
          dnsPacket.getHeader().setRcode(DNSRecordType.RCODE_ERROR);
          dnsPacket.getHeader().setQr(DNSRecordType.RESPONSE);
          return  dnsPacket.toJSONObject();
      } catch (JSONException e) {
          e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
      return null;


  }
  /**
   *
   * @return
   */
  private boolean checkIncomingPacketOkay() {
    if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Qname: " + incomingPacket.getQname()
            + " Is Query" + incomingPacket.isQuery());
    if (transmissionCount == 1 && (incomingPacket.getQname() == null || !incomingPacket.isQuery())) {
//			if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("Sending Error Response");

      GNS.getStatLogger().fine("Failed-LookupDNSPacketError"
              + lookupNumber + "\t"
              + incomingPacket.getQrecordKey() + "\t"
              + incomingPacket.getQname() + "\t"
              + transmissionCount + "\t"
              + receivedTime + "\t"
              + nameserversQueried.toString());

      //The packet contains error. Respond back with an error
      errorResponse(incomingPacket, DNSRecordType.RCODE_ERROR, senderAddress, senderPort);
      return false;
    }
    return true;
  }

  /**
   * Initial book keeping for a new DNS Request.
   */
  private void newDNSRequestBookkeeping() {
    LocalNameServer.incrementLookupRequest(incomingPacket.getQname());
  }

  /**
   * Log data for entries already in cache.
   */
  private void loggingForAddressInCache() {
    NameRecordKey nameRecordKey = incomingPacket.getQrecordKey();
    String name = incomingPacket.getQname();
    if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Valid Address in cache... "
            + "Time:" + LocalNameServer.timeSinceAddressCached(name, nameRecordKey) + "ms");
    LocalNameServer.incrementLookupResponse(name);
    QueryInfo tempQueryInfo = new QueryInfo(-1, incomingPacket.getQname(), incomingPacket.getQrecordKey(), receivedTime, -1, QueryInfo.CACHE, lookupNumber,
            incomingPacket, senderAddress, senderPort);
    tempQueryInfo.setRecvTime(System.currentTimeMillis());
    String stats = tempQueryInfo.getLookupStats();
    GNS.getStatLogger().info("Success-LookupRequest\t" + stats);
    if (GNS.getLogger().isLoggable(Level.FINER)) {
      if (StartLocalNameServer.debugMode) GNS.getLogger().finer(LocalNameServer.cacheLogString("LNS CACHE: "));
      if (StartLocalNameServer.debugMode) GNS.getLogger().finer(LocalNameServer.nameRecordStatsMapLogString());
    }
  }

  /**
   * Send DNS Query reply to User
   */
  private void sendReplyToUser(QueryResultValue value, int TTL) {
//    CacheEntry entry = LocalNameServer.getCacheEntry(incomingPacket.getQname());
    if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Send response from cache: " + incomingPacket.getQname());
    DNSPacket outgoingPacket = new DNSPacket(incomingPacket.getHeader().getId(),incomingPacket.getQname(),incomingPacket.getQrecordKey(),value,TTL);
    try {
        if (senderAddress != null && senderPort > 0) {
    	    LNSListener.udpTransport.sendPacket(outgoingPacket.toJSONObject(), senderAddress, senderPort);
        }
//      Packet.sendUDPPacket(LocalNameServer.socket, outgoingPacket.responseToJSONObject(), senderAddress, senderPort);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Sends a DNS query by selecting an active/primary.
   *
   * @return True if any name server to send this query to is found, false if no name servers are found.
   */
  private int sendDNSLookupToNameServer() {
    String name = incomingPacket.getQname();
    NameRecordKey nameRecordKey = incomingPacket.getQrecordKey();
//		if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("sendDNSLookup for name:" + name + " / " + nameRecordKey.name() + " for queryId:" + queryId + " to the queryTransmittedMap");
    //		LocalNameServer.setDataToProceed(queryId, false);
    //		transmissionTime = System.currentTimeMillis();
    if (StartLocalNameServer.debugMode) GNS.getLogger().info("Lookup # " + transmissionCount + " for " + name + " / " + nameRecordKey.getName());

//		if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("Query-" + lookupNumber + "\t" + System.currentTimeMillis() + "\t"
//        + incomingPacket.qname + "\tSend-dns-lookup-enter");
    int nameServerID = -1;
    String queryStatus = null;
    while (true) {

        nameServerID = LocalNameServer.getClosestActiveNameServerFromCache(name, nameserversQueried);
    	// ACTIVE IN CACHE.
//      if (LocalNameServer.isValidNameserverInCache(name, nameRecordKey)) {
//        // Active name server information available in cache.s
//        // Send a lookup query to the closest active name server.
//        if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("Name: " + name + " / " + nameRecordKey.getName() + " Address invalid in cache" + "TimeAddress:" + LocalNameServer.timeSinceAddressCached(name, nameRecordKey) + "ms");
//        if (StartLocalNameServer.beehiveReplication && StartLocalNameServer.loadDependentRedirection) {
//        	nameServerID = LocalNameServer.getLoadAwareBeehiveNameServerFromCache(name, nameRecordKey, nameserversQueried);
//        }
//        else if (StartLocalNameServer.beehiveReplication) {
//          nameServerID = LocalNameServer.getBeehiveNameServerFromCache(name, nameRecordKey, nameserversQueried);
//        } else if (StartLocalNameServer.loadDependentRedirection) {
//          nameServerID = LocalNameServer.getBestActiveNameServerFromCache(name, nameRecordKey, nameserversQueried);
//        } else {
//
//        }

//      }
      if (nameServerID == -1) {
    	  // NO ACTIVE IN CACHE
    	  LocalNameServer.removeQueryInfo(queryId);
    	  LNSQueryTask queryTaskObject = new LNSQueryTask(
  				incomingPacket,
  				1, // number of transmissions = 0
  				senderAddress,
  				senderPort,
  				receivedTime,
  				lookupNumber,
  				0,
  				new HashSet<Integer>());
    	  PendingTasks.addToPendingRequests(name, //nameRecordKey, 
                  queryTaskObject, 0,
                  senderAddress, senderPort, getErrorPacket(incomingPacket));
    	  SendActivesRequestTask.requestActives(name);
    	  return 2;
      }
        else {
          if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Contacting closest active. Name:" + name + " / " + nameRecordKey.getName() + " ClosestNS:" + nameServerID);
      }
      
      //No name server available to query. Ignore this query.
      if (nameServerID == -1) {
        if (StartLocalNameServer.debugMode) GNS.getLogger().fine("LNSListenerQuery: NO NAME SERVER FOR " +
                name + " / " + nameRecordKey.getName());
        //      errorResponse(incomingPacket, RecordType.RCODE_ERROR, senderAddress, senderPort);

        return 1;
      }

      latestNameServerQueried = nameServerID;
//			if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("Query-" + lookupNumber + "\t" + System.currentTimeMillis() + "\t"
//	        + incomingPacket.qname + "\tSelected-name-server");
      //Save query information at the local name server to match response
      if (transmissionCount == 1) {
    	  
    	nameserversQueried.add(nameServerID);
        queryStatus = (queryStatus == null) ? QueryInfo.SINGLE_TRANSMISSION : queryStatus + "-" + QueryInfo.SINGLE_TRANSMISSION;
        //Get a unique id for this query
        queryId = LocalNameServer.addQueryInfo(name, nameRecordKey, nameServerID,
                receivedTime, queryStatus, (int) lookupNumber,
                incomingPacket, senderAddress, senderPort);
        
        if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Added name:" + name + " / " + nameRecordKey.getName() + " queryId:" + queryId + " to the queryTransmittedMap");
        if (GNS.getLogger().isLoggable(Level.FINER)) {
          if (StartLocalNameServer.debugMode) GNS.getLogger().finer(LocalNameServer.queryLogString("QUERYTABLE: "));
        }
        break;

      } else {
        queryStatus = (queryStatus == null) ? QueryInfo.MULTIPLE_TRANSMISSION : queryStatus + "-" + QueryInfo.MULTIPLE_TRANSMISSION;
        nameserversQueried.add(nameServerID);
        //Re-transmit the query to another active name server.
        if (LocalNameServer.addNameServerQueried(queryId, nameServerID, queryStatus)) {
          break;
        }

      }
    }

//		if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("Query-" + lookupNumber + "\t" + System.currentTimeMillis() + "\t"
//        + incomingPacket.qname + "\tAfter-while-loop");
    //store the id of the active name server being queried
//		nameserversQueried.add(nameServerID);

    //Create a DNS packet and send the query to the name server
    Header header = new Header(queryId, DNSRecordType.QUERY, DNSRecordType.RCODE_NO_ERROR);
    DNSPacket queryrecord = new DNSPacket(header, name, nameRecordKey, LocalNameServer.nodeID);

//		if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("Query-" + lookupNumber + "\t" + System.currentTimeMillis() + "\t"
//        + incomingPacket.qname + "\tBefore-send-packet");
    JSONObject json;

    try {
      json = queryrecord.toJSONObjectQuestion();
    } catch (JSONException e) {
      e.printStackTrace();
      if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Error Converting Query to JSON Object.");
      return 0;
    }
    Random r = new Random();
    try {
    	if (StartLocalNameServer.delayScheduling) {
			double latency = ConfigFileInfo.getPingLatency(nameServerID) * 
					( 1 + r.nextDouble() * StartLocalNameServer.variation);
			long timerDelay = (long) latency;
			
			LocalNameServer.timer.schedule(new SendQueryWithDelay2(json, nameServerID), timerDelay);
		}
		else {
			LNSListener.udpTransport.sendPacket(json, nameServerID, GNS.PortType.UPDATE_PORT);
//			Packet.sendUDPPacket(nameServerID, LocalNameServer.socket, json, GNRS.PortType.DNS_PORT);
		}
    	
    	int sentPacketSize = json.toString().getBytes().length;
      if (StartLocalNameServer.debugMode) GNS.getLogger().info("LNS-SentPkt\t" + lookupNumber + "\t" + sentPacketSize + "\t");
      
      StatusClient.sendTrafficStatus(LocalNameServer.nodeID, nameServerID, GNS.PortType.DNS_PORT, Packet.PacketType.DNS, name
              //, nameRecordKey
              );
      
    } catch (JSONException e)
	{
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
//		if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("Query-" + lookupNumber + "\t" + System.currentTimeMillis() + "\t"
//        + incomingPacket.qname + "\tAfter-send-packet");
    if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Sent Lookup to NS:" + nameServerID + " --> " + json.toString());
    return 0;
  }

  private void logFailureMessage(QueryInfo query) {

    String failureCode = "Failed-LookupNoResponseReceived";
    if (nameserversQueried.isEmpty()) {
      failureCode = "Failed-LookupNoNameServer";
    }
    String queryStatus = "";
    if (query != null && query.getQueryStatus() != null) {
      queryStatus = query.getQueryStatus();
    }
    GNS.getStatLogger().fine(failureCode + "\t"
            + lookupNumber + "\t"
            + incomingPacket.getQrecordKey() + "\t"
            + incomingPacket.getQname() + "\t"
            + transmissionCount + "\t"
            + receivedTime + "\t"
            + queryStatus + "\t"
            + nameserversQueried.toString());
  }
}

class SendQueryWithDelay2 extends TimerTask {
	JSONObject json;
	int nameserver;
	public SendQueryWithDelay2(JSONObject json, int nameserver) {
		this.json = json;
		this.nameserver = nameserver;
	}
	@Override
	public void run() {
		// send packet
		try
		{
			LNSListener.udpTransport.sendPacket(json, nameserver, GNS.PortType.UPDATE_PORT);
		} catch (JSONException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		try {
//			Packet.sendUDPPacket(nameserver, LocalNameServer.socket, json, GNRS.PortType.DNS_PORT);
//		}catch (IOException e)
//		{
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
	}
	
	
}
