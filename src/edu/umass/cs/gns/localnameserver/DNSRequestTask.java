package edu.umass.cs.gns.localnameserver;

/**
 * Created with IntelliJ IDEA.
 * User: abhigyan
 * Date: 8/30/13
 * Time: 3:33 PM
 * To change this template use File | Settings | File Templates.
 */

import edu.umass.cs.gns.client.Intercessor;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.ReplicationFrameworkType;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.nameserver.ResultValue;
import edu.umass.cs.gns.packet.DNSPacket;
import edu.umass.cs.gns.packet.DNSRecordType;
import edu.umass.cs.gns.util.BestServerSelection;
import edu.umass.cs.gns.util.ConfigFileInfo;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.TimerTask;
import java.util.logging.Level;

//import edu.umass.cs.gns.packet.QueryResultValue;



public class DNSRequestTask extends TimerTask {

  DNSPacket incomingPacket;
  InetAddress senderAddress;
  int senderPort;
  long receivedTime; // overall latency
  private int transmissionCount  = 0;
  private int lookupNumber;
  private int queryId = 0;
  private int numRestarts;
  private HashSet<Integer> nameserversQueried;
  private int coordinatorID = -1;
  private static final boolean DISABLECACHE = false; // for testing - Westy

  public DNSRequestTask(DNSPacket incomingPacket,
                       InetAddress senderAddress, int senderPort, long receivedTime,
                       int lookupNumber, int queryId,
                       HashSet<Integer> nameserversQueried, int numRestarts) {
    this.incomingPacket = incomingPacket;
    this.senderAddress = senderAddress;
    this.senderPort = senderPort;
    this.receivedTime = receivedTime;
    this.lookupNumber = lookupNumber;
//    this.queryId = queryId;
    this.nameserversQueried = nameserversQueried;
    this.numRestarts = numRestarts;
//    latestNameServerQueried = -1;
//    this.totalTimerWait = totalTimerWait;
  }

  @Override
  public void run() {
    try{
      transmissionCount++;
//      long t0 = System.currentTimeMillis();
////      GNS.getLogger().severe("entering lns query task .... " + System.currentTimeMillis() + " count = " + transmissionCount);
//      long tA = 0;
//      long tB = 0;
//      long tC = 0;
//      long tD = 0;
//      long tE = 0;
//      long tF = 0;
//      long tG = 0;
//    if(transmissionCount == 1) {
//      long delay = start - receivedTime;
//      if (delay > 10) {
//        GNS.getLogger().severe("LNS-long-startup\t" + delay+"\t"+ System.currentTimeMillis());
//      }
//    }

//      if (numRestarts > StartLocalNameServer.MAX_RESTARTS) { // just a defensive code
//        errorResponse(incomingPacket, DNSRecordType.RCODE_ERROR, senderAddress, senderPort);
//        logFailureMessage();
//        throw new MyException();
//      }

      if (System.currentTimeMillis() - receivedTime > StartLocalNameServer.maxQueryWaitTime) {
        // send error response to user and log error
        if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Query timeout exceeded. " + incomingPacket.getQrecordKey() + " " + incomingPacket.getQname());



        DNSRequestInfo query = LocalNameServer.removeDNSRequestInfo(queryId);
        if (query != null) {
          if (StartLocalNameServer.debugMode) GNS.getLogger().fine("3.2.1 Query Info Removed." + query.getId());
        } else {
          if (StartLocalNameServer.debugMode) GNS.getLogger().fine("3.2.2 Query Info Does not exist.");
        }
        if (query == null && queryId != 0) {
          // means query response received
        } else {
          errorResponse(incomingPacket, DNSRecordType.RCODE_ERROR, senderAddress, senderPort);
          logFailureMessage();
        }



        throw new MyException();
      }
//      tA = System.currentTimeMillis();
      if (transmissionCount > 1)   {
        if (queryId != 0 && LocalNameServer.containsDNSRequestInfo(queryId) == false) {
          if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Query ID not found. Response recvd or invalid " +
                  "active error. Query ID\t" + queryId + "\t" + transmissionCount + "\t" + nameserversQueried + "\t");
          throw new MyException();
          //    	return;
        }
      }

      int ns = -1;
      if (StartLocalNameServer.replicateAll) {
        ns = BestServerSelection.getSmallestLatencyNS(ConfigFileInfo.getAllNameServerIDs(), nameserversQueried);
      } else {
//      tB = System.currentTimeMillis();
        CacheEntry cacheEntry = LocalNameServer.getCacheEntry(incomingPacket.getQname());

//      tC = System.currentTimeMillis();
        if (cacheEntry != null) {
          coordinatorID = LocalNameServer.getDefaultCoordinatorReplica(incomingPacket.getQname(),
                  cacheEntry.getActiveNameServers());
          ResultValue value = cacheEntry.getValue(incomingPacket.getQrecordKey());

          if (value != null) {
            if (transmissionCount > 1) LocalNameServer.removeQueryInfo(queryId);
            loggingForAddressInCache();
            sendCachedReplyToUser(value, cacheEntry.getTTL());

            throw new MyException();
//                  return;
          }
        }
//      tD = System.currentTimeMillis();
        if (cacheEntry == null || cacheEntry.isValidNameserver() == false) {
//        GNS.getLogger().severe("here invalid name server ....");
          if (transmissionCount > 1) LocalNameServer.removeQueryInfo(queryId);


//          if (numRestarts == StartLocalNameServer.MAX_RESTARTS) {
//            errorResponse(incomingPacket, DNSRecordType.RCODE_ERROR, senderAddress, senderPort);
//            logFailureMessage();
//            throw new MyException();
//          }

          DNSRequestTask queryTaskObject = new DNSRequestTask(
                  incomingPacket,
                  senderAddress,
                  senderPort,
                  receivedTime,
                  lookupNumber,
                  0,
                  new HashSet<Integer>(), numRestarts + 1);

          PendingTasks.addToPendingRequests(incomingPacket.getQname(),
                  queryTaskObject, StartLocalNameServer.queryTimeout,
                  senderAddress, senderPort, getErrorPacket(incomingPacket),getFailureLogMessage(lookupNumber, incomingPacket.getQrecordKey(), incomingPacket.getQname(),transmissionCount,receivedTime, numRestarts + 1, -1, nameserversQueried),0);
//        RequestActivesTask.requestActives(incomingPacket.getQname());
          throw new MyException();
        }
        if (StartLocalNameServer.loadDependentRedirection) {
          ns = LocalNameServer.getBestActiveNameServerFromCache(incomingPacket.getQname(), nameserversQueried);
        }
        else if (StartLocalNameServer.replicationFramework == ReplicationFrameworkType.BEEHIVE) {
          ns = LocalNameServer.getBeehiveNameServer(nameserversQueried, cacheEntry);
        }
        else {
          ns = BestServerSelection.getSmallestLatencyNS(cacheEntry.getActiveNameServers(), nameserversQueried);
        }
      }


      if (ns >= 0) {

//        tE = System.currentTimeMillis();
        nameserversQueried.add(ns);
        //Save query information at the local name server to match response
        if (transmissionCount == 1) {
//        queryStatus = (queryStatus == null) ? QueryInfo.SINGLE_TRANSMISSION : queryStatus + "-" + QueryInfo.SINGLE_TRANSMISSION;
          //Get a unique id for this query
          // TODO: change back to time query received
          queryId = LocalNameServer.addDNSRequestInfo(incomingPacket.getQname(), incomingPacket.getQrecordKey(), ns,
                  receivedTime, "x", lookupNumber, incomingPacket, senderAddress, senderPort, numRestarts);
        } else {
          DNSRequestInfo info = LocalNameServer.getDNSRequestInfo(queryId);
          if (info!= null) {
            info.setNameserverID(ns);
//            info.setLastNameServer();
//            info.setLastSendTime();
          }
        }
//          if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Added name:" + name + " / " + nameRecordKey.getName() + " queryId:" + queryId + " to the queryTransmittedMap");
        int clientQueryID = incomingPacket.getQueryId();
        incomingPacket.setLnsId(LocalNameServer.nodeID);
        incomingPacket.getHeader().setId(queryId);
//        tF = System.currentTimeMillis();

        JSONObject json;

        try {
          json = incomingPacket.toJSONObjectQuestion();
          GNS.getLogger().fine(">>>>>>>>>>>>>Send to node = " + ns + "  DNS Request = " + json);
        } catch (JSONException e) {
          e.printStackTrace();
          if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Error Converting Query to JSON Object.");
          return;
        }

        incomingPacket.getHeader().setId(clientQueryID);
//        tG = System.currentTimeMillis();

        LocalNameServer.sendToNS(json, ns);
//      throw new MyException();

//        long t1 = System.currentTimeMillis();
//        if (t1 - t0 > 20) {
//          GNS.getLogger().severe(" LNSQuerytask longlatency " + (t1 - t0) + "\tbreakdown\t" + (tA - t0)  + "\t"+ (tB - tA)  + "\t"  + (tC - tB)+ "\t" + (tD - tC) + "\t" + (tE - tD) + "\t" + (tF - tE) + "\t" + (tG - tF) + "\t" + (t1 - tG)) ;
//        }
//          break;
      }
    }catch (Exception e) {
      if (e.getClass().equals(MyException.class)) {
//        GNS.getLogger().severe("Catch here ...");
        throw new RuntimeException();
      }
      GNS.getLogger().severe("Exception Exception Exception .... ");
      e.printStackTrace();
    }
  }

  /**
   * Send error response to users.
   *
   * @param dnsPacket
   * @param errorCode
   * @param address
   * @param port
   * @throws org.json.JSONException
   */
  private void errorResponse(DNSPacket dnsPacket, int errorCode, InetAddress address, int port) {

    dnsPacket.getHeader().setRcode(errorCode);
    dnsPacket.getHeader().setQr(DNSRecordType.RESPONSE);

    try {
//      Packet.sendUDPPacket(LocalNameServer.socket, dnsPacket.questionToJSONObject(),
//              address, port);
      if (address!= null && port > 0){
        LNSListener.udpTransport.sendPacket(dnsPacket.toJSONObject(), address, port);
      } else if (StartLocalNameServer.runHttpServer) {
        Intercessor.getInstance().checkForResult(dnsPacket.toJSONObject());
      }
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
   * Log data for entries already in cache.
   */
  private void loggingForAddressInCache() {
    NameRecordKey nameRecordKey = incomingPacket.getQrecordKey();
    String name = incomingPacket.getQname();
    if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Valid Address in cache... "
            + "Time:" + LocalNameServer.timeSinceAddressCached(name, nameRecordKey) + "ms");
    LocalNameServer.incrementLookupResponse(name);
    DNSRequestInfo tempQueryInfo = new DNSRequestInfo(-1, incomingPacket.getQname(), incomingPacket.getQrecordKey(), receivedTime, -1, "NA", lookupNumber,
            incomingPacket, senderAddress, senderPort, numRestarts);
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
  private void sendCachedReplyToUser(ResultValue value, int TTL) {
//    CacheEntry entry = LocalNameServer.getCacheEntry(incomingPacket.getQname());
    if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Send response from cache: " + incomingPacket.getQname());
    DNSPacket outgoingPacket = new DNSPacket(incomingPacket.getHeader().getId(),incomingPacket.getQname(),incomingPacket.getQrecordKey(),value, TTL, new HashSet<Integer>());
    try {
      if (senderAddress != null && senderPort > 0) {
        LNSListener.udpTransport.sendPacket(outgoingPacket.toJSONObject(), senderAddress, senderPort);
      } else if (StartLocalNameServer.runHttpServer) {
        Intercessor.getInstance().checkForResult(outgoingPacket.toJSONObject());
      }
//      Packet.sendUDPPacket(LocalNameServer.socket, outgoingPacket.responseToJSONObject(), senderAddress, senderPort);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void logFailureMessage() {
    GNS.getStatLogger().fine(getFailureLogMessage(lookupNumber, incomingPacket.getQrecordKey(),
            incomingPacket.getQname(),transmissionCount,receivedTime, numRestarts, coordinatorID, nameserversQueried));
  }


  public static String getFailureLogMessage(int lookupNumber, NameRecordKey recordKey, String name,
                                            int transmissionCount, long receivedTime, int numRestarts,
                                            int coordinatorID, Set<Integer> nameserversQueried) {
    String failureCode = "Failed-LookupNoActiveResponse";
    if (nameserversQueried == null || nameserversQueried.isEmpty()) {
      failureCode = "Failed-LookupNoPrimaryResponse";
    }
//    String queryStatus = "NA";
//    if (query != null && query.getQueryStatus() != null) {
//      queryStatus = query.getQueryStatus();
//    }
    return (failureCode + "\t"
            + lookupNumber + "\t"
            + recordKey + "\t"
            + name + "\t"
            + transmissionCount + "\t"
            + receivedTime + "\t"
            + numRestarts + "\t"
            + coordinatorID + "\t"
            + nameserversQueried);
  }
}

class SendQueryWithDelayLNS extends TimerTask {
  JSONObject json;
  int destID;
  public SendQueryWithDelayLNS(JSONObject json, int destID) {
    this.json = json;
    this.destID = destID;
  }

  @Override
  public void run() {
    try {
      LNSListener.udpTransport.sendPacket(json,destID, GNS.PortType.LNS_UDP_PORT);
    } catch (JSONException e) {
      e.printStackTrace();
    }
//    try {
//      LNSListener.tcpTransport.sendToID(destID,json);
//    } catch (IOException e) {
//      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//    }
  }


}

class MyException extends  Exception  {

}
