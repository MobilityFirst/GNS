package edu.umass.cs.gns.localnameserver;

/**
 * Created with IntelliJ IDEA.
 * User: abhigyan
 * Date: 8/30/13
 * Time: 3:33 PM
 * To change this template use File | Settings | File Templates.
 */

import edu.umass.cs.gns.client.Intercessor;
import edu.umass.cs.gns.nameserver.ResultValue;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.packet.DNSPacket;
import edu.umass.cs.gns.packet.DNSRecordType;
import edu.umass.cs.gns.util.BestServerSelection;
import edu.umass.cs.gns.util.ConfigFileInfo;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

//import edu.umass.cs.gns.packet.QueryResultValue;



public class LNSQueryTask2 extends TimerTask {

  DNSPacket incomingPacket;
  InetAddress senderAddress;
  int senderPort;
  long receivedTime; // overall latency
  private int transmissionCount  = 0;
  private int lookupNumber;
  private int queryId = 0;
  private HashSet<Integer> nameserversQueried;

  public LNSQueryTask2(DNSPacket incomingPacket,
               InetAddress senderAddress, int senderPort, long receivedTime,
               int lookupNumber, int queryId,
               HashSet<Integer> nameserversQueried) {
    this.incomingPacket = incomingPacket;
    this.senderAddress = senderAddress;
    this.senderPort = senderPort;
    this.receivedTime = receivedTime;
    this.lookupNumber = lookupNumber;
//    this.queryId = queryId;
    this.nameserversQueried = nameserversQueried;
//    latestNameServerQueried = -1;
//    this.totalTimerWait = totalTimerWait;
  }

  @Override
  public void run() {

    transmissionCount++;
//    long start = System.currentTimeMillis();
//    if(transmissionCount == 1) {
//      long delay = start - receivedTime;
//      if (delay > 10) {
//        GNS.getLogger().severe("LNS-long-startup\t" + delay+"\t"+ System.currentTimeMillis());
//      }
//    }

    if (transmissionCount > 1)   {

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
        throw new RuntimeException();
      }
      if (LocalNameServer.containsQueryInfo(queryId) == false) {
        if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Query ID does not exist. Removed due to Invalid Active NS or response recvd Query ID = " + queryId + " \t " + transmissionCount + "\t" + nameserversQueried + "\t");
        throw new RuntimeException();
        //    	return;
      }
    }

    CacheEntry cacheEntry = LocalNameServer.getCacheEntry(incomingPacket.getQname());
    if (cacheEntry != null) {
      if (transmissionCount > 1) LocalNameServer.removeQueryInfo(queryId);
      ResultValue value = cacheEntry.getValue(incomingPacket.getQrecordKey());
      GNS.getLogger().finer("CACHE VALUE=" + value);
      if (value != null) {
        loggingForAddressInCache();
        sendCachedReplyToUser(value, cacheEntry.getTTL());

        throw new RuntimeException();
//                  return;
      }
    }

    if (cacheEntry == null || cacheEntry.isValidNameserver() == false) {
      LocalNameServer.removeQueryInfo(queryId);
      LNSQueryTask2 queryTaskObject = new LNSQueryTask2(
              incomingPacket,
              senderAddress,
              senderPort,
              receivedTime,
              lookupNumber,
              0,
              new HashSet<Integer>());
      PendingTasks.addToPendingRequests(incomingPacket.getQname(),
              queryTaskObject, StartLocalNameServer.queryTimeout,
              senderAddress, senderPort, getErrorPacket(incomingPacket));
      SendActivesRequestTask.requestActives(incomingPacket.getQname());
      throw new RuntimeException();
    }
//    int ns = 0;
    int ns = BestServerSelection.getSmallestLatencyNS(cacheEntry.getActiveNameServers(), nameserversQueried);
    if (ns >= 0) {
      nameserversQueried.add(ns);
      //Save query information at the local name server to match response
      if (transmissionCount == 1) {


//        queryStatus = (queryStatus == null) ? QueryInfo.SINGLE_TRANSMISSION : queryStatus + "-" + QueryInfo.SINGLE_TRANSMISSION;
        //Get a unique id for this query
        queryId = LocalNameServer.addQueryInfo(incomingPacket.getQname(), incomingPacket.getQrecordKey(), ns,
                receivedTime, "x", (int) lookupNumber,
                incomingPacket, senderAddress, senderPort);
      }
//          if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Added name:" + name + " / " + nameRecordKey.getName() + " queryId:" + queryId + " to the queryTransmittedMap");
      int clientQueryID = incomingPacket.getQueryId();
      incomingPacket.setLnsId(LocalNameServer.nodeID);
      incomingPacket.getHeader().setId(queryId);


      JSONObject json;

      try {
        json = incomingPacket.toJSONObjectQuestion();
        GNS.getLogger().fine(">>>>>>>>>>>>>JSON Query is " + json);
      } catch (JSONException e) {
        e.printStackTrace();
        if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Error Converting Query to JSON Object.");
        return;
      }

      incomingPacket.getHeader().setId(clientQueryID);


      if (StartLocalNameServer.delayScheduling) {
        Random r = new Random();
        double latency = ConfigFileInfo.getPingLatency(ns) *
                ( 1 + r.nextDouble() * StartLocalNameServer.variation);
        long timerDelay = (long) latency;

        LocalNameServer.executorService.schedule(new SendQueryWithDelay2(json, ns), timerDelay, TimeUnit.MILLISECONDS);
      }
      else {
        try {
          LNSListener.tcpTransport.sendToID(ns, json);
        } catch (IOException e) {
          e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        //			LNSListener.udpTransport.sendPacket(json, nameServerID, GNS.PortType.UPDATE_PORT);
        //			Packet.sendUDPPacket(nameServerID, LocalNameServer.socket, json, GNRS.PortType.DNS_PORT);
      }
      throw new RuntimeException();
//      long delay = System.currentTimeMillis() - start;
//      if (delay > 10) {
//        GNS.getLogger().severe("LNS-long-processing\t" + delay+"\t"+ System.currentTimeMillis());
//      }
//          break;

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
    QueryInfo tempQueryInfo = new QueryInfo(-1, incomingPacket.getQname(), incomingPacket.getQrecordKey(), receivedTime, -1, "NA", lookupNumber,
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
  private void sendCachedReplyToUser(ResultValue value, int TTL) {
//    CacheEntry entry = LocalNameServer.getCacheEntry(incomingPacket.getQname());
    if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Send response from cache: " + incomingPacket.getQname());
    DNSPacket outgoingPacket = new DNSPacket(incomingPacket.getHeader().getId(), 
            incomingPacket.getQname(), incomingPacket.getQrecordKey(), value, TTL);
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

  private void logFailureMessage(QueryInfo query) {

    String failureCode = "Failed-LookupNoResponseReceived";
    if (nameserversQueried.isEmpty()) {
      failureCode = "Failed-LookupNoNameServer";
    }
    String queryStatus = "NA";
//    if (query != null && query.getQueryStatus() != null) {
//      queryStatus = query.getQueryStatus();
//    }
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
    try {
      LNSListener.tcpTransport.sendToID(nameserver,json);
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }


}
