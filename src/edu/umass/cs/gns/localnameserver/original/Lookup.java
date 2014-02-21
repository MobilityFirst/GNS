package edu.umass.cs.gns.localnameserver.original;

import edu.umass.cs.gns.client.Intercessor;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nameserver.ValuesMap;
import edu.umass.cs.gns.packet.DNSPacket;
import edu.umass.cs.gns.packet.Transport;
import edu.umass.cs.gns.util.AdaptiveRetransmission;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

//import edu.umass.cs.gns.packet.QueryResultValue;
public class Lookup {

  static int lookupCount = 0;
  static Object lock = new ReentrantLock();
//    static int numQueries = 0;
//    static int numQueryResponse = 0;

  public static void handlePacketLookupRequest(JSONObject json, DNSPacket dnsPacket)
          throws JSONException, UnknownHostException {
    synchronized (lock) {
      lookupCount++;
//      if GNS.getLogger().severe("\tLookupCount\t"+lookupCount);
    }
    InetAddress address = null;
    int port = Transport.getReturnPort(json);
    if (port > 0 && Transport.getReturnAddress(json) != null) {
      address = InetAddress.getByName(Transport.getReturnAddress(json));
    }
    LocalNameServer.incrementLookupRequest(dnsPacket.getGuid()); // important: used to count votes for names.
//    if (StartLocalNameServer.experimentMode) return;

//        numQueries++;
    //		if (StartLocalNameServer.debugMode) GNRS.getLogger().finer("Query-" + numQueries + "\t" + System.currentTimeMillis() + "\t"
    //                + dnsPacket.getQname() + "\tRecvd-packet");
    // schedule this task immediately, send query out.
    DNSRequestTask queryTaskObject = new DNSRequestTask(
            dnsPacket,
            address,
            port,
            System.currentTimeMillis(),
            0,
            0,
            new HashSet<Integer>(), 0);
//      LocalNameServer.executorService.schedule(queryTaskObject,0,TimeUnit.MILLISECONDS);
//    GNS.getLogger().fine("Query timeout is " + StartLocalNameServer.queryTimeout);
    long timeOut =  StartLocalNameServer.queryTimeout; //StartLocalNameServer.queryTimeout;
    if (StartLocalNameServer.adaptiveTimeout) {
      timeOut =  AdaptiveRetransmission.getTimeoutInterval(0);
    }

//    GNS.getLogger().severe("Query timeout is " + timeOut);
    LocalNameServer.executorService.scheduleAtFixedRate(queryTaskObject, 0, timeOut, TimeUnit.MILLISECONDS);
//        LocalNameServer.timer.schedule(queryTaskObject, 0);
    GNS.getLogger().finer("Scheduled this object.");
  }


  public static void handlePacketLookupResponse(JSONObject json, DNSPacket dnsPacket) throws JSONException {
//      long t0 = System.currentTimeMillis();
//      numQueryResponse++;
    GNS.getLogger().fine("LNS-RecvdPkt\t" + json);
    GNS.getLogger().fine("Query-" + dnsPacket.getQueryId() + "\t"
            + System.currentTimeMillis() + "\t" + dnsPacket.getGuid() + "\tListener-response-enter");
//      long receivedTime = System.currentTimeMillis();
//      long t1 = System.currentTimeMillis();
    if (dnsPacket.isResponse() && dnsPacket.containsAnyError() == false) {
      //Packet is a response and does not have a response error
      GNS.getLogger().finer("LNSListenerResponse: Received ResponseNum: "
              + (0) + " --> " + dnsPacket.toJSONObject().toString());
//        long t2 = System.currentTimeMillis();
      //Match response to the query sent
      DNSRequestInfo query = LocalNameServer.removeDNSRequestInfo(dnsPacket.getQueryId());
//        long t3 = System.currentTimeMillis();
      if (query == null) return;

//        long t4 = System.currentTimeMillis();
      if (LocalNameServer.r.nextDouble() < StartLocalNameServer.outputSampleRate) {
        query.setRecvTime(System.currentTimeMillis());
        String stats = query.getLookupStats();
        GNS.getStatLogger().fine("Success-LookupRequest\t" + stats);
//        long t5 = System.currentTimeMillis();
      }
//        if (StartLocalNameServer.debugMode) GNS.getLogger().finer("Query-" + lookupNumber + "\t" + System.currentTimeMillis() + "\t"
//                + incomingPacket.getQname() + "\tEnd-of-processing");
//        return;
//            if (query == null) {
//                if (StartLocalNameServer.debugMode) {
//                    GNS.getLogger().finer("LNSListenerResponse: No entry in queryTransmittedMap. QueryID:" + dnsPacket.getQueryId());
//                }
//                return;
//            }
      // if already received response, continue.
//            if (query.hasDataToProceed()) {
//                if (StartLocalNameServer.debugMode) {
//                    GNS.getLogger().finer("LNSListenerResponse: Received response already. QueryID:" + dnsPacket.getQueryId());
//                }
//                return;
//            }
      //Signal other threads that have received the data
//            query.setHasDataToProceed(true);

//            query.setRecvTime(receivedTime);

      if (StartLocalNameServer.adaptiveTimeout) {
        query.setRecvTime(System.currentTimeMillis());
        long responseTimeSample = query.getResponseTime();
        if (responseTimeSample != -1) {
          AdaptiveRetransmission.addResponseTimeSample(responseTimeSample);
        }
      }
//        long t2 = System.currentTimeMillis();
      // Abhigyan: need to update cache even for TTL == 0, because active name servers are updated.
      CacheEntry cacheEntry = LocalNameServer.updateCacheEntry(dnsPacket);
      if (cacheEntry == null) {
        cacheEntry = LocalNameServer.addCacheEntry(dnsPacket);
        GNS.getLogger().finer("LNSListenerResponse: Adding to cache QueryID:" + dnsPacket.getQueryId());
      }

      //Cache response at the local name server, and update the set of active name servers.
//			if ( ==)) {
//				;
//				if (StartLocalNameServer.debugMode) GNRS.getLogger().finer("LNSListenerResponse: Updating cache QueryID:" + dnsPacket.getQueryId());


      // Add to NameRecordStats.
//        LocalNameServer.incrementLookupResponse(dnsPacket.getQname());
//        long t6 = System.currentTimeMillis();
//            dnsPacket.s
      // send response to user right now.
      sendReplyToUser(query, dnsPacket.getRecordValue(), dnsPacket.getTTL());
//            sendReplyToUser(query, entry);
//        long tn = System.currentTimeMillis();
//        if (tn - t0 > 50) {
//          GNS.getLogger().severe("handle-response-long\t" + (tn-t0) + "\t" + (t1-t0) + "\t" + (t2-t1) + "\t" + (t3-t2) + "\t" + (t4-t3) + "\t" + (t5-t4) + "\t" + (tn-t5) + "\t" + tn);
//        }
      //GNRS.getLogger().finer("LNSListenerResponse: Removed name:" + dnsPacket.qname + " id:" + dnsPacket.getQueryId() + " from query table", debugMode);
//        if (GNS.getLogger().isLoggable(Level.FINER)) {
//          GNS.getLogger().finer(LocalNameServer.queryLogString("LNSListenerResponse QUERYTABLE:"));
//          GNS.getLogger().finer(LocalNameServer.cacheLogString("LNSListenerResponse CACHE: "));
//          GNS.getLogger().finer(LocalNameServer.nameRecordStatsMapLogString());
//        }
    }

  }

  public static void handlePacketLookupErrorResponse(JSONObject jsonObject, DNSPacket dnsPacket) throws JSONException {

    GNS.getLogger().fine("Recvd Lookup Error Response\t" + jsonObject);

//    DNSRequestInfo query = LocalNameServer.getDNSRequestInfo(dnsPacket.getQueryId());
//    if (query == null) return;
//    if (query.numRestarts >= 2 && dnsPacket.containsInvalidActiveNSError()) return;
    DNSRequestInfo query = LocalNameServer.removeDNSRequestInfo(dnsPacket.getQueryId());

    if (query == null) {
      GNS.getLogger().severe("LNSListenerResponse: No entry in queryTransmittedMap. QueryID:" + dnsPacket.getQueryId());
      return;
    }
    if (dnsPacket.containsInvalidActiveNSError()) { // if invalid active name server error, get correct active name servers
      GNS.getLogger().info(" Invalid Active Name Server.\tName\t" + dnsPacket.getGuid() + "\tRequest new actives.");
//      if (query.numRestarts == StartLocalNameServer.MAX_RESTARTS) {
//        GNS.getLogger().severe("Max restarts reached .... for name .. " + query.getqName() + " logging lookup failure");
//        query.setRecvTime(System.currentTimeMillis());
//        GNS.getStatLogger().fine(LNSQueryTask2.getFailureLogMessage(0,dnsPacket.getQrecordKey(),dnsPacket.getQname(),0,query.getLookupRecvdTime(),new HashSet<Integer>()));
//        try {
//          if (query.getSenderAddress()!= null && query.getSenderPort()> 0) {
//            LNSListener.udpTransport.sendPacket(LNSQueryTask2.getErrorPacket(query.getIncomingPacket()), query.getSenderAddress(), query.getSenderPort());
//          } else if (StartLocalNameServer.runHttpServer) {
//            Intercessor.checkForResult(LNSQueryTask2.getErrorPacket(query.getIncomingPacket()));
//          }
//          if (StartLocalNameServer.debugMode) {
//            GNS.getLogger().severe("other error sent to client --> " + jsonObject + " query ID = " + query.getIncomingPacket().getQueryId());
//          }
//        } catch (JSONException e) {
//          e.printStackTrace();
//        }
//        return;
//      }

      LocalNameServer.invalidateActiveNameServer(dnsPacket.getGuid());
      DNSRequestTask queryTaskObject = new DNSRequestTask(
              query.getIncomingPacket(), query.getSenderAddress(), query.getSenderPort(),
              query.getLookupRecvdTime(), query.getId(),
              0,
              new HashSet<Integer>(), query.numRestarts + 1);

      long delay = StartLocalNameServer.queryTimeout;

      if (query.numRestarts == 0) delay = 0;

      String failureMsg = DNSRequestTask.getFailureLogMessage(0,dnsPacket.getKey(),dnsPacket.getGuid(),
              0, query.getLookupRecvdTime(), query.numRestarts + 1, -1, new HashSet<Integer>());

      PendingTasks.addToPendingRequests(query.getqName(), queryTaskObject, StartLocalNameServer.queryTimeout,
              query.getSenderAddress(), query.getSenderPort(), DNSRequestTask.getErrorPacket(query.getIncomingPacket()),
              failureMsg, delay);

      GNS.getLogger().fine(" Scheduled lookup task.");

    } else {      // other types of errors, send error response to client
      try {
        if (query.getSenderAddress() != null && query.getSenderPort() > 0) {
          LNSListener.udpTransport.sendPacket(DNSRequestTask.getErrorPacket(query.getIncomingPacket()), query.getSenderAddress(), query.getSenderPort());
        } else if (StartLocalNameServer.runHttpServer) {
          Intercessor.checkForResult(DNSRequestTask.getErrorPacket(query.getIncomingPacket()));
        }
        GNS.getLogger().warning("other error sent to client --> " + jsonObject + " query ID = " + query.getIncomingPacket().getQueryId());
        GNS.getStatLogger().fine(DNSRequestTask.getFailureLogMessage(0,dnsPacket.getKey(),dnsPacket.getGuid(),
                0,query.getLookupRecvdTime(), query.numRestarts, -1, new HashSet<Integer>()));

      } catch (JSONException e) {
        e.printStackTrace();
      }

    }

    if (GNS.getLogger().isLoggable(Level.FINEST)) {
      GNS.getLogger().finest(LocalNameServer.dnsRequestInfoLogString("LNSListenerResponse QUERYTABLE:"));
      GNS.getLogger().finest(LocalNameServer.cacheLogString("LNSListenerResponse CACHE: "));
      GNS.getLogger().finest(LocalNameServer.nameRecordStatsMapLogString());
    }

  }

  /**
   * Send reply to user after DNS record is received.
   *
   * @param query
   */
  private static void sendReplyToUser(DNSRequestInfo query, ValuesMap returnValue, int TTL) {

    try {
      DNSPacket outgoingPacket = new DNSPacket(query.getIncomingPacket().getHeader().getId(), query.getIncomingPacket().getGuid(),
              query.getIncomingPacket().getKey(), returnValue, TTL, new HashSet<Integer>(), null, null, null);
      if (query.getSenderAddress() != null && query.getSenderPort() > 0) {
        LNSListener.udpTransport.sendPacket(outgoingPacket.toJSONObject(), query.getSenderAddress(), query.getSenderPort());
      } else if (StartLocalNameServer.runHttpServer) {
        Intercessor.checkForResult(outgoingPacket.toJSONObject());
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}