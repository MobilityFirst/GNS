package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.client.Intercessor;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.packet.DNSPacket;
import edu.umass.cs.gns.packet.QueryResultValue;
import edu.umass.cs.gns.packet.Transport;
import edu.umass.cs.gns.util.AdaptiveRetransmission;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class Lookup {

//    static int numQueries = 0;
//    static int numQueryResponse = 0;

    public static void handlePacketLookupRequest(JSONObject json, DNSPacket dnsPacket) throws JSONException, UnknownHostException {

        InetAddress address = null;
        int port = Transport.getReturnPort(json);
        if (port > 0 && Transport.getReturnAddress(json) != null) {
            address = InetAddress.getByName(Transport.getReturnAddress(json));
        }

//        numQueries++;
        //		if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("Query-" + numQueries + "\t" + System.currentTimeMillis() + "\t"
        //                + dnsPacket.getQname() + "\tRecvd-packet");
        // schedule this task immediately, send query out.
        LNSQueryTask2 queryTaskObject = new LNSQueryTask2(
                dnsPacket,
                address,
                port,
                System.currentTimeMillis(),
                0,
                0,
                new HashSet<Integer>());
//      LocalNameServer.executorService.schedule(queryTaskObject,0,TimeUnit.MILLISECONDS);
      LocalNameServer.executorService.scheduleAtFixedRate(queryTaskObject,0,StartLocalNameServer.queryTimeout, TimeUnit.MILLISECONDS);
//        LocalNameServer.timer.schedule(queryTaskObject, 0);
        if (StartLocalNameServer.debugMode) {
            GNS.getLogger().fine("Scheduled this object.");
        }
    }

    public static void handlePacketLookupResponse(JSONObject json, DNSPacket dnsPacket) throws JSONException {
//      long t0 = System.currentTimeMillis();
//      numQueryResponse++;
      if (StartLocalNameServer.debugMode) {
        GNS.getLogger().info("LNS-RecvdPkt\t");
      }
      if (StartLocalNameServer.debugMode) {
        GNS.getLogger().fine("Query-" + dnsPacket.getQueryId() + "\t"
                + System.currentTimeMillis() + "\t" + dnsPacket.getQname() + "\tListener-response-enter");
      }

//      long receivedTime = System.currentTimeMillis();
//      long t1 = System.currentTimeMillis();
      if (dnsPacket.isResponse() && dnsPacket.containsAnyError() == false) {
        //Packet is a response and does not have a response error
        if (StartLocalNameServer.debugMode) {
          GNS.getLogger().fine("LNSListenerResponse: Received ResponseNum: "
                  + (0) + " --> " + dnsPacket.toJSONObject().toString());
        }
//        long t2 = System.currentTimeMillis();
        //Match response to the query sent
        QueryInfo query = LocalNameServer.removeQueryInfo(dnsPacket.getQueryId());
//        long t3 = System.currentTimeMillis();
        if (query == null) return;


//        long t4 = System.currentTimeMillis();
        if (LocalNameServer.r.nextDouble() < StartLocalNameServer.outputSampleRate) {
          query.setRecvTime(System.currentTimeMillis());
          String stats = query.getLookupStats();
          GNS.getStatLogger().fine("Success-LookupRequest\t" + stats);
//        long t5 = System.currentTimeMillis();
        }
//        if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Query-" + lookupNumber + "\t" + System.currentTimeMillis() + "\t"
//                + incomingPacket.getQname() + "\tEnd-of-processing");
//        return;
//            if (query == null) {
//                if (StartLocalNameServer.debugMode) {
//                    GNS.getLogger().fine("LNSListenerResponse: No entry in queryTransmittedMap. QueryID:" + dnsPacket.getQueryId());
//                }
//                return;
//            }
        // if already received response, continue.
//            if (query.hasDataToProceed()) {
//                if (StartLocalNameServer.debugMode) {
//                    GNS.getLogger().fine("LNSListenerResponse: Received response already. QueryID:" + dnsPacket.getQueryId());
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
        if (dnsPacket.getTTL() != 0) {// no need to update cache if TTL = 0.
          CacheEntry cacheEntry = LocalNameServer.updateCacheEntry(dnsPacket);

          //Cache response at the local name server, and update the set of active name servers.
//			if ( ==)) {
//				;
//				if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("LNSListenerResponse: Updating cache QueryID:" + dnsPacket.getQueryId());
          if (cacheEntry == null) {
            cacheEntry = LocalNameServer.addCacheEntry(dnsPacket);
            if (StartLocalNameServer.debugMode) {
              GNS.getLogger().fine("LNSListenerResponse: Adding to cache QueryID:" + dnsPacket.getQueryId());
            }
          }
        }
        // Add to NameRecordStats.
//        LocalNameServer.incrementLookupResponse(dnsPacket.getQname());
//        long t6 = System.currentTimeMillis();
//            dnsPacket.s
        // send response to user right now.
        sendReplyToUser(query, dnsPacket.getFieldValue(), dnsPacket.getTTL());
//            sendReplyToUser(query, entry);
//        long tn = System.currentTimeMillis();
//        if (tn - t0 > 50) {
//          GNS.getLogger().severe("handle-response-long\t" + (tn-t0) + "\t" + (t1-t0) + "\t" + (t2-t1) + "\t" + (t3-t2) + "\t" + (t4-t3) + "\t" + (t5-t4) + "\t" + (tn-t5) + "\t" + tn);
//        }
        //GNRS.getLogger().fine("LNSListenerResponse: Removed name:" + dnsPacket.qname + " id:" + dnsPacket.getQueryId() + " from query table", debugMode);
//        if (GNS.getLogger().isLoggable(Level.FINER)) {
//          GNS.getLogger().finer(LocalNameServer.queryLogString("LNSListenerResponse QUERYTABLE:"));
//          GNS.getLogger().finer(LocalNameServer.cacheLogString("LNSListenerResponse CACHE: "));
//          GNS.getLogger().finer(LocalNameServer.nameRecordStatsMapLogString());
//        }
      }

    }

    public static void handlePacketLookupErrorResponse(JSONObject jsonObject, DNSPacket dnsPacket) throws JSONException {

        QueryInfo query = LocalNameServer.removeQueryInfo(dnsPacket.getQueryId());
        if (query == null) {
            if (StartLocalNameServer.debugMode) {
                GNS.getLogger().fine("LNSListenerResponse: No entry in queryTransmittedMap. QueryID:" + dnsPacket.getQueryId());
            }
            return;
        }
        if (dnsPacket.containsInvalidActiveNSError()) { // if invalid active name server error, get correct active name servers
            if (StartLocalNameServer.debugMode) {
                GNS.getLogger().fine("ListenerResponse: Received ResponseNum: 0"
                        + "	Invalid-active-name-server. " + dnsPacket.toJSONObject().toString());
            }
            LocalNameServer.invalidateActiveNameServer(dnsPacket.getQname());
            LNSQueryTask2 queryTaskObject = new LNSQueryTask2(
                    query.incomingPacket,
                    query.senderAddress,
                    query.senderPort,
                    query.getLookupRecvdTime(),
                    query.id,
                    0,
                    new HashSet<Integer>());
            PendingTasks.addToPendingRequests(query.qName, //query.qRecordKey,
                    queryTaskObject, StartLocalNameServer.queryTimeout,
                    query.senderAddress, query.senderPort, LNSQueryTask.getErrorPacket(query.incomingPacket));
            SendActivesRequestTask.requestActives(query.qName);
            if (StartLocalNameServer.debugMode) {
                GNS.getLogger().fine(" Scheduled lookup task.");
            }
        }
        else {      // other types of errors, send error response to client
            try {
                if (query.senderAddress != null && query.senderPort > 0) {
                LNSListener.udpTransport.sendPacket(LNSQueryTask.getErrorPacket(query.incomingPacket), query.senderAddress, query.senderPort);
                } else if (StartLocalNameServer.runHttpServer) {
                  Intercessor.getInstance().checkForResult(LNSQueryTask.getErrorPacket(query.incomingPacket));
                }
                if (StartLocalNameServer.debugMode) GNS.getLogger().fine("other error sent to client --> " + jsonObject + " query ID = " + query.incomingPacket.getQueryId());
            }  catch (JSONException e) {
                e.printStackTrace();
            }

        }

        if (GNS.getLogger().isLoggable(Level.FINER)) {
            GNS.getLogger().finer(LocalNameServer.queryLogString("LNSListenerResponse QUERYTABLE:"));
            GNS.getLogger().finer(LocalNameServer.cacheLogString("LNSListenerResponse CACHE: "));
            GNS.getLogger().finer(LocalNameServer.nameRecordStatsMapLogString());
        }

    }



    /**
     * Send reply to user after DNS record is received.
     *
     * @param query
     */
    private static void sendReplyToUser(QueryInfo query, QueryResultValue value, int TTL) {
//		CacheEntry entry = LocalNameServer.getCacheEntry(query.qName, query.qRecordKey);
//		if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("LNSListenerQuery: send response from cache: " + entry);
//        DNSPacket outgoingPacket = new DNSPacket(query.incomingPacket.getHeader().getId(), entry, query.incomingPacket.getQrecordKey());

        try {

            if (query.senderAddress != null && query.senderPort > 0) {
              DNSPacket outgoingPacket = new DNSPacket(query.incomingPacket.getHeader().getId(),query.incomingPacket.getQname(),query.incomingPacket.getQrecordKey(),value,TTL);
                LNSListener.udpTransport.sendPacket(outgoingPacket.toJSONObject(), query.senderAddress, query.senderPort);
            } else  if (StartLocalNameServer.runHttpServer) {
              DNSPacket outgoingPacket = new DNSPacket(query.incomingPacket.getHeader().getId(),query.incomingPacket.getQname(),query.incomingPacket.getQrecordKey(),value,TTL);
              Intercessor.getInstance().checkForResult(outgoingPacket.toJSONObject());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}