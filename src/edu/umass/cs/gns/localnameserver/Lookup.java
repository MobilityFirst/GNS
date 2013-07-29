package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.packet.DNSPacket;
import edu.umass.cs.gns.packet.Transport;
import edu.umass.cs.gns.util.AdaptiveRetransmission;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.logging.Level;

public class Lookup {

    static int numQueries = 0;
    static int numQueryResponse = 0;

    public static void handlePacketLookupRequest(JSONObject json, DNSPacket dnsPacket) throws JSONException, UnknownHostException {

        InetAddress address = null;
        int port = -1;
        port = Transport.getReturnPort(json);
        if (Transport.getReturnAddress(json) != null) {
            address = InetAddress.getByName(Transport.getReturnAddress(json));
        }

        numQueries++;
        //		if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("Query-" + numQueries + "\t" + System.currentTimeMillis() + "\t"
        //                + dnsPacket.getQname() + "\tRecvd-packet");
        // schedule this task immediately, send query out.
        LNSQueryTask queryTaskObject = new LNSQueryTask(
                dnsPacket,
                1,
                address,
                port,
                System.currentTimeMillis(),
                numQueries,
                0,
                new HashSet<Integer>(),
                0);
        LocalNameServer.timer.schedule(queryTaskObject, 0);
        if (StartLocalNameServer.debugMode) {
            GNS.getLogger().fine("Scheduled this object.");
        }
    }

    public static void handlePacketLookupResponse(JSONObject json, DNSPacket dnsPacket) throws JSONException {
        numQueryResponse++;
        if (StartLocalNameServer.debugMode) {
            GNS.getLogger().info("LNS-RecvdPkt\t" + numQueryResponse + "\t");
        }
        if (StartLocalNameServer.debugMode) {
            GNS.getLogger().fine("Query-" + dnsPacket.getQueryId() + "\t"
                    + System.currentTimeMillis() + "\t" + dnsPacket.getQname() + "\tListener-response-enter");
        }

        long receivedTime = System.currentTimeMillis();

        if (dnsPacket.isResponse() && !dnsPacket.containsAnyError()) {
            //Packet is a response and does not have a response error
            if (StartLocalNameServer.debugMode) {
                GNS.getLogger().fine("LNSListenerResponse: Received ResponseNum: "
                        + (++numQueryResponse) + " --> " + dnsPacket.toJSONObject().toString());
            }

            //Match response to the query sent
            QueryInfo query = LocalNameServer.getQueryInfo(dnsPacket.getQueryId());
            if (query == null) {
                if (StartLocalNameServer.debugMode) {
                    GNS.getLogger().fine("LNSListenerResponse: No entry in queryTransmittedMap. QueryID:" + dnsPacket.getQueryId());
                }
                return;
            }
            // if already received response, continue.
            if (query.hasDataToProceed()) {
                if (StartLocalNameServer.debugMode) {
                    GNS.getLogger().fine("LNSListenerResponse: Received response already. QueryID:" + dnsPacket.getQueryId());
                }
                return;
            }
            //Signal other threads that have received the data
            query.setHasDataToProceed(true);

            query.setRecvTime(receivedTime);

            if (StartLocalNameServer.adaptiveTimeout) {
                long responseTimeSample = query.getResponseTime();
                if (responseTimeSample != -1) {
                    AdaptiveRetransmission.addResponseTimeSample(responseTimeSample);
                }
            }
            CacheEntry entry = LocalNameServer.updateCacheEntry(dnsPacket);
            //Cache response at the local name server, and update the set of active name servers.
//			if ( ==)) {
//				;
//				if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("LNSListenerResponse: Updating cache QueryID:" + dnsPacket.getQueryId());
            if (entry == null) {
                entry = LocalNameServer.addCacheEntry(dnsPacket);
                if (StartLocalNameServer.debugMode) {
                    GNS.getLogger().fine("LNSListenerResponse: Adding to cache QueryID:" + dnsPacket.getQueryId());
                }
            }

            // Add to NameRecordStats.
            LocalNameServer.incrementLookupResponse(dnsPacket.getQname());

//            dnsPacket.s
            // send response to user right now.
            sendReplyToUser(query, entry);

            //GNRS.getLogger().fine("LNSListenerResponse: Removed name:" + dnsPacket.qname + " id:" + dnsPacket.getQueryId() + " from query table", debugMode);
            if (GNS.getLogger().isLoggable(Level.FINER)) {
                GNS.getLogger().finer(LocalNameServer.queryLogString("LNSListenerResponse QUERYTABLE:"));
                GNS.getLogger().finer(LocalNameServer.cacheLogString("LNSListenerResponse CACHE: "));
                GNS.getLogger().finer(LocalNameServer.nameRecordStatsMapLogString());
            }
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
                GNS.getLogger().fine("ListenerResponse: Received ResponseNum: " + (++numQueryResponse)
                        + "	Invalid-active-name-server. " + dnsPacket.toJSONObject().toString());
            }
            LocalNameServer.invalidateActiveNameServer(dnsPacket.getQname());
            LNSQueryTask queryTaskObject = new LNSQueryTask(
                    query.incomingPacket,
                    1,
                    query.senderAddress,
                    query.senderPort,
                    query.getLookupRecvdTime(),
                    query.id,
                    0,
                    new HashSet<Integer>(),
                    0);
            PendingTasks.addToPendingRequests(query.qName, //query.qRecordKey,
                    queryTaskObject, 0,
                    query.senderAddress, query.senderPort, LNSQueryTask.getErrorPacket(query.incomingPacket));
            SendActivesRequestTask.requestActives(query.qName);
            if (StartLocalNameServer.debugMode) {
                GNS.getLogger().fine(" Scheduled lookup task.");
            }
        }
        else {      // other types of errors, send error response to client
            try {

                LNSListener.udpTransport.sendPacket(LNSQueryTask.getErrorPacket(query.incomingPacket), query.senderAddress, query.senderPort);
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
    private static void sendReplyToUser(QueryInfo query, CacheEntry entry) {
//		CacheEntry entry = LocalNameServer.getCacheEntry(query.qName, query.qRecordKey);
//		if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("LNSListenerQuery: send response from cache: " + entry);
        DNSPacket outgoingPacket = new DNSPacket(query.incomingPacket.getHeader().getId(), entry, query.incomingPacket.getQrecordKey());
        try {

            if (query.senderAddress != null && query.senderPort > 0) {
                LNSListener.udpTransport.sendPacket(outgoingPacket.toJSONObject(), query.senderAddress, query.senderPort);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}