package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.client.Intercessor;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nameserver.ValuesMap;
import edu.umass.cs.gns.packet.DNSPacket;
import edu.umass.cs.gns.packet.QueryResponsePacket;
import edu.umass.cs.gns.packet.Transport;
import edu.umass.cs.gns.util.AdaptiveRetransmission;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import org.json.JSONException;
import org.json.JSONObject;

public class Query {

//    static int numQueries = 0;
//    static int numQueryResponse = 0;
  public static void handlePacketLookupRequest(JSONObject json, DNSPacket dnsPacket) throws JSONException, UnknownHostException {

    InetAddress address = null;
    int port = Transport.getReturnPort(json);
    if (port > 0 && Transport.getReturnAddress(json) != null) {
      address = InetAddress.getByName(Transport.getReturnAddress(json));
    }
    LocalNameServer.incrementLookupRequest(dnsPacket.getQname()); // important: used to count votes for names.
//        numQueries++;
    //		if (StartLocalNameServer.debugMode) GNRS.getLogger().finer("Query-" + numQueries + "\t" + System.currentTimeMillis() + "\t"
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
    LocalNameServer.executorService.scheduleAtFixedRate(queryTaskObject, 0, StartLocalNameServer.queryTimeout, TimeUnit.MILLISECONDS);
//        LocalNameServer.timer.schedule(queryTaskObject, 0);
    GNS.getLogger().finer("Scheduled this object.");

  }

  public static void handlePacketLookupResponse(JSONObject json, QueryResponsePacket dnsPacket) throws JSONException {


  }

  // WILL NEED SOME OF THIS BELOW AS WELL
  
//  private static void sendReplyToUser(DNSQueryInfo query, ValuesMap returnValue, int TTL) {
////		CacheEntry entry = LocalNameServer.getCacheEntry(query.qName, query.qRecordKey);
////		if (StartLocalNameServer.debugMode) GNRS.getLogger().finer("LNSListenerQuery: send response from cache: " + entry);
////        DNSPacket outgoingPacket = new DNSPacket(query.incomingPacket.getHeader().getId(), entry, query.incomingPacket.getQrecordKey());
//
//    try {
//      QueryResponsePacket outgoingPacket = new QueryResponsePacket(query.incomingPacket.getHeader().getId(), query.incomingPacket.getQname(),
//              query.incomingPacket.getQrecordKey(), returnValue, TTL);
//      if (query.senderAddress != null && query.senderPort > 0) {
//        LNSListener.udpTransport.sendPacket(outgoingPacket.toJSONObject(), query.senderAddress, query.senderPort);
//      } else if (StartLocalNameServer.runHttpServer) {
//        Intercessor.getInstance().checkForResult(outgoingPacket.toJSONObject());
//      }
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
//  }
}