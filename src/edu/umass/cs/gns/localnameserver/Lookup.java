package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.client.Intercessor;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nameserver.ValuesMap;
import edu.umass.cs.gns.packet.DNSPacket;
import edu.umass.cs.gns.packet.Transport;
import edu.umass.cs.gns.util.AdaptiveRetransmission;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import org.json.JSONException;
import org.json.JSONObject;

//import edu.umass.cs.gns.packet.QueryResultValue;
public class Lookup {

  static int lookupCount = 0;
  static Object lock = new ReentrantLock();

  public static void handlePacketLookupRequest(JSONObject json, DNSPacket dnsPacket)
          throws JSONException, UnknownHostException {
    synchronized (lock) {
      lookupCount++;
    }
    InetAddress address = null;
    int port = Transport.getReturnPort(json);
    if (port > 0 && Transport.getReturnAddress(json) != null) {
      address = InetAddress.getByName(Transport.getReturnAddress(json));
    }
    LocalNameServer.incrementLookupRequest(dnsPacket.getGuid()); // important: used to count votes for names.
    DNSRequestTask queryTaskObject = new DNSRequestTask(
            dnsPacket,
            address,
            port,
            System.currentTimeMillis(),
            0,
            0,
            new HashSet<Integer>(), 0);
    long timeOut = StartLocalNameServer.queryTimeout;
    if (StartLocalNameServer.adaptiveTimeout) {
      timeOut = AdaptiveRetransmission.getTimeoutInterval(0);
    }

    LocalNameServer.executorService.scheduleAtFixedRate(queryTaskObject, 0, timeOut, TimeUnit.MILLISECONDS);
    GNS.getLogger().finer("Scheduled this object.");
  }

  public static void handlePacketLookupResponse(JSONObject json, DNSPacket dnsPacket) throws JSONException {
//      long t0 = System.currentTimeMillis();
//      numQueryResponse++;
    GNS.getLogger().fine("LNS-RecvdPkt\t" + json);
    GNS.getLogger().fine("Query-" + dnsPacket.getQueryId() + "\t"
            + System.currentTimeMillis() + "\t" + dnsPacket.getGuid() + "\tListener-response-enter");
    if (dnsPacket.isResponse() && dnsPacket.containsAnyError() == false) {
      //Packet is a response and does not have a response error
      GNS.getLogger().finer("LNSListenerResponse: Received ResponseNum: "
              + (0) + " --> " + dnsPacket.toJSONObject().toString());
      //Match response to the query sent
      DNSRequestInfo query = LocalNameServer.removeDNSRequestInfo(dnsPacket.getQueryId());
      if (query == null) {
        return;
      }

      if (LocalNameServer.r.nextDouble() < StartLocalNameServer.outputSampleRate) {
        query.setRecvTime(System.currentTimeMillis());
        String stats = query.getLookupStats();
        GNS.getStatLogger().fine("Success-LookupRequest\t" + stats);;
      }

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
      // send response to user right now.
      sendReplyToUser(query, dnsPacket.getRecordValue(), dnsPacket.getTTL());
    }

  }

  public static void handlePacketLookupErrorResponse(JSONObject jsonObject, DNSPacket dnsPacket) throws JSONException {

    GNS.getLogger().fine("Recvd Lookup Error Response\t" + jsonObject);
    DNSRequestInfo query = LocalNameServer.removeDNSRequestInfo(dnsPacket.getQueryId());

    if (query == null) {
      GNS.getLogger().severe("LNSListenerResponse: No entry in queryTransmittedMap. QueryID:" + dnsPacket.getQueryId());
      return;
    }
    // if invalid active name server error, get correct active name servers
    if (dnsPacket.containsInvalidActiveNSError()) {
      GNS.getLogger().info(" Invalid Active Name Server.\tName\t" + dnsPacket.getGuid() + "\tRequest new actives.");
      LocalNameServer.invalidateActiveNameServer(dnsPacket.getGuid());
      DNSRequestTask queryTaskObject = new DNSRequestTask(
              query.getIncomingPacket(), query.getSenderAddress(), query.getSenderPort(),
              query.getLookupRecvdTime(), query.getId(),
              0,
              new HashSet<Integer>(), query.numRestarts + 1);

      long delay = StartLocalNameServer.queryTimeout;

      if (query.numRestarts == 0) {
        delay = 0;
      }

      String failureMsg = DNSRequestTask.getFailureLogMessage(0, dnsPacket.getKey(), dnsPacket.getGuid(),
              0, query.getLookupRecvdTime(), query.numRestarts + 1, -1, new HashSet<Integer>());

      PendingTasks.addToPendingRequests(query.getqName(), queryTaskObject, StartLocalNameServer.queryTimeout,
              query.getSenderAddress(), query.getSenderPort(), DNSRequestTask.getErrorPacket(query.getIncomingPacket()),
              failureMsg, delay);

      GNS.getLogger().fine(" Scheduled lookup task.");

    } else {      // other types of errors, send error response to client
      // a little weird here... the packet already has the error in it and we blow it away using getErrorPacket
      // prevents us from sending different errors back to the client
      // CHECK THIS AND FIX IT
      Intercessor.handleIncomingPackets(DNSRequestTask.getErrorPacket(query.getIncomingPacket()));

      GNS.getLogger().warning("other error sent to client --> " + jsonObject + " query ID = " + query.getIncomingPacket().getQueryId());
      GNS.getStatLogger().fine(DNSRequestTask.getFailureLogMessage(0, dnsPacket.getKey(), dnsPacket.getGuid(),
              0, query.getLookupRecvdTime(), query.numRestarts, -1, new HashSet<Integer>()));


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
      Intercessor.handleIncomingPackets(outgoingPacket.toJSONObject());
    } catch (Exception e) {
      GNS.getLogger().severe("Problem converting packet to JSON: " + e);
    }
  }
}