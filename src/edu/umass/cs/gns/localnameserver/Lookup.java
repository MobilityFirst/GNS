/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.clientsupport.Intercessor;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nameserver.ValuesMap;
import edu.umass.cs.gns.packet.DNSPacket;
import edu.umass.cs.gns.packet.DNSRecordType;
import edu.umass.cs.gns.packet.NSResponseCode;
import edu.umass.cs.gns.util.AdaptiveRetransmission;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * Class contains a few static methods for handling lookup requests from clients as well responses to lookups from
 * name servers. Most functionality for handling lookup requests from clients is implemented in
 * <code>DNSRequestTask</code>. So also refer to its documentation.
 * <p>
 * Successful responses for a lookup are cached at the local name server. Therefore a lookup requests are either
 * answered from cache (<code>CacheEntry</code>), or sent to an active replica for a name.
 * We obtain the set of active replicas also from the cache at local name server. Among active replicas, requests
 * are sent to the closest active replica. If no response is received until a timeout, requests are retransmitted
 * to the next closest active replica.
 * <p>
 * A local name server's cache may not have the set of active replicas for several reasons: (1) this could be the first
 * request for a name at this local name server and therefore no cache entry would exist for it. (2) the cache entry
 * may have been evicted due to replacement policy. (3) the set of active replicas changes over time, therefore
 * the cached set of active replicas may not be valid; A local name server learns this upon sending a request to
 * an invalid active replica, which generates a pre-defined error code in {@link edu.umass.cs.gns.packet.NSResponseCode}.
 * <p>
 * In all the above cases, a local name server obtains the set of active replicas for a name by requests the replica
 * controllers for that name. The replica controllers for a name are fixed and a local name server can compute the set of
 * replica controllers locally. This functionality is implemented in {@link edu.umass.cs.gns.localnameserver.RequestActivesTask}
 * and {@link edu.umass.cs.gns.localnameserver.PendingTasks}.
 * <p>
 * There is another way local name server updates the set of active replicas in its cache: the responses to lookups
 * also contain the active replicas within the same packet. This mechanism helps in scenarios when both the old
 * and the new set of active replicas contain some common name servers. If a local name server is not aware of a change
 * in the set of active replicas, but it sends a lookup to a name server that is a common member in both sets,
 * this mechanisms helps local name server learn of a change in the set of replicas.
 *
 * @see edu.umass.cs.gns.localnameserver.DNSRequestTask
 * @see edu.umass.cs.gns.localnameserver.DNSRequestInfo
 * @see edu.umass.cs.gns.packet.DNSPacket
 */
public class Lookup {

  private static Random random = new Random();


  public static void handlePacketLookupRequest(JSONObject json, DNSPacket dnsPacket)
          throws JSONException, UnknownHostException {

    LocalNameServer.incrementLookupRequest(dnsPacket.getGuid()); // important: used to count votes for names.
    DNSRequestTask queryTaskObject = new DNSRequestTask(
            dnsPacket,
            System.currentTimeMillis(),
            0,
            0,
            new HashSet<Integer>(), 0);
    long timeOut = StartLocalNameServer.queryTimeout;
    if (StartLocalNameServer.adaptiveTimeout) {
      timeOut = AdaptiveRetransmission.getTimeoutInterval(0);
    }

    LocalNameServer.getExecutorService().scheduleAtFixedRate(queryTaskObject, 0, timeOut, TimeUnit.MILLISECONDS);
    GNS.getLogger().finer("Scheduled this object.");
  }

  public static void handlePacketLookupResponse(JSONObject json, DNSPacket dnsPacket) throws JSONException {
    GNS.getLogger().fine("LNS-RecvdPkt\t" + json);
    GNS.getLogger().fine("Query-" + dnsPacket.getQueryId() + "\t"
            + System.currentTimeMillis() + "\t" + dnsPacket.getGuid() + "\tListener-response-enter");
    if (dnsPacket.isResponse() && !dnsPacket.containsAnyError()) {
      //Packet is a response and does not have a response error
      GNS.getLogger().finer("LNSListenerResponse: Received ResponseNum: "
              + (0) + " --> " + dnsPacket.toJSONObject().toString());
      //Match response to the query sent
      DNSRequestInfo query = LocalNameServer.removeDNSRequestInfo(dnsPacket.getQueryId());
      if (query == null) {
        // if there is none it means we already handled this request?
        return;
      }

      if (random.nextDouble() < StartLocalNameServer.outputSampleRate) {
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
      GNS.getLogger().fine(" Invalid Active Name Server.\tName\t" + dnsPacket.getGuid() + "\tRequest new actives.");
      LocalNameServer.invalidateActiveNameServer(dnsPacket.getGuid());

      // create objects to be passed to PendingTasks
      boolean firstInvalidActiveError = (query.numInvalidActiveError == 0);
      DNSRequestTask queryTaskObject = new DNSRequestTask(
              query.getIncomingPacket(),
              query.getLookupRecvdTime(), query.getId(),
              0,
              new HashSet<Integer>(), query.numInvalidActiveError + 1);
      String failureMsg = DNSRequestInfo.getFailureLogMessage(0, dnsPacket.getKey(), dnsPacket.getGuid(),
              0, query.getLookupRecvdTime(), query.numInvalidActiveError + 1, -1, new HashSet<Integer>());


      PendingTasks.addToPendingRequests(query.getqName(), queryTaskObject, StartLocalNameServer.queryTimeout,
              getErrorPacket(query.getIncomingPacket()), failureMsg, firstInvalidActiveError);

      GNS.getLogger().fine(" Scheduled lookup task.");

    } else { // other types of errors, forward error response to client
      GNS.getLogger().fine("Forwarding incoming error packet for query " + query.getIncomingPacket().getQueryId() +
              ": " + dnsPacket.toJSONObject());
      // set the correct id for the client
      dnsPacket.getHeader().setId(query.getIncomingPacket().getQueryId());
      Intercessor.handleIncomingPackets(dnsPacket.toJSONObject());
      // this might need updating... not sure how it's used though so we'll punt
      GNS.getStatLogger().fine(DNSRequestInfo.getFailureLogMessage(0, dnsPacket.getKey(), dnsPacket.getGuid(),
              0, query.getLookupRecvdTime(), query.numInvalidActiveError, -1, new HashSet<Integer>()));

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
              query.getIncomingPacket().getKey(), returnValue, TTL, new HashSet<Integer>());
      Intercessor.handleIncomingPackets(outgoingPacket.toJSONObject());
    } catch (Exception e) {
      GNS.getLogger().severe("Problem converting packet to JSON: " + e);
    }
  }

  // why does this totally ignore the error code that exists in the incoming packet?
  public static JSONObject getErrorPacket(DNSPacket dnsPacketIn) {
    try {
      DNSPacket dnsPacketOut = new DNSPacket(dnsPacketIn.toJSONObjectQuestion());
      dnsPacketOut.getHeader().setResponseCode(NSResponseCode.ERROR);
      dnsPacketOut.getHeader().setQRCode(DNSRecordType.RESPONSE);
      return dnsPacketOut.toJSONObject();
    } catch (JSONException e) {
      GNS.getLogger().severe("Problem converting packet to JSON: " + e);
    }
    return null;
  }
}
