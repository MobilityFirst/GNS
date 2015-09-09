/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.gnsApp.clientCommandProcessor.demultSupport;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.gnsApp.packet.DNSPacket;
import edu.umass.cs.utils.DelayProfiler;
import org.json.JSONException;
import org.json.JSONObject;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Random;

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
 * an invalid active replica, which generates a pre-defined error code in {@link edu.umass.cs.gns.util.NSResponseCode}.
 * <p>
 * In all the above cases, a local name server obtains the set of active replicas for a name by requesting the (fixed)
 * replica controllers for that name. This functionality is implemented in {@link edu.umass.cs.gns.gnsApp.clientCommandProcessor.demultSupport.RequestActivesTask}
 * and {@link edu.umass.cs.gns.gnsApp.clientCommandProcessor.demultSupport.PendingTasks}.
 * <p>
 * There is another way local name server updates the set of active replicas in its cache: the responses to lookups
 * also contain the active replicas within the same packet. This mechanism helps in scenarios when both the old
 * and the new set of active replicas contain some common name servers. If a local name server is not aware of a change
 * in the set of active replicas, but it sends a lookup to a name server that is a common member in both sets,
 * this mechanisms helps local name server learn of a change in the set of replicas.
 *
 * @see SendDNSRequestTask
 * @see edu.umass.cs.gns.gnsApp.clientCommandProcessor.demultSupport.DNSRequestInfo
 * @see edu.umass.cs.gns.gnsApp.packet.DNSPacket
 *
 * @author abhigyan
 */
public class Lookup {

  private static Random random = new Random();

  public static void handlePacketLookupRequest(JSONObject json, DNSPacket<String> incomingPacket, ClientRequestHandlerInterface handler)
          throws JSONException, UnknownHostException {
    long startTime = System.currentTimeMillis();
    if (handler.getParameters().isDebugMode()) {
      GNS.getLogger().info(">>>>>>>>>>>>>>>>>>>>>>> CCP DNS Request:" + json);
    }
    int ccpReqID = handler.getUniqueRequestID();
    DNSRequestInfo<String> requestInfo = new DNSRequestInfo<String>(ccpReqID, incomingPacket.getGuid(), -1, incomingPacket, handler.getGnsNodeConfig());
    handler.addRequestInfo(ccpReqID, requestInfo);
    int clientQueryID = incomingPacket.getQueryId(); // BS: save the value because we reuse the field in the packet
    incomingPacket.setCCPAddress(handler.getNodeAddress());
    incomingPacket.getHeader().setId(ccpReqID);
    JSONObject outgoingJSON = incomingPacket.toJSONObjectQuestion();
    incomingPacket.getHeader().setId(clientQueryID); // BS: restore the value because we reuse the field in the packet
    DelayProfiler.updateDelay("handlePacketLookupRequestSetup", startTime);
    handler.getApp().handleRequest(new DNSPacket<String>(outgoingJSON, handler.getGnsNodeConfig()));
    DelayProfiler.updateDelay("handlePacketLookupRequest", startTime);
    //handler.sendToNS(outgoingJSON, handler.getActiveReplicaID());
  }

  public static void handlePacketLookupResponse(JSONObject json, DNSPacket<String> dnsPacket, ClientRequestHandlerInterface handler) throws JSONException {
    if (handler.getParameters().isDebugMode()) {
      GNS.getLogger().info(">>>>>>>>>>>>>>>>>>>>>>> CCP DNS Response" + json);
    }
    if (dnsPacket.isResponse() && !dnsPacket.containsAnyError()) {
      //Packet is a response and does not have a response error
      //Match response to the query sent
      @SuppressWarnings("unchecked")
      DNSRequestInfo<String> requestInfo = (DNSRequestInfo<String>) handler.removeRequestInfo(dnsPacket.getQueryId());
      if (requestInfo == null) {
        // if there is none it means we already handled this request?
        return;
      }
      requestInfo.setSuccess(true);
      requestInfo.setFinishTime();

      DelayProfiler.updateDelay("dnsRequest", requestInfo.getStartTime());
      // send response to user right now.
      try {
        DNSPacket<String> outgoingPacket = new DNSPacket<>(requestInfo.getIncomingPacket().getSourceId(),
                requestInfo.getIncomingPacket().getHeader().getId(),
                requestInfo.getIncomingPacket().getGuid(),
                requestInfo.getIncomingPacket().getKey(), requestInfo.getIncomingPacket().getKeys(),
                dnsPacket.getRecordValue(), dnsPacket.getTTL(), new HashSet<Integer>());
        outgoingPacket.setResponder(dnsPacket.getResponder());
        //outgoingPacket.setLookupTime(dnsPacket.getLookupTime());
        sendDNSResponseBackToSource(outgoingPacket, handler);
      } catch (JSONException e) {
        GNS.getLogger().severe("Problem converting packet to JSON: " + e);
      }

      //sendReplyToUser(requestInfo, dnsPacket.getRecordValue(), dnsPacket.getTTL(), dnsPacket.getResponder(), handler);
    }
  }

  public static void handlePacketLookupErrorResponse(JSONObject jsonObject, DNSPacket<String> dnsPacket, ClientRequestHandlerInterface handler) throws JSONException {

    if (handler.getParameters().isDebugMode()) {
      GNS.getLogger().info("Recvd Lookup Error Response" + jsonObject);
    }
    @SuppressWarnings("unchecked")
    DNSRequestInfo<String> requestInfo = (DNSRequestInfo<String>) handler.removeRequestInfo(dnsPacket.getQueryId());
    if (requestInfo == null) {
      GNS.getLogger().severe("No entry in queryTransmittedMap. QueryID:" + dnsPacket.getQueryId());
      return;
    }
    requestInfo.setSuccess(false);
    requestInfo.setFinishTime();
    //requestInfo.addEventCode(LNSEventCode.OTHER_ERROR);
    if (handler.getParameters().isDebugMode()) {
      GNS.getLogger().info("Forwarding incoming error packet for query "
              + requestInfo.getIncomingPacket().getQueryId() + ": " + dnsPacket.toJSONObject());
    }
    // set the correct id for the client
    dnsPacket.getHeader().setId(requestInfo.getIncomingPacket().getQueryId());
    sendDNSResponseBackToSource(dnsPacket, handler);
      //GNS.getStatLogger().fine(requestInfo.getLogString());

    //}
  }

  /**
   * Handles the returning of packets back to the appropriate source (local intercessor or another NameServer).
   *
   * @throws JSONException
   */
  public static void sendDNSResponseBackToSource(DNSPacket<String> packet, ClientRequestHandlerInterface handler) throws JSONException {
    if (packet.getSourceId() == null) {
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().info("Sending back to Intercessor: " + packet.toJSONObject().toString());
      }
      handler.getIntercessor().handleIncomingPacket(packet.toJSONObject());
    } else {
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().info("Sending back to Node " + packet.getSourceId() + ":" + packet.toJSONObject().toString());
      }
      handler.sendToNS(packet.toJSONObject(), packet.getSourceId());
    }
  }

}
