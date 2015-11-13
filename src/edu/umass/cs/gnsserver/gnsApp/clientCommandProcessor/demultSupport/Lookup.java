/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Abhigyan Sharma, Westy
 *
 */
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport;

import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.gnsApp.packet.DNSPacket;
import edu.umass.cs.utils.DelayProfiler;
import org.json.JSONException;
import org.json.JSONObject;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Random;

/**
 * Class contains a few static methods for handling lookup requests from 
 * the LNS as well responses to lookups from name servers.
 *
 * @see edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.DNSRequestInfo
 * @see edu.umass.cs.gnsserver.gnsApp.packet.DNSPacket
 *
 * @author abhigyan
 */
public class Lookup {

  private static Random random = new Random();

  /**
   *
   * @param json
   * @param incomingPacket
   * @param handler
   * @throws JSONException
   * @throws UnknownHostException
   */
  public static void handlePacketLookupRequest(DNSPacket<String> incomingPacket, ClientRequestHandlerInterface handler)
          throws JSONException, UnknownHostException {
    long startTime = System.currentTimeMillis();
    if (handler.getParameters().isDebugMode()) {
      //GNS.getLogger().info(">>>>>>>>>>>>>>>>>>>>>>> CCP DNS Request:" + incomingPacket.toString());
      GNS.getLogger().info(">>>>>>>>>>>>>>>>>>>>>>> CCP DNS Request:" + incomingPacket.toReasonableString());
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
    handler.getApp().execute(new DNSPacket<String>(outgoingJSON, handler.getGnsNodeConfig()));
    DelayProfiler.updateDelay("handlePacketLookupRequest", startTime);
  }

  /**
   *
   * @param json
   * @param dnsPacket
   * @param handler
   * @throws JSONException
   */
  public static void handlePacketLookupResponse(DNSPacket<String> dnsPacket, ClientRequestHandlerInterface handler) throws JSONException {
    if (handler.getParameters().isDebugMode()) {
      //GNS.getLogger().info(">>>>>>>>>>>>>>>>>>>>>>> CCP DNS Response" + dnsPacket.toString());
      GNS.getLogger().info(">>>>>>>>>>>>>>>>>>>>>>> CCP DNS Response" + dnsPacket.toReasonableString());
    }
    if (dnsPacket.isResponse() && !dnsPacket.containsAnyError()) {
      //Packet is a response and does not have a response error
      //Match response to the query sent
      @SuppressWarnings("unchecked")
      DNSRequestInfo<String> requestInfo = (DNSRequestInfo<String>) 
              handler.removeRequestInfo(dnsPacket.getQueryId());
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
        sendDNSResponseBackToSource(outgoingPacket, handler);
      } catch (JSONException e) {
        GNS.getLogger().severe("Problem converting packet to JSON: " + e);
      }
    }
  }

  /**
   * Handles the returning of error packets.
   * 
   * @param jsonObject
   * @param dnsPacket
   * @param handler
   * @throws JSONException
   */
  public static void handlePacketLookupErrorResponse(DNSPacket<String> dnsPacket, ClientRequestHandlerInterface handler) throws JSONException {

    if (handler.getParameters().isDebugMode()) {
      GNS.getLogger().info("Recvd Lookup Error Response" + dnsPacket.toReasonableString());
    }
    @SuppressWarnings("unchecked")
    DNSRequestInfo<String> requestInfo = (DNSRequestInfo<String>) handler.removeRequestInfo(dnsPacket.getQueryId());
    if (requestInfo == null) {
      GNS.getLogger().severe("No entry in queryTransmittedMap. QueryID:" + dnsPacket.getQueryId());
      return;
    }
    requestInfo.setSuccess(false);
    requestInfo.setFinishTime();
    if (handler.getParameters().isDebugMode()) {
      GNS.getLogger().info("Forwarding incoming error packet for query "
              + requestInfo.getIncomingPacket().getQueryId() + ": " + dnsPacket.toJSONObject());
    }
    // set the correct id for the client
    dnsPacket.getHeader().setId(requestInfo.getIncomingPacket().getQueryId());
    sendDNSResponseBackToSource(dnsPacket, handler);
  }

  /**
   * Handles the returning of packets back to the appropriate source (local intercessor or another NameServer).
   *
   * @param packet
   * @param handler
   * @throws JSONException
   */
  public static void sendDNSResponseBackToSource(DNSPacket<String> packet, ClientRequestHandlerInterface handler) throws JSONException {
    if (packet.getSourceId() == null) {
      if (handler.getParameters().isDebugMode()) {
        //GNS.getLogger().info("Sending back to Intercessor: " + packet.toJSONObject().toString());
        GNS.getLogger().info("Sending back to Intercessor: " + packet.toJSONObject().toReasonableString());
      }
      handler.getIntercessor().handleIncomingPacket(packet.toJSONObject());
    } else {
      if (handler.getParameters().isDebugMode()) {
        //GNS.getLogger().info("Sending back to Node " + packet.getSourceId() + ":" + packet.toJSONObject().toString());
        GNS.getLogger().info("Sending back to Node " + packet.getSourceId() + ":" + packet.toJSONObject().toReasonableString());
      }
      handler.sendToNS(packet.toJSONObject(), packet.getSourceId());
    }
  }

}
