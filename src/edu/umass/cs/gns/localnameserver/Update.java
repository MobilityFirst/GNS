/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.packet.ConfirmUpdatePacket;
import edu.umass.cs.gns.nsdesign.packet.DNSPacket;
import edu.umass.cs.gns.nsdesign.packet.UpdatePacket;
import edu.umass.cs.gns.util.NSResponseCode;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.UnknownHostException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Class contains a few static methods for handling update requests from clients as well responses to updates from
 * name servers. Most functionality for handling updates from clients is implemented in
 * <code>SendUpdatesTask</code>. So also refer to its documentation.
 * <p>
 * An update request is sent to an active replica of a name.
 * Refer to documentation in {@link edu.umass.cs.gns.localnameserver.Lookup} to know how a local name server obtains
 * the set of active replicas. Like other requests, updates are also retransmitted to a different name server
 * if no confirmation is received until a timeout value.
 * <p>
 *
 * @author abhigyan
 */
public class Update {

  private static Random r = new Random();

  public static void handlePacketUpdate(JSONObject json, ClientRequestHandlerInterface handler)
          throws JSONException, UnknownHostException {

    UpdatePacket updatePacket = new UpdatePacket(json);
    if (handler.getParameters().isDebugMode()) GNS.getLogger().fine("UPDATE PACKET RECVD: " + json.toString());
    int lnsReqID = handler.getUniqueRequestID();
    UpdateInfo info = new UpdateInfo(lnsReqID, updatePacket.getName(), System.currentTimeMillis(),
            -1, updatePacket);
    handler.addRequestInfo(lnsReqID, info);
    handler.incrementUpdateRequest(updatePacket.getName()); // important: used to count votes for names.
    SendUpdatesTask updateTask = new SendUpdatesTask(lnsReqID, handler, updatePacket);
    handler.getExecutorService().scheduleAtFixedRate(updateTask, 0, handler.getParameters().getQueryTimeout(), TimeUnit.MILLISECONDS);
  }

  public static void handlePacketConfirmUpdate(JSONObject json, ClientRequestHandlerInterface handler) throws UnknownHostException, JSONException {
    ConfirmUpdatePacket confirmPkt = new ConfirmUpdatePacket(json);

    if (handler.getParameters().isDebugMode()) GNS.getLogger().fine("ConfirmUpdate recvd: ResponseNum: " + " --> " + confirmPkt);
    if (confirmPkt.isSuccess()) {
      // we are removing request info as processing for this request is complete
      UpdateInfo updateInfo = (UpdateInfo) handler.removeRequestInfo(confirmPkt.getLNSRequestID());
      // if update info isn't available, we cant do anything. probably response is overly delayed and an error response
      // has already been sent to client.
      if (updateInfo == null) {
        if (handler.getParameters().isDebugMode()) GNS.getLogger().warning("Update info not found. quitting. SUCCESS update.  " + confirmPkt);
        return;
      }
      // update the cache BEFORE we send back the confirmation
      handler.updateCacheEntry(confirmPkt, updateInfo.getName(), null);
      // send the confirmation back to the originator of the update
      Update.sendConfirmUpdatePacketBackToSource(confirmPkt, handler);
      updateInfo.setSuccess(confirmPkt.isSuccess());
      updateInfo.setFinishTime();
      updateInfo.addEventCode(LNSEventCode.SUCCESS);
      // instrumentation?
      if (r.nextDouble() <= handler.getParameters().getOutputSampleRate()) {
        GNS.getStatLogger().info(updateInfo.getLogString());
      }
    } else if (confirmPkt.getResponseCode().equals(NSResponseCode.ERROR_INVALID_ACTIVE_NAMESERVER)) {
      // NOTE: we are NOT removing request info as processing for this request is still ongoing
      UpdateInfo updateInfo = (UpdateInfo) handler.getRequestInfo(confirmPkt.getLNSRequestID());
      if (updateInfo == null) {
        if (handler.getParameters().isDebugMode()) GNS.getLogger().warning("Update info not found. quitting. INVALID_ACTIVE_ERROR update " + confirmPkt);
        return;
      }
      updateInfo.addEventCode(LNSEventCode.INVALID_ACTIVE_ERROR);
      // if error type is invalid active error, we fetch a fresh set of actives from replica controllers and try again
      handleInvalidActiveError(updateInfo, handler);
    } else { // In all other types of errors, we immediately send response to client.
      // we are removing request info as processing for this request is complete
      UpdateInfo updateInfo = (UpdateInfo) handler.removeRequestInfo(confirmPkt.getLNSRequestID());
      if (updateInfo == null) {
        if (handler.getParameters().isDebugMode()) GNS.getLogger().warning("Update info not found. quitting.  ERROR update. " + confirmPkt);
        return;
      }
      Update.sendConfirmUpdatePacketBackToSource(confirmPkt, handler);
      updateInfo.setSuccess(confirmPkt.isSuccess());
      updateInfo.setFinishTime();
      updateInfo.addEventCode(LNSEventCode.OTHER_ERROR);
      if (r.nextDouble() <= handler.getParameters().getOutputSampleRate()) {
        GNS.getStatLogger().info(updateInfo.getLogString());
      }
    }
  }

  /**
   * Update request reached invalid active replica, so obtain a new set of actives and send request again.
   *
   * @param updateInfo state for this request stored at local name server
   * @throws JSONException
   */
  private static void handleInvalidActiveError(UpdateInfo updateInfo, ClientRequestHandlerInterface handler) throws JSONException {
    if (handler.getParameters().isDebugMode()) GNS.getLogger().fine("\tInvalid Active Name Server.\tName\t" + 
            updateInfo.getName() + "\tRequest new actives.\t");
    
    UpdatePacket updatePacket = (UpdatePacket) updateInfo.getUpdatePacket();

    // clear out current cache
    handler.invalidateActiveNameServer(updateInfo.getName());
    // create objects that must be passed to PendingTasks
    SendUpdatesTask task = new SendUpdatesTask(updateInfo.getLnsReqID(), handler, updatePacket);
//    ConfirmUpdatePacket failPacket = ConfirmUpdatePacket.createFailPacket(updatePacket, NSResponseCode.ERROR);

    PendingTasks.addToPendingRequests(updateInfo, task, handler.getParameters().getQueryTimeout());
  }

  public static void sendConfirmUpdatePacketBackToSource(ConfirmUpdatePacket packet, ClientRequestHandlerInterface handler) throws JSONException {
    if (packet.getReturnTo() == DNSPacket.LOCAL_SOURCE_ID) {
      if (handler.getParameters().isDebugMode()) GNS.getLogger().fine("Sending back to Intercessor: " + packet.toJSONObject().toString());
      
      LocalNameServer.getIntercessor().handleIncomingPacket(packet.toJSONObject());
    } else {
      if (handler.getParameters().isDebugMode()) GNS.getLogger().fine("Sending back to Node " + packet.getReturnTo() + 
              ":" + packet.toJSONObject().toString());
      
      try {
        handler.sendToNS(packet.toJSONObject(), packet.getReturnTo());
      } catch (JSONException e) {
        GNS.getLogger().severe("Unable to send packet back to NS: " + e);
      }
    }
  }
}
