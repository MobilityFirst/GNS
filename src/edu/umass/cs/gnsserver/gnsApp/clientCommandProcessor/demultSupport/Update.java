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
import edu.umass.cs.gnsserver.gnsApp.packet.ConfirmUpdatePacket;
import edu.umass.cs.gnsserver.gnsApp.packet.UpdatePacket;
import edu.umass.cs.gnsserver.gnsApp.NSResponseCode;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.UnknownHostException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Class contains a few static methods for handling update requests from clients as well responses to updates from
 * name servers.
 * Most functionality for handling updates from clients is implemented in
 * <code>SendUpdatesTask</code>. So also refer to its documentation.
 * <p>
 * An update request is sent to an active replica of a name.
 * Refer to documentation in {@link edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.Lookup} to know how a local name server obtains
 * the set of active replicas. Like other requests, updates are also retransmitted to a different name server
 * if no confirmation is received until a timeout value.
 * <p>
 *
 * @author abhigyan
 */
public class Update {

  private static Random r = new Random();

  /**
   * Handles incoming Update packets.
   *
   * @param json
   * @param handler
   * @throws JSONException
   * @throws UnknownHostException
   */
  public static void handlePacketUpdate(JSONObject json, ClientRequestHandlerInterface handler)
          throws JSONException, UnknownHostException {

    UpdatePacket<String> updatePacket = new UpdatePacket<String>(json, handler.getGnsNodeConfig());
    if (handler.getParameters().isDebugMode()) {
      GNS.getLogger().fine("UPDATE PACKET RECVD: " + json.toString());
    }
    int ccpRequestID = handler.getUniqueRequestID();
    UpdateInfo<String> info = new UpdateInfo<String>(ccpRequestID, updatePacket.getName(), null, updatePacket, handler);
    handler.addRequestInfo(ccpRequestID, info);
    //handler.incrementUpdateRequest(updatePacket.getName()); // important: used to count votes for names.
    if (!handler.reallySendUpdateToReplica()) {
      handlePacketLocally(ccpRequestID, handler, updatePacket, handler.getActiveReplicaID());
    } else {
      // otherwise send it to the colocated replica, but still with retransmission
      SendUpdatesTask updateTask = new SendUpdatesTask(ccpRequestID, handler, updatePacket,
              handler.getActiveReplicaID(), handler.reallySendUpdateToReplica());
      handler.getExecutorService().scheduleAtFixedRate(updateTask, 0, handler.getParameters().getQueryTimeout(), TimeUnit.MILLISECONDS);
    }
  }

  private static void handlePacketLocally(int ccpReqID, ClientRequestHandlerInterface handler,
          UpdatePacket<String> updatePacket, String nameServerID) {
    handler.getApp().execute(makeNewUpdatePacket(ccpReqID, handler, updatePacket, nameServerID));
  }

  /**
   * Makes an update packet to send to an AR.
   * Puts all the right stuff in the right places.
   *
   * @param ccpReqID
   * @param handler
   * @param updatePacket
   * @param nameServerID
   * @return an UpdatePacket
   */
  public static UpdatePacket<String> makeNewUpdatePacket(int ccpReqID, ClientRequestHandlerInterface handler,
          UpdatePacket<String> updatePacket, String nameServerID) {
    UpdatePacket<String> pkt = new UpdatePacket<String>(
            updatePacket.getSourceId(), // DON'T JUST USE -1!!!!!! THIS IS IMPORTANT!!!!
            updatePacket.getRequestID(),
            ccpReqID, // the id used by the CCP (that would be us here)
            updatePacket.getName(),
            updatePacket.getKey(),
            updatePacket.getUpdateValue(),
            updatePacket.getOldValue(),
            updatePacket.getArgument(),
            updatePacket.getUserJSON(),
            updatePacket.getOperation(),
            handler.getNodeAddress(),
            nameServerID, updatePacket.getTTL(),
            //signature info
            updatePacket.getAccessor(),
            updatePacket.getSignature(),
            updatePacket.getMessage());
    return pkt;
  }

  /**
   * Handles incoming Update confirmation packets.
   *
   * @param json
   * @param handler
   * @throws UnknownHostException
   * @throws JSONException
   */
  public static void handlePacketConfirmUpdate(JSONObject json, ClientRequestHandlerInterface handler) throws UnknownHostException, JSONException {
    ConfirmUpdatePacket<String> confirmPkt = new ConfirmUpdatePacket<String>(json, handler.getGnsNodeConfig());

    if (handler.getParameters().isDebugMode()) {
      GNS.getLogger().fine("ConfirmUpdate recvd: ResponseNum: " + " --> " + confirmPkt);
    }
    if (confirmPkt.isSuccess()) {
      // we are removing request info as processing for this request is complete
      @SuppressWarnings("unchecked")
      UpdateInfo<String> updateInfo = (UpdateInfo<String>) handler.removeRequestInfo(confirmPkt.getCCPRequestID());
      // if update info isn't available, we cant do anything. probably response is overly delayed and an error response
      // has already been sent to client.
      if (updateInfo == null) {
        if (handler.getParameters().isDebugMode()) {
          GNS.getLogger().warning("Update info not found. quitting. SUCCESS update.  " + confirmPkt);
        }
        return;
      }
      // send the confirmation back to the originator of the update
      Update.sendConfirmUpdatePacketBackToSource(confirmPkt, handler);
      updateInfo.setSuccess(confirmPkt.isSuccess());
      updateInfo.setFinishTime();
      // FIXME: Verify that this branch is never being called.
    } else if (confirmPkt.getResponseCode().equals(NSResponseCode.ERROR_INVALID_ACTIVE_NAMESERVER)) {
      // NOTE: we are NOT removing request info as processing for this request is still ongoing
      @SuppressWarnings("unchecked")
      UpdateInfo<String> updateInfo = (UpdateInfo<String>) handler.getRequestInfo(confirmPkt.getCCPRequestID());
      if (updateInfo == null) {
        if (handler.getParameters().isDebugMode()) {
          GNS.getLogger().warning("Update info not found. quitting. INVALID_ACTIVE_ERROR update " + confirmPkt);
        }
        return;
      }
      // if error type is invalid active error, we fetch a fresh set of actives from replica controllers and try again
      handleInvalidActiveError(updateInfo, handler);
    } else { // In all other types of errors, we immediately send response to client.
      // we are removing request info as processing for this request is complete
      @SuppressWarnings("unchecked")
      UpdateInfo<String> updateInfo = (UpdateInfo<String>) handler.removeRequestInfo(confirmPkt.getCCPRequestID());
      if (updateInfo == null) {
        if (handler.getParameters().isDebugMode()) {
          GNS.getLogger().warning("Update info not found. quitting.  ERROR update. " + confirmPkt);
        }
        return;
      }
      Update.sendConfirmUpdatePacketBackToSource(confirmPkt, handler);
      updateInfo.setSuccess(confirmPkt.isSuccess());
      updateInfo.setFinishTime();
    }
  }

  /**
   * Update request reached invalid active replica, so obtain a new set of actives and send request again.
   *
   * @param updateInfo state for this request stored at local name server
   * @throws JSONException
   */
  private static void handleInvalidActiveError(UpdateInfo<String> updateInfo, ClientRequestHandlerInterface handler) throws JSONException {
    if (handler.getParameters().isDebugMode()) {
      GNS.getLogger().fine("\tInvalid Active Name Server.\tName\t"
              + updateInfo.getName() + "\tRequest new actives.\t");
    }

    @SuppressWarnings("unchecked")
    UpdatePacket<String> updatePacket = (UpdatePacket<String>) updateInfo.getUpdatePacket();

    // clear out current cache
    //handler.invalidateActiveNameServer(updateInfo.getName());
    // create objects that must be passed to PendingTasks
    SendUpdatesTask task = new SendUpdatesTask(updateInfo.getCCPReqID(), handler, updatePacket, null);
//    ConfirmUpdatePacket<String> failPacket = ConfirmUpdatePacket.createFailPacket(updatePacket, NSResponseCode.ERROR);

    PendingTasks.addToPendingRequests(updateInfo, task, handler.getParameters().getQueryTimeout(), handler);
  }

  /**
   * Checks the returnTo slot of the packet and sends the confirmation packet back to
   * the correct destination. This can be null meaning that the original request
   * originated at a standard client - the packet is then sent to the Intercessor which will
   * pass the packet back to the client. If the returnTo is some other NodeId the packet will be
   * send back to that NameServer node (which is acting like a client in this case).
   *
   * This method is public because it also gets called from Add/Remove methods.
   *
   * @param packet
   * @param handler
   * @throws JSONException
   */
  public static void sendConfirmUpdatePacketBackToSource(ConfirmUpdatePacket<String> packet, ClientRequestHandlerInterface handler) throws JSONException {
    if (packet.getReturnTo() == null) {
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().info("Sending back to Intercessor: " + packet.toJSONObject().toString());
      }

      handler.getIntercessor().handleIncomingPacket(packet.toJSONObject());
    } else {
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().info("Sending back to Node " + packet.getReturnTo().toString()
                + ":" + packet.toJSONObject().toString());
      }

      try {
        handler.sendToNS(packet.toJSONObject(), packet.getReturnTo());
      } catch (JSONException e) {
        GNS.getLogger().severe("Unable to send packet back to NS: " + e);
      }
    }
  }
}
