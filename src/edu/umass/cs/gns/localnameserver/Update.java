/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.clientsupport.Intercessor;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nsdesign.packet.ConfirmUpdatePacket;
import edu.umass.cs.gns.nsdesign.packet.DNSPacket;
import edu.umass.cs.gns.nsdesign.packet.UpdatePacket;
import edu.umass.cs.gns.util.NSResponseCode;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Class contains a few static methods for handling update requests from clients as well responses to updates from
 * name servers. Most functionality for handling updates from clients is implemented in
 * <code>SendUpdatesTask</code>. So also refer to its documentation.
 * <p>
 * An update request is sent to an active replica of a name, except for a special type of update called upsert (update + insert).
 * Refer to documentation in {@link edu.umass.cs.gns.localnameserver.Lookup} to know how a local name server obtains
 * the set of active replicas. Like other requests, updates are also retransmitted to a different name server
 * if no confirmation is received until a timeout value.
 * <p>
 * An upsert request may create a new name record, unlike an update which modifies an existing name record.
 * Becasue addition of a name is done by replica controllers, we send an upserts to replica controllers.
 * If upsert request is for an already existing name, it is handled like an update. To this end, replica controllers
 * will forward the request to active replicas.
 * <p>
 * @author abhigyan
 */
public class Update {

  private static Random r = new Random();

  public static void handlePacketUpdate(JSONObject json)
          throws JSONException, UnknownHostException {

    UpdatePacket updateAddressPacket = new UpdatePacket(json);

    GNS.getLogger().info("UPDATE PACKET RECVD: " + json.toString());

    LocalNameServer.incrementUpdateRequest(updateAddressPacket.getName()); // important: used to count votes for names.
    SendUpdatesTask updateTask = new SendUpdatesTask(updateAddressPacket,
            System.currentTimeMillis(), new HashSet<Integer>(), 0);
    LocalNameServer.getExecutorService().scheduleAtFixedRate(updateTask, 0, StartLocalNameServer.queryTimeout,
            TimeUnit.MILLISECONDS);
  }

  public static void handlePacketConfirmUpdate(JSONObject json) throws UnknownHostException, JSONException {
    ConfirmUpdatePacket confirmPkt = new ConfirmUpdatePacket(json);

    if (StartLocalNameServer.debugMode) {
      GNS.getLogger().fine("ConfirmUpdate recvd: ResponseNum: " + " --> " + confirmPkt.toString());
    }

    // if update info isnt available, we cant do anything.
    UpdateInfo updateInfo = LocalNameServer.removeUpdateInfo(confirmPkt.getLNSRequestID());
    if (updateInfo == null) {
      GNS.getLogger().warning("Update info not found. quitting.  " + confirmPkt);
      return;
    }

    if (confirmPkt.isSuccess()) {
      // update the cache BEFORE we send back the confirmation
      LocalNameServer.updateCacheEntry(confirmPkt, updateInfo.getName(), null);
      // send the confirmation back to the originator of the update
      Update.sendConfirmUpdatePacketBackToSource(confirmPkt);
      // instrumentation?
      if (r.nextDouble() <= StartLocalNameServer.outputSampleRate) {
        GNS.getStatLogger().info(updateInfo.getUpdateStats(confirmPkt));
      }
    } else if (confirmPkt.getResponseCode().equals(NSResponseCode.ERROR_INVALID_ACTIVE_NAMESERVER)) {
      // if error type is invalid active error, we fetch a fresh set of actives from replica controllers and try again
      handleInvalidActiveError(updateInfo);
    } else { // In all other types of errors, we immediately send response to client.
      Update.sendConfirmUpdatePacketBackToSource(confirmPkt);
      if (r.nextDouble() <= StartLocalNameServer.outputSampleRate) {
        GNS.getStatLogger().info(updateInfo.getUpdateStats(confirmPkt));
      }

    }
  }

  /**
   * Update request reached invalid active replica, so obtain a new set of actives and send request again.
   *
   * @param updateInfo state for this request stored at local name server
   * @throws JSONException
   */
  private static void handleInvalidActiveError(UpdateInfo updateInfo) throws JSONException {
    if (StartLocalNameServer.debugMode) {
      GNS.getLogger().fine("\tInvalid Active Name Server.\tName\t" + updateInfo.getName() + "\tRequest new actives.\t");
    }

    UpdatePacket updatePacket = (UpdatePacket) updateInfo.getUpdatePacket();

    // clear out current cache
    LocalNameServer.invalidateActiveNameServer(updateInfo.getName());

    // create objects that must be passed to PendingTasks
    SendUpdatesTask task = new SendUpdatesTask(updatePacket, updateInfo.getSendTime(), new HashSet<Integer>(),
            updateInfo.getNumInvalidActiveError() + 1);
    String failedStats = UpdateInfo.getUpdateFailedStats(updateInfo.getName(), new HashSet<Integer>(),
            LocalNameServer.getNodeID(), updatePacket.getRequestID(), updateInfo.getSendTime(),
            updateInfo.getNumInvalidActiveError() + 1, -1, updatePacket.getType());
    ConfirmUpdatePacket failPacket = ConfirmUpdatePacket.createFailPacket(updatePacket, NSResponseCode.ERROR);

    boolean firstInvalidActiveError = (updateInfo.getNumInvalidActiveError() == 0);

    PendingTasks.addToPendingRequests(updateInfo.getName(), task, StartLocalNameServer.queryTimeout,
            failPacket.toJSONObject(), failedStats, firstInvalidActiveError);
  }

  public static void sendConfirmUpdatePacketBackToSource(ConfirmUpdatePacket packet) throws JSONException {
    if (packet.getReturnTo() == DNSPacket.LOCAL_SOURCE_ID) {
      if (StartLocalNameServer.debugMode) {
        GNS.getLogger().info("Sending back to Intercessor: " + packet.toJSONObject().toString());
      }
      Intercessor.handleIncomingPackets(packet.toJSONObject());
    } else {
      if (StartLocalNameServer.debugMode) {
        GNS.getLogger().info("Sending back to Node " + packet.getReturnTo() + ":" + packet.toJSONObject().toString());
      }
      try {
        LocalNameServer.sendToNS(packet.toJSONObject(), packet.getReturnTo());
//        Packet.sendTCPPacket(LocalNameServer.getGnsNodeConfig(), packet.toJSONObject(),
//                packet.getReturnTo(), GNS.PortType.NS_TCP_PORT);
//      } catch (IOException e) {
//        GNS.getLogger().severe("Unable to send packet back to NS: " + e);
      } catch (JSONException e) {
        GNS.getLogger().severe("Unable to send packet back to NS: " + e);
      }
    }
  }
}
