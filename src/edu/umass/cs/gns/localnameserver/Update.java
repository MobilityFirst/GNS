/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.clientsupport.Intercessor;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.packet.ConfirmUpdateLNSPacket;
import edu.umass.cs.gns.packet.NSResponseCode;
import edu.umass.cs.gns.packet.UpdateAddressPacket;
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
 *
 */
public class Update {

  private static Random r = new Random();


  public static void handlePacketUpdateAddressLNS(JSONObject json)
          throws JSONException, UnknownHostException {

    UpdateAddressPacket updateAddressPacket = new UpdateAddressPacket(json);

    GNS.getLogger().fine(" UPDATE PACKET RECVD. Operation: " + updateAddressPacket.getOperation());

    if (updateAddressPacket.getOperation().isUpsert()) {
      AddRemove.handleUpsert(updateAddressPacket);
    } else {
      LocalNameServer.incrementUpdateRequest(updateAddressPacket.getName()); // important: used to count votes for names.
      SendUpdatesTask updateTask = new SendUpdatesTask(updateAddressPacket,
              System.currentTimeMillis(), new HashSet<Integer>(), 0);
      LocalNameServer.getExecutorService().scheduleAtFixedRate(updateTask, 0, StartLocalNameServer.queryTimeout, TimeUnit.MILLISECONDS);
    }
  }

  public static void handlePacketConfirmUpdateLNS(JSONObject json) throws UnknownHostException, JSONException {
    ConfirmUpdateLNSPacket confirmPkt = new ConfirmUpdateLNSPacket(json);

    if (StartLocalNameServer.debugMode) {
      GNS.getLogger().fine("ConfirmUpdateLNS recvd: ResponseNum: " + " --> " + confirmPkt.toString());
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
        GNS.getLogger().fine("LNSListenerUpdate CONFIRM UPDATE (ns " + LocalNameServer.getNodeID() + ") to "
                + " : " + json.toString());
        Intercessor.handleIncomingPackets(json);
        // instrumentation?
        if (r.nextDouble() <= StartLocalNameServer.outputSampleRate) {
          GNS.getStatLogger().info(updateInfo.getUpdateStats(confirmPkt, updateInfo.getName()));
        }
    } else if (confirmPkt.getResponseCode().isAccessOrSignatureError()) {
      // if it's an access or signature failure just return it to the client support
        Intercessor.handleIncomingPackets(json);

    } else {
      // current active replica set invalid, so obtain the current set of actives for a name by contacting
      // replica controllers.
      handleInvalidActiveError(updateInfo);
    }

  }

  /**
   * Update request reached invalid active replica, so obtain a new set of actives and send request again.
   * @param updateInfo
   * @throws JSONException
   */
  private static void handleInvalidActiveError(UpdateInfo updateInfo) throws JSONException{
    GNS.getLogger().fine("\tInvalid Active Name Server.\tName\t" + updateInfo.getName() + "\tRequest new actives.\t");

    UpdateAddressPacket updateAddressPacket = updateInfo.getUpdateAddressPacket();

    // clear out current cache
    LocalNameServer.invalidateActiveNameServer(updateInfo.getName());

    // create objects that must be passed to PendingTasks
    SendUpdatesTask task = new SendUpdatesTask(updateAddressPacket, updateInfo.getSendTime(), new HashSet<Integer>(),
            updateInfo.getNumInvalidActiveError() + 1);
    String failedStats = UpdateInfo.getUpdateFailedStats(updateInfo.getName(), new HashSet<Integer>(),
            LocalNameServer.getNodeID(), updateAddressPacket.getRequestID(), updateInfo.getSendTime(), updateInfo.getNumInvalidActiveError() + 1, -1);
    ConfirmUpdateLNSPacket confirmFailPacket = ConfirmUpdateLNSPacket.createFailPacket(updateAddressPacket, NSResponseCode.ERROR);


    boolean firstInvalidActiveError = (updateInfo.getNumInvalidActiveError() == 0);

    PendingTasks.addToPendingRequests(updateInfo.getName(), task, StartLocalNameServer.queryTimeout,
            confirmFailPacket.toJSONObject(), failedStats, firstInvalidActiveError);


  }
}