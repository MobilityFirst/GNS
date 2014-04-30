/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 *
 */
package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nsdesign.packet.*;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * Class contains a few static methods for handling ADD, REMOVE, and UPSERT (update + insert) requests from clients
 * as well responses to these requests from name servers. Most functionality for handling request sent by clients
 * is implemented in <code>SendAddRemoveUpsertTask</code>. So also refer to its documentation.
 * <p>
 * The addition and removal of a name in GNS is handled by replica controllers, therefore we send ADD and REMOVE
 * to a replica controller. The replica controllers for a name are fixed and a local name server can compute the set of
 * replica controllers locally (see method {@link edu.umass.cs.gns.util.ConsistentHashing#getReplicaControllerSet(String)}).
 * Like other requests, add/removes are also retransmitted to a different name server if no confirmation is received
 * until a timeout value.
 * <p>
 * An upsert request may create a new name record, unlike an update which modifies an existing name record.
 * Becasue addition of a name is done by replica controllers, we send an upsert to replica controllers.
 * If upsert request is for an already existing name, it is handled like an update. To this end, replica controllers
 * will forward the request to active replicas.
 * <p>
 *
 */
public class AddRemove {

  /**
   *
   * @param json
   * @throws JSONException
   * @throws UnknownHostException
   */
  static void handlePacketAddRecord(JSONObject json) throws JSONException, UnknownHostException {

    AddRecordPacket addRecordPacket = new AddRecordPacket(json);

    SendAddRemoveUpsertTask addTask = new SendAddRemoveUpsertTask(addRecordPacket, addRecordPacket.getName(),
            System.currentTimeMillis(), new HashSet<Integer>());
    LocalNameServer.getExecutorService().scheduleAtFixedRate(addTask, 0, StartLocalNameServer.queryTimeout, TimeUnit.MILLISECONDS);
    addRecordPacket.getLocalNameServerID();
    GNS.getLogger().info(" Add Task Scheduled. " + "Name: " + addRecordPacket.getName() + " Request: " + addRecordPacket.getRequestID());
  }

  /**
   *
   * @param updateAddressPacket
   * @throws JSONException
   */
  static void handleUpsert(UpdatePacket updateAddressPacket) throws JSONException {

    SendAddRemoveUpsertTask upsertTask = new SendAddRemoveUpsertTask(updateAddressPacket, updateAddressPacket.getName(),
            System.currentTimeMillis(), new HashSet<Integer>());
    LocalNameServer.getExecutorService().scheduleAtFixedRate(upsertTask, 0, StartLocalNameServer.queryTimeout, TimeUnit.MILLISECONDS);
    if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Upsert Task Scheduled: "
            + "Name: " + updateAddressPacket.getName() + " Request: " + updateAddressPacket.getRequestID());
  }

  /**
   *
   * @param json
   * @throws JSONException
   * @throws NoSuchAlgorithmException
   * @throws UnsupportedEncodingException
   */
  static void handlePacketRemoveRecord(JSONObject json)
          throws JSONException, NoSuchAlgorithmException, UnsupportedEncodingException, UnknownHostException {

    RemoveRecordPacket removeRecord = new RemoveRecordPacket(json);
    InetAddress senderAddress = null;
    int senderPort = -1;
    senderPort = Transport.getReturnPort(json);
    if (Transport.getReturnAddress(json) != null) {
      senderAddress = InetAddress.getByName(Transport.getReturnAddress(json));
    }
    SendAddRemoveUpsertTask task = new SendAddRemoveUpsertTask(removeRecord, removeRecord.getName(),
            System.currentTimeMillis(), new HashSet<Integer>());
    LocalNameServer.getExecutorService().scheduleAtFixedRate(task, 0, StartLocalNameServer.queryTimeout, TimeUnit.MILLISECONDS);

    if (StartLocalNameServer.debugMode) {
      GNS.getLogger().fine(" Remove  Task Scheduled. "
              + "Name: " + removeRecord.getName() + " Request: " + removeRecord.getRequestID());
    }
  }

  /**
   * Handles confirmation of add request from NS
   */
  static void handlePacketConfirmAdd(JSONObject json) throws JSONException, UnknownHostException {
    ConfirmUpdatePacket confirmAddPacket = new ConfirmUpdatePacket(json);
    UpdateInfo addInfo = LocalNameServer.removeUpdateInfo(confirmAddPacket.getLNSRequestID());
    GNS.getLogger().info("Confirm add packet: " + confirmAddPacket.toString() + " add info " + addInfo);
    if (addInfo == null) {
      GNS.getLogger().warning("Add confirmation return info not found.: lns request id = " + confirmAddPacket.getLNSRequestID());
    } else {
      // update our cache BEFORE we confirm
      LocalNameServer.updateCacheEntry(confirmAddPacket, addInfo.getName(), null);

      String stats = addInfo.getUpdateStats(confirmAddPacket);
      GNS.getStatLogger().info(stats);
      Update.sendConfirmUpdatePacketBackToSource(confirmAddPacket);
    }
  }

  /**
   * Handles confirmation of add request from NS
   */
  static void handlePacketConfirmRemove(JSONObject json) throws JSONException, UnknownHostException {
    ConfirmUpdatePacket confirmRemovePacket = new ConfirmUpdatePacket(json);
    UpdateInfo removeInfo = LocalNameServer.removeUpdateInfo(confirmRemovePacket.getLNSRequestID());
    GNS.getLogger().info("Confirm remove packet: " + confirmRemovePacket.toString() + " remove info " + removeInfo);
    if (removeInfo == null) {
      GNS.getLogger().warning("Remove confirmation return info not found.: lns request id = " + confirmRemovePacket.getLNSRequestID());
    } else {
      // update our cache BEFORE we confirm
      LocalNameServer.updateCacheEntry(confirmRemovePacket, removeInfo.getName(), null);
      String stats = removeInfo.getUpdateStats(confirmRemovePacket);
      GNS.getStatLogger().info(stats);
      Update.sendConfirmUpdatePacketBackToSource(confirmRemovePacket);
    }
  }
}
