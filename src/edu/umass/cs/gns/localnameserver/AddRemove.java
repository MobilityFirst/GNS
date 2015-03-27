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
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeUnit;

/**
 * Class contains a few static methods for handling ADD, and REMOVE requests from clients
 * as well responses to these requests from name servers. Most functionality for handling request sent by clients
 * is implemented in <code>SendAddRemoveTask</code>. So also refer to its documentation.
 * <p>
 * The addition and removal of a name in GNS is handled by replica controllers, therefore we send ADD and REMOVE
 * to a replica controller. The replica controllers for a name are fixed and a local name server can compute the set of
 * replica controllers locally (see method {@link edu.umass.cs.gns.util.ConsistentHashing#getReplicaControllerSet(String)}).
 * Like other requests, add/removes are also retransmitted to a different name server if no confirmation is received
 * until a timeout value.
 * <p>
 *
 * @author abhigyan
 */
public class AddRemove {

  /**
   *
   * @param json
   * @throws JSONException
   * @throws UnknownHostException
   */
  public static void handlePacketAddRecord(JSONObject json, ClientRequestHandlerInterface handler) throws JSONException, UnknownHostException {

    AddRecordPacket addRecordPacket = new AddRecordPacket(json, handler.getGnsNodeConfig());
    int lnsReqID = handler.getUniqueRequestID();
    UpdateInfo info = new UpdateInfo(lnsReqID, addRecordPacket.getName(), null, addRecordPacket, handler);
    handler.addRequestInfo(lnsReqID, info);
    //
    SendAddRemoveTask addTask = new SendAddRemoveTask(lnsReqID, handler, addRecordPacket, addRecordPacket.getName(),
            System.currentTimeMillis());
    handler.getExecutorService().scheduleAtFixedRate(addTask, 0, StartLocalNameServer.queryTimeout, TimeUnit.MILLISECONDS);
    if (handler.getParameters().isDebugMode()) {
      GNS.getLogger().info(" Add Task Scheduled. " + "Name: " + addRecordPacket.getName() + " Request: " + addRecordPacket.getRequestID());
    }
  }

  /**
   *
   * @param json
   * @throws JSONException
   * @throws NoSuchAlgorithmException
   * @throws UnsupportedEncodingException
   */
  public static void handlePacketRemoveRecord(JSONObject json, ClientRequestHandlerInterface handler)
          throws JSONException, NoSuchAlgorithmException, UnsupportedEncodingException, UnknownHostException {

    RemoveRecordPacket removeRecord = new RemoveRecordPacket(json, handler.getGnsNodeConfig());
    int lnsReqID = handler.getUniqueRequestID();
    UpdateInfo info = new UpdateInfo(lnsReqID, removeRecord.getName(), null, removeRecord, handler);
    handler.addRequestInfo(lnsReqID, info);

    SendAddRemoveTask task = new SendAddRemoveTask(lnsReqID, handler, removeRecord, removeRecord.getName(),
            System.currentTimeMillis());
    handler.getExecutorService().scheduleAtFixedRate(task, 0, StartLocalNameServer.queryTimeout, TimeUnit.MILLISECONDS);

    if (handler.getParameters().isDebugMode()) {
      GNS.getLogger().info("Remove Task Scheduled. " + "Name: " + removeRecord.getName()
              + " Request: " + removeRecord.getRequestID());
    }
  }
  
  /**
   * Handles confirmation of add request from NS
   */
  public static void handlePacketConfirmAdd(JSONObject json, ClientRequestHandlerInterface handler) throws JSONException, UnknownHostException {
    ConfirmUpdatePacket confirmAddPacket = new ConfirmUpdatePacket(json, handler.getGnsNodeConfig());
    UpdateInfo addInfo = (UpdateInfo) handler.removeRequestInfo(confirmAddPacket.getLNSRequestID());
    if (handler.getParameters().isDebugMode()) {
      GNS.getLogger().info("Confirm add packet for " + addInfo.getName() + ": " + confirmAddPacket.toString());
    }
    if (addInfo == null) {
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().warning("Add confirmation return info not found.: lns request id = "
                + confirmAddPacket.getLNSRequestID());
      }
    } else {
      // update our cache BEFORE we confirm
      handler.updateCacheEntry(confirmAddPacket, addInfo.getName(), null);
      addInfo.setSuccess(confirmAddPacket.isSuccess());
      addInfo.setFinishTime();
      if (confirmAddPacket.isSuccess()) {
        addInfo.addEventCode(LNSEventCode.SUCCESS);
      } else {
        addInfo.addEventCode(LNSEventCode.OTHER_ERROR);
      }
      GNS.getStatLogger().info(addInfo.getLogString());
      Update.sendConfirmUpdatePacketBackToSource(confirmAddPacket, handler);
    }
  }

  /**
   * Handles confirmation of add request from NS
   */
  public static void handlePacketConfirmRemove(JSONObject json, ClientRequestHandlerInterface handler) throws JSONException, UnknownHostException {
    ConfirmUpdatePacket confirmRemovePacket = new ConfirmUpdatePacket(json, handler.getGnsNodeConfig());
    UpdateInfo removeInfo = (UpdateInfo) handler.removeRequestInfo(confirmRemovePacket.getLNSRequestID());
    if (handler.getParameters().isDebugMode()) {
      GNS.getLogger().fine("Confirm remove packet for " + removeInfo.getName() + ": " + confirmRemovePacket.toString() + " remove info " + removeInfo);
    }
    if (removeInfo == null) {
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().warning("Remove confirmation return info not found.: lns request id = " + confirmRemovePacket.getLNSRequestID());
      }
    } else {
      // update our cache BEFORE we confirm
      handler.updateCacheEntry(confirmRemovePacket, removeInfo.getName(), null);
      removeInfo.setSuccess(confirmRemovePacket.isSuccess());
      removeInfo.setFinishTime();
      if (confirmRemovePacket.isSuccess()) {
        removeInfo.addEventCode(LNSEventCode.SUCCESS);
      } else {
        removeInfo.addEventCode(LNSEventCode.OTHER_ERROR);
      }
      String stats = removeInfo.getLogString();
      GNS.getStatLogger().fine(stats);
      Update.sendConfirmUpdatePacketBackToSource(confirmRemovePacket, handler);
    }
  }
}
