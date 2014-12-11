/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Abhigyan.
 */
package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.exceptions.CancelExecutorTaskException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.packet.ConfirmUpdatePacket;
import edu.umass.cs.gns.nsdesign.packet.UpdatePacket;
import edu.umass.cs.gns.nsdesign.replicationframework.BeehiveReplication;
import edu.umass.cs.gns.nsdesign.replicationframework.ReplicationFrameworkType;
import edu.umass.cs.gns.util.Util;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.TimerTask;

/**
 * Send an address update request from client and to active replicas one by one in the order
 * of their distance from this local name server. The repeat execution of this task is cancelled in following cases:
 * (1) name server responds to update request.
 * (2) max wait time for a request is exceeded, in which case, we send error message to client.
 * (3) local name server's cache does not have active replicas for a name. In this case, we start the process
 * of obtaining current set of actives for the name.
 *
 * @param <NodeIDType>
 * @see edu.umass.cs.gns.localnameserver.Update
 * @see edu.umass.cs.gns.localnameserver.UpdateInfo
 * @see edu.umass.cs.gns.nsdesign.packet.UpdatePacket
 *
 * @author abhigyan
 */
public class SendUpdatesTask<NodeIDType> extends TimerTask {

  private final String name;
  private UpdatePacket updatePacket;
  private final int lnsReqID;

  private HashSet<NodeIDType> activesQueried;
  private int timeoutCount = -1;

  private int requestActivesCount = -1;
  private final ClientRequestHandlerInterface<NodeIDType> handler;

  public SendUpdatesTask(int lnsReqID, ClientRequestHandlerInterface<NodeIDType> handler, UpdatePacket updatePacket) {
    // based on request info.
    this.lnsReqID = lnsReqID;
    this.handler = handler;
    this.name = updatePacket.getName();
    this.updatePacket = updatePacket;
    this.activesQueried = new HashSet<>();
  }

  @Override
  // Pretty much the same code as in DNSRequestTask
  // except DNSRequestTask can return a cached response to client
  public void run() {
    try {
      timeoutCount++;
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().fine("ENTER name = " + name + " timeout = " + timeoutCount);
      }
      if (isResponseReceived() || isMaxWaitTimeExceeded()) {
        throw new CancelExecutorTaskException();
      }
      CacheEntry cacheEntry = handler.getCacheEntry(name);
      // IF we don't have one or more valid active replicas in the cache entry
      // we need to request a new set for this name.
      if (cacheEntry == null || !cacheEntry.isValidNameserver()) {
        requestNewActives(handler);
        // Cancel the task now. 
        // When the new actives are received, a new task in place of this task will be rescheduled.
        throw new CancelExecutorTaskException();
      }
      NodeIDType nameServerID = selectNS(cacheEntry);

      sendToNS(nameServerID);

    } catch (Exception e) { // we catch all possible exceptions because executor service does not print message on exception
      if (e.getClass().equals(CancelExecutorTaskException.class)) {
        throw new RuntimeException();
      }
      // all exceptions other than CancelExecutorTaskException are logged.
      GNS.getLogger().severe("Unexpected Exception in send updates task: " + e);
      e.printStackTrace();
    }
  }

  private boolean isResponseReceived() {
    UpdateInfo info = (UpdateInfo) handler.getRequestInfo(lnsReqID);
    if (info == null) {
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().fine("UpdateInfo not found. Update complete. Cancel task. " + lnsReqID + "\t" + updatePacket);
      }
      return true;
    } else if (requestActivesCount == -1) {
      requestActivesCount = info.getNumLookupActives();
    } else if (requestActivesCount != info.getNumLookupActives()) {  // set timer task ID to LNS
      // invalid active response received in this case
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().fine("Invalid active response received. Cancel task. " + lnsReqID + "\t" + updatePacket);
      }
      return true;
    }
    return false;
  }

  private boolean isMaxWaitTimeExceeded() {
    UpdateInfo info = (UpdateInfo) handler.getRequestInfo(lnsReqID);
    if (info != null) {   // probably NS sent response
      // Too much time elapsed, send failed msg to user and log error
      if (System.currentTimeMillis() - info.getStartTime() > handler.getParameters().getMaxQueryWaitTime()) {
        // remove from request info as LNS must clear all state for this request
        info = (UpdateInfo) handler.removeRequestInfo(lnsReqID);
        if (info != null) {
          if (handler.getParameters().isDebugMode()) {
            GNS.getLogger().fine("UPDATE FAILED no response until MAX-wait time: request ID = " + lnsReqID + " name = " + name);
          }
          // create a failure packet and send it back to client support

          try {
            ConfirmUpdatePacket<NodeIDType> confirmPkt = new ConfirmUpdatePacket<NodeIDType>(info.getErrorMessage(), handler.getGnsNodeConfig());
            Update.sendConfirmUpdatePacketBackToSource(confirmPkt, handler);
          } catch (JSONException e) {
            e.printStackTrace();
            GNS.getLogger().severe("Problem converting packet to JSON: " + e);
          }
          info.setFinishTime();
          info.setSuccess(false);
          info.addEventCode(LNSEventCode.MAX_WAIT_ERROR);
          GNS.getStatLogger().info(info.getLogString());
        }
        return true;
      }
    }
    return false;

  }

  private void requestNewActives(ClientRequestHandlerInterface handler) {
    // remove update info from LNS
    UpdateInfo info = (UpdateInfo) handler.getRequestInfo(lnsReqID);
    if (info != null) {   // probably NS sent response
      SendUpdatesTask<NodeIDType> newTask = new SendUpdatesTask<NodeIDType>(lnsReqID, handler, updatePacket);
      PendingTasks.addToPendingRequests(info, newTask, handler.getParameters().getQueryTimeout(), handler);
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().fine("Created a request actives task. " + info.getNumLookupActives());
      }

    }
  }

  private NodeIDType selectNS(CacheEntry cacheEntry) {
    NodeIDType nameServerID;
    if (handler.getParameters().isLoadDependentRedirection()) {
      nameServerID = (NodeIDType) handler.getGnsNodeConfig().getClosestServer(cacheEntry.getActiveNameServers(),
              activesQueried);
    } else if (handler.getParameters().getReplicationFramework() == ReplicationFrameworkType.BEEHIVE) {
      nameServerID = (NodeIDType) BeehiveReplication.getBeehiveNameServer(handler.getGnsNodeConfig(),
              cacheEntry.getActiveNameServers(), activesQueried);
    } else {
      nameServerID = (NodeIDType) handler.getGnsNodeConfig().getClosestServer(cacheEntry.getActiveNameServers(),
              activesQueried);
    }
    return nameServerID;
  }

  private void sendToNS(NodeIDType nameServerID) {

    if (nameServerID == null) {
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().fine("ERROR: No more actives left to query. Actives Queried " + Util.setOfNodeIdToString(activesQueried));
      }
      return;
    }
    UpdateInfo info = (UpdateInfo) handler.getRequestInfo(lnsReqID);
    if (info != null) info.addEventCode(LNSEventCode.CONTACT_ACTIVE);
    activesQueried.add(nameServerID);
    // FIXME we are creating a clone of the packet here? Why? Any other way to do this?
    // create the packet that we'll send to the primary
    UpdatePacket<NodeIDType> pkt = new UpdatePacket(
            updatePacket.getSourceId(), // DON'T JUST USE -1!!!!!! THIS IS IMPORTANT!!!!
            updatePacket.getRequestID(),
            lnsReqID, // the id use by the LNS (that would be us here)
            name,
            updatePacket.getRecordKey(),
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

    if (handler.getParameters().isDebugMode()) {
      GNS.getLogger().fine("Sending Update to Node: " + nameServerID.toString());
    }

    // and send it off
    try {
      JSONObject jsonToSend = pkt.toJSONObject();
      handler.sendToNS(jsonToSend, nameServerID);
      // keep track of which NS we sent it to
      UpdateInfo<NodeIDType> updateInfo = (UpdateInfo) handler.getRequestInfo(lnsReqID);
      if (updateInfo != null) {
        updateInfo.setNameserverID(nameServerID);
      }
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().fine("Send update to: " + nameServerID.toString() + " Name:" + name + " Id:" + lnsReqID
                + " Time:" + System.currentTimeMillis() + " --> " + jsonToSend.toString());
      }
    } catch (JSONException e) {
      e.printStackTrace();
    }
  }
}
