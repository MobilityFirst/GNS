/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Abhigyan.
 */
package edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport;

import edu.umass.cs.gns.exceptions.CancelExecutorTaskException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.newApp.packet.ConfirmUpdatePacket;
import edu.umass.cs.gns.newApp.packet.UpdatePacket;
import edu.umass.cs.gns.util.NSResponseCode;
import edu.umass.cs.gns.util.Util;
import org.json.JSONException;
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
 * @see edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport.Update
 * @see edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport.UpdateInfo
 * @see edu.umass.cs.gns.newApp.packet.UpdatePacket
 *
 * @author abhigyan
 */
public class SendUpdatesTask extends TimerTask {

  private final String name;
  private UpdatePacket<String> updatePacket;
  private final int ccpReqID;

  private HashSet<String> activesQueried;
  private int timeoutCount = -1;

  private int requestActivesCount = -1;
  private final ClientRequestHandlerInterface handler;

  private String nameServerID; // just send it to this one
  // If this is true we send the update over the network to the AR
  // This is not normally going to be false except in the case of updates happening during
  // creation of guids which need to be explicitly coordinated by ARs 
  final boolean reallySendUpdatesToReplica;

  public SendUpdatesTask(int lnsReqID, ClientRequestHandlerInterface handler,
          UpdatePacket<String> updatePacket, String nameServerID) {
    this(lnsReqID, handler, updatePacket, nameServerID, false);
  }

  public SendUpdatesTask(int lnsReqID, ClientRequestHandlerInterface handler,
          UpdatePacket<String> updatePacket, String nameServerID, boolean reallySendUpdatesToReplica) {
    // based on request info.
    this.ccpReqID = lnsReqID;
    this.handler = handler;
    this.name = updatePacket.getName();
    this.updatePacket = updatePacket;
    this.activesQueried = new HashSet<>();
    this.nameServerID = nameServerID;
    this.reallySendUpdatesToReplica = reallySendUpdatesToReplica;
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
      if (nameServerID == null) { // new code to not request actives and only send to one server
        CacheEntry<String> cacheEntry = handler.getCacheEntry(name);
        // IF we don't have one or more valid active replicas in the cache entry
        // we need to request a new set for this name.
        if (cacheEntry == null || !cacheEntry.isValidNameserver()) {
          requestNewActives(handler);
          // Cancel the task now. 
          // When the new actives are received, a new task in place of this task will be rescheduled.
          throw new CancelExecutorTaskException();
        }
        nameServerID = selectNS(cacheEntry);
      }

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
    @SuppressWarnings("unchecked")
    UpdateInfo<String> info = (UpdateInfo<String>) handler.getRequestInfo(ccpReqID);
    if (info == null) {
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().fine("UpdateInfo<String> not found. Update complete. Cancel task. " + ccpReqID + "\t" + updatePacket);
      }
      return true;
    } else if (requestActivesCount == -1) {
      requestActivesCount = info.getNumLookupActives();
    } else if (requestActivesCount != info.getNumLookupActives()) {  // set timer task ID to CCP
      // invalid active response received in this case
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().fine("Invalid active response received. Cancel task. " + ccpReqID + "\t" + updatePacket);
      }
      return true;
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  private boolean isMaxWaitTimeExceeded() {
    UpdateInfo<String> info = (UpdateInfo<String>) handler.getRequestInfo(ccpReqID);
    if (info != null) {   // probably NS sent response
      // Too much time elapsed, send failed msg to user and log error
      if (System.currentTimeMillis() - info.getStartTime() > handler.getParameters().getMaxQueryWaitTime()) {
        // remove from request info as CCP must clear all state for this request
        info = (UpdateInfo<String>) handler.removeRequestInfo(ccpReqID);
        if (info != null) {
          if (handler.getParameters().isDebugMode()) {
            GNS.getLogger().fine("UPDATE FAILED no response until MAX-wait time: request ID = " + ccpReqID + " name = " + name);
          }
          // create a failure packet and send it back to client support

          try {
            ConfirmUpdatePacket<String> confirmPkt = new ConfirmUpdatePacket<String>(info.getErrorMessage(NSResponseCode.UPDATE_TIMEOUT), handler.getGnsNodeConfig());
            Update.sendConfirmUpdatePacketBackToSource(confirmPkt, handler);
          } catch (JSONException e) {
            e.printStackTrace();
            GNS.getLogger().severe("Problem converting packet to JSON: " + e);
          }
          info.setFinishTime();
          info.setSuccess(false);
        }
        return true;
      }
    }
    return false;

  }

  private void requestNewActives(ClientRequestHandlerInterface handler) {
    // remove update info from CCP
    @SuppressWarnings("unchecked")
    UpdateInfo<String> info = (UpdateInfo<String>) handler.getRequestInfo(ccpReqID);
    if (info != null) {   // probably NS sent response
      SendUpdatesTask newTask = new SendUpdatesTask(ccpReqID, handler, updatePacket, null);
      PendingTasks.addToPendingRequests(info, newTask, handler.getParameters().getQueryTimeout(), handler);
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().fine("Created a request actives task. " + info.getNumLookupActives());
      }

    }
  }

  private String selectNS(CacheEntry<String> cacheEntry) {
    String nameServerID;
    return handler.getGnsNodeConfig().getClosestServer(cacheEntry.getActiveNameServers(),
              activesQueried);
  }

  private void sendToNS(String nameServerID) {

    if (nameServerID == null) {
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().fine("ERROR: No more actives left to query. Actives Queried " + Util.setOfNodeIdToString(activesQueried));
      }
      return;
    }
    @SuppressWarnings("unchecked")
    UpdateInfo<String> info = (UpdateInfo<String>) handler.getRequestInfo(ccpReqID);
    activesQueried.add(nameServerID);
    UpdatePacket<String> pkt = Update.makeNewUpdatePacket(ccpReqID, handler, updatePacket, nameServerID);
    // FIXME we are creating a clone of the packet here? Why? Any other way to do this?
    // create the packet that we'll send to the primary
//    UpdatePacket<NodeIDType> pkt = new UpdatePacket<NodeIDType>(
//            updatePacket.getSourceId(), // DON'T JUST USE -1!!!!!! THIS IS IMPORTANT!!!!
//            updatePacket.getRequestID(),
//            ccpReqID, // the id use by the CCP (that would be us here)
//            name,
//            updatePacket.getRecordKey(),
//            updatePacket.getUpdateValue(),
//            updatePacket.getOldValue(),
//            updatePacket.getArgument(),
//            updatePacket.getUserJSON(),
//            updatePacket.getOperation(),
//            handler.getNodeAddress(),
//            nameServerID, updatePacket.getTTL(),
//            //signature info
//            updatePacket.getAccessor(),
//            updatePacket.getSignature(),
//            updatePacket.getMessage());

    if (handler.getParameters().isDebugMode()) {
      GNS.getLogger().fine("Sending Update to Node: " + nameServerID.toString());
    }

    // and send it off
    if (!reallySendUpdatesToReplica) {
      handler.getApp().handleRequest(pkt);
    } else {
      // This is not normally used except in the case of updates happening during
      // creation of guids which need to be explicitly coordinated by ARs 
      try {
        if (handler.getParameters().isDebugMode()) {
          GNS.getLogger().info("++++++++++++++++++ REALLY SENDING " + pkt.toJSONObject() + " TO " + nameServerID);
        }
        handler.sendToNS(pkt.toJSONObject(), nameServerID);
      } catch (JSONException e) {
        GNS.getLogger().severe("Problem sending JSON update to " + nameServerID + ": " + e);
      }
    }
    // keep track of which NS we sent it to
    @SuppressWarnings("unchecked")
    UpdateInfo<String> updateInfo = (UpdateInfo<String>) handler.getRequestInfo(ccpReqID);
    if (updateInfo != null) {
      updateInfo.setNameserverID(nameServerID);
    }
    if (handler.getParameters().isDebugMode()) {
      GNS.getLogger().fine("Send update to: " + nameServerID.toString() + " Name:" + name + " Id:" + ccpReqID
              + " Time:" + System.currentTimeMillis() + " --> " + pkt.toString());
    }
//    } catch (JSONException e) {
//      e.printStackTrace();
//    }
  }
}
