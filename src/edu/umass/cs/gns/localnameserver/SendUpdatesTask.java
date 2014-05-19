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
import edu.umass.cs.gns.nsdesign.replicationframework.RandomReplication;
import edu.umass.cs.gns.nsdesign.replicationframework.ReplicationFrameworkType;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nsdesign.packet.*;
import edu.umass.cs.gns.util.NSResponseCode;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.TimerTask;

/**
 * Send an address update request from client (non-upsert case) and to active replicas one by one in the order
 * of their distace from this local name server. The repeat execution of this task is cancelled in following cases:
 * (1) name server responds to update request.
 * (2) max wait time for a request is exceeded, in which case, we send error message to client.
 * (3) local name server's cache does not have active replicas for a name. In this case, we start the process
 * of obtaining current set of actives for the name.
 *
 * @see edu.umass.cs.gns.localnameserver.Update
 * @see edu.umass.cs.gns.localnameserver.UpdateInfo
 * @see edu.umass.cs.gns.nsdesign.packet.UpdatePacket
 *
 * @author abhigyan
 */
public class SendUpdatesTask extends TimerTask {

  private String name;
  private UpdatePacket updatePacket;
  private int updateRequestID;
  private HashSet<Integer> activesQueried;
  private int timeoutCount = -1;
  private long requestRecvdTime;
  private int numRestarts;
  private int coordinatorID = -1;

  public SendUpdatesTask(UpdatePacket updatePacket,
          long requestRecvdTime, HashSet<Integer> activesQueried, int numRestarts) {
    this.name = updatePacket.getName();
    this.updatePacket = updatePacket;
    this.activesQueried = activesQueried;
    this.requestRecvdTime = requestRecvdTime;
    this.numRestarts = numRestarts;
  }

  @Override
  // Pretty much the same code as in DNSRequestTask
  public void run() {
    try {

      timeoutCount++;

      if (StartLocalNameServer.debugMode) {
        GNS.getLogger().fine("ENTER name = " + name + " timeout = " + timeoutCount);
      }
      if (isMaxWaitTimeExceeded() || isResponseReceived()) {
        throw new CancelExecutorTaskException();
      }

      CacheEntry cacheEntry = LocalNameServer.getCacheEntry(name);
      // IF we don't have one or more valid active replicas in the cache entry
      // we need to request a new set for this name.
      if (cacheEntry == null || cacheEntry.isValidNameserver() == false) {
        requestNewActives();
        // Cancel the task now. 
        // When the request is satisfied this current task will be rescheduled.
        throw new CancelExecutorTaskException();
      }
      int nameServerID = selectNS(cacheEntry);

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

    if (timeoutCount > 0 && LocalNameServer.getUpdateInfo(updateRequestID) == null) {
      if (StartLocalNameServer.debugMode) {
        GNS.getLogger().fine("UpdateInfo not found. Update complete or actives invalidated. Cancel task.");
      }
      return true;
    }
    return false;

  }

  private boolean isMaxWaitTimeExceeded() {
    // Too much time elaspsed, send failed msg to user and log error
    if (System.currentTimeMillis() - requestRecvdTime > StartLocalNameServer.maxQueryWaitTime) {
      if (StartLocalNameServer.debugMode) {
        GNS.getLogger().fine("UPDATE FAILED no response until MAX-wait time: request ID = " + updateRequestID + " name = " + name);
      }
      handleFailure();
      return true;
    }
    return false;
  }

  private void requestNewActives() {
    // remove update info from LNS
    if (timeoutCount > 0) {
      LocalNameServer.removeUpdateInfo(updateRequestID);
    }

    // add to pending requests task
    try {
      SendUpdatesTask newTask = new SendUpdatesTask(updatePacket, requestRecvdTime, new HashSet<Integer>(), numRestarts + 1);
      String failureLogMsg = UpdateInfo.getUpdateFailedStats(name, new HashSet<Integer>(), LocalNameServer.getNodeID(),
              updatePacket.getRequestID(), requestRecvdTime, numRestarts + 1, -1, updatePacket.getType());
      PendingTasks.addToPendingRequests(name, newTask, StartLocalNameServer.queryTimeout,
              ConfirmUpdatePacket.createFailPacket(updatePacket, NSResponseCode.ERROR).toJSONObject(),
              failureLogMsg, numRestarts == 0);

    } catch (JSONException e) {
      GNS.getLogger().severe("Problem creating a JSON object: " + e);
    }

    if (StartLocalNameServer.debugMode) {
      GNS.getLogger().fine("Created a request actives task. " + numRestarts);
    }
  }

  private int selectNS(CacheEntry cacheEntry) {
    int nameServerID;
    if (StartLocalNameServer.loadDependentRedirection) {
      nameServerID = LocalNameServer.getGnsNodeConfig().getClosestServer(cacheEntry.getActiveNameServers(),
              activesQueried);
    } else if (StartLocalNameServer.replicationFramework == ReplicationFrameworkType.BEEHIVE) {
      nameServerID = RandomReplication.getBeehiveNameServer(LocalNameServer.getGnsNodeConfig(),
              cacheEntry.getActiveNameServers(), activesQueried);
    } else {
      nameServerID = LocalNameServer.getGnsNodeConfig().getClosestServer(cacheEntry.getActiveNameServers(),
              activesQueried);
      coordinatorID = LocalNameServer.getDefaultCoordinatorReplica(name, cacheEntry.getActiveNameServers());
    }
    return nameServerID;
  }

  private void sendToNS(int nameServerID) {

    if (nameServerID == -1) {

      if (StartLocalNameServer.debugMode) {
        GNS.getLogger().fine("ERROR: No more actives left to query. Actives Queried " + activesQueried);
      }
      return;
    }

    activesQueried.add(nameServerID);

    if (timeoutCount == 0) {

      updateRequestID = LocalNameServer.addUpdateInfo(name, nameServerID, requestRecvdTime, numRestarts, updatePacket);
      if (StartLocalNameServer.debugMode) {
        GNS.getLogger().fine("Update Info Added: Id = " + updateRequestID);
      }
    }
    // create the packet that we'll send to the primary
    UpdatePacket pkt = new UpdatePacket(
            updatePacket.getSourceId(), // DON'T JUST USE -1!!!!!! THIS IS IMPORTANT!!!!
            updatePacket.getRequestID(),
            updateRequestID, // the id use by the LNS (that would be us here)
            name,
            updatePacket.getRecordKey(),
            updatePacket.getUpdateValue(),
            updatePacket.getOldValue(),
            updatePacket.getArgument(),
            updatePacket.getOperation(),
            LocalNameServer.getNodeID(),
            nameServerID, updatePacket.getTTL(),
            //signature info
            updatePacket.getAccessor(),
            updatePacket.getSignature(),
            updatePacket.getMessage());

    if (StartLocalNameServer.debugMode) {
      GNS.getLogger().fine("Sending Update to Node: " + nameServerID);
    }

    // and send it off
    try {
      JSONObject jsonToSend = pkt.toJSONObject();
      LocalNameServer.sendToNS(jsonToSend, nameServerID);
      // keep track of which NS we sent it to
      UpdateInfo updateInfo = LocalNameServer.getUpdateInfo(nameServerID);
      if (updateInfo != null) {
        updateInfo.setNameserverID(nameServerID);
      }
      if (StartLocalNameServer.debugMode) {
        GNS.getLogger().fine("Send update to: " + nameServerID + " Name:" + name + " Id:" + updateRequestID
                + " Time:" + System.currentTimeMillis()
                + " --> " + jsonToSend.toString());
      }
    } catch (JSONException e) {
      e.printStackTrace();
    }
  }

  private void handleFailure() {
    // create a failure packet and send it back to client support
    ConfirmUpdatePacket confirmPkt = ConfirmUpdatePacket.createFailPacket(updatePacket, NSResponseCode.ERROR);
    try {
      Update.sendConfirmUpdatePacketBackToSource(confirmPkt);
      //Intercessor.handleIncomingPackets(confirmPkt.toJSONObject());
    } catch (JSONException e) {
      GNS.getLogger().severe("Problem converting packet to JSON: " + e);
    }

    UpdateInfo updateInfo = LocalNameServer.removeUpdateInfo(updateRequestID);
    if (updateInfo == null) {
      if (timeoutCount == 0) {
        GNS.getStatLogger().fine(UpdateInfo.getUpdateFailedStats(name, activesQueried, LocalNameServer.getNodeID(),
                updatePacket.getRequestID(), requestRecvdTime, numRestarts, coordinatorID, updatePacket.getType()));
      }
    } else {
      GNS.getStatLogger().fine(updateInfo.getUpdateFailedStats(activesQueried, LocalNameServer.getNodeID(),
              updatePacket.getRequestID(), coordinatorID));

    }
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @return the updatePacket
   */
  public UpdatePacket getUpdatePacket() {
    return updatePacket;
  }

  /**
   * @return the updateRequestID
   */
  public int getUpdateRequestID() {
    return updateRequestID;
  }

  /**
   * @return the activesQueried
   */
  public HashSet<Integer> getActivesQueried() {
    return activesQueried;
  }

  /**
   * @return the timeoutCount
   */
  public int getTimeoutCount() {
    return timeoutCount;
  }

  /**
   * @return the requestRecvdTime
   */
  public long getRequestRecvdTime() {
    return requestRecvdTime;
  }

  /**
   * @return the numInvalidActiveError
   */
  public int getNumRestarts() {
    return numRestarts;
  }

  /**
   * @return the coordinatorID
   */
  public int getCoordinatorID() {
    return coordinatorID;
  }
}
