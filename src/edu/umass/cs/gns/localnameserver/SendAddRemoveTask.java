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
import edu.umass.cs.gns.nsdesign.packet.AddRecordPacket;
import edu.umass.cs.gns.nsdesign.packet.BasicPacket;
import edu.umass.cs.gns.nsdesign.packet.ConfirmUpdatePacket;
import static edu.umass.cs.gns.nsdesign.packet.Packet.PacketType.*;
import edu.umass.cs.gns.nsdesign.packet.RemoveRecordPacket;
import org.json.JSONException;
import org.json.JSONObject;
import java.util.HashSet;
import java.util.TimerTask;

/**
 * Sends three types of messages (with retries): AddRecordPacket, RemoveRecordPacket, and
 * to replica controllers. These messages are sent one by one to all
 * replica controllers in order of their distance until
 * (1) local name server receives a response from one of the primary replicas.
 * (2) no response is received until {@code maxQueryWaitTime}. In this case, an error response is sent to client.
 *
 * User: abhigyan
 * Date: 8/9/13
 * Time: 4:59 PM
 */
public class SendAddRemoveTask extends TimerTask {

  private final String name;
  private final BasicPacket packet;
  private final int lnsRequestID;
  private final HashSet<Integer> replicaControllersQueried;
  private int timeoutCount = -1;
  private final long requestRecvdTime;
  private final ClientRequestHandlerInterface handler;

  public SendAddRemoveTask(int lnsRequestID, ClientRequestHandlerInterface handler, BasicPacket packet, String name, long requestRecvdTime) {
    this.name = name;
    this.handler = handler;
    this.packet = packet;
    this.lnsRequestID = lnsRequestID;
    this.replicaControllersQueried = new HashSet<Integer>();
    this.requestRecvdTime = requestRecvdTime;
  }

  @Override
  public void run() {
    try {
      timeoutCount = timeoutCount + 1;
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().fine("ENTER name = " + getName() + " timeout = " + getTimeoutCount());
      }

      if (isResponseReceived() || isMaxWaitTimeExceeded()) {
        throw new CancelExecutorTaskException();
      }

      int nameServerID = selectNS();

      sendToNS(nameServerID);

    } catch (Exception e) {
      // we catch all exceptions here because executor service will not print any exception messages
      if (e.getClass().equals(CancelExecutorTaskException.class)) {
        // this exception is only way to terminate this task from repeat execution
        throw new RuntimeException();
      }
      GNS.getLogger().severe("Exception Exception Exception ... ");
      e.printStackTrace();
    }
  }

  private boolean isResponseReceived() {
    if (LocalNameServer.getRequestInfo(getLnsRequestID()) == null) {
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().fine("UpdateInfo not found. Either update complete or invalid actives. Cancel task.");
      }
      return true;
    }
    return false;
  }

  private boolean isMaxWaitTimeExceeded() {
    if (getTimeoutCount() > 0 && System.currentTimeMillis() - getRequestRecvdTime() > handler.getParameters().getMaxQueryWaitTime()) {
      UpdateInfo updateInfo = (UpdateInfo) handler.removeRequestInfo(getLnsRequestID());

      if (updateInfo == null) {
        GNS.getLogger().warning("TIME EXCEEDED: UPDATE INFO IS NULL!!: " + getPacket());
        return true;
      }
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().fine("Request FAILED no response until MAX-wait time: " + getLnsRequestID() + " name = " + getName());
      }
      try {
        ConfirmUpdatePacket confirmPkt = new ConfirmUpdatePacket(updateInfo.getErrorMessage());
        Update.sendConfirmUpdatePacketBackToSource(confirmPkt, handler);
      } catch (JSONException e) {
        e.printStackTrace();
        GNS.getLogger().severe("Problem converting packet to JSON: " + e);
      }

      updateInfo.setSuccess(false);
      updateInfo.setFinishTime();
      updateInfo.addEventCode(LNSEventCode.MAX_WAIT_ERROR);
      String updateStats = updateInfo.getLogString();
      GNS.getStatLogger().info(updateStats);

      return true;
    }
    return false;
  }

  private int selectNS() {
    return handler.getClosestReplicaController(getName(), replicaControllersQueried);
  }

  private void sendToNS(int nameServerID) {

    if (nameServerID == -1) {
      GNS.getLogger().fine("ERROR: No more primaries left to query. RETURN. Primaries queried " + replicaControllersQueried);
      return;
    }

    UpdateInfo info = (UpdateInfo) LocalNameServer.getRequestInfo(lnsRequestID);
    if (info != null) info.addEventCode(LNSEventCode.CONTACT_RC);
    replicaControllersQueried.add(nameServerID);

    if (getTimeoutCount() == 0) {
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().fine("Add/remove/upsert Info Added: Id = " + getLnsRequestID());
      }
      updatePacketWithRequestID(getPacket(), getLnsRequestID());
    }
    // create the packet that we'll send to the primary

    if (handler.getParameters().isDebugMode()) {
      GNS.getLogger().fine("Sending request to node: " + nameServerID);
    }

    // and send it off
    try {
      JSONObject jsonToSend = getPacket().toJSONObject();
      handler.sendToNS(jsonToSend, nameServerID);

      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().fine(" Send add/remove/upsert to: " + nameServerID + " Name:" + getName() + " Id:" + getLnsRequestID()
                + " Time:" + System.currentTimeMillis() + " --> " + jsonToSend.toString());
      }
    } catch (JSONException e) {
      e.printStackTrace();
    }
  }


  // Special case code like this screams for using a super class other than BasicPacket
  private void updatePacketWithRequestID(BasicPacket packet, int requestID) {
    switch (packet.getType()) {
      case ADD_RECORD:
        AddRecordPacket addRecordPacket = (AddRecordPacket) packet;
        addRecordPacket.setLNSRequestID(requestID);
        addRecordPacket.setLnsAddress(handler.getNodeAddress());
        break;
      case REMOVE_RECORD:
        RemoveRecordPacket removeRecordPacket = (RemoveRecordPacket) packet;
        removeRecordPacket.setLNSRequestID(requestID);
        removeRecordPacket.setLnsAddress(handler.getNodeAddress());
        break;
    }
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @return the packet
   */
  public BasicPacket getPacket() {
    return packet;
  }

  /**
   * @return the lnsRequestID
   */
  public int getLnsRequestID() {
    return lnsRequestID;
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

}
