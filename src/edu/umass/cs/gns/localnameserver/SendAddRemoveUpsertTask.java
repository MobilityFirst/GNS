/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Abhigyan.
 */
package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.client.Intercessor;
import edu.umass.cs.gns.exceptions.CancelExecutorTaskException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.packet.*;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.TimerTask;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Sends three types of messages (with retries): AddRecordPacket, RemoveRecordPacket, and
 * UpdateAddressPacket with upsert.  These messages are sent one by one to all primaries in order of their distance.
 * If no response is received until {@code maxQueryWaitTime}, an error response is sent to client.
 * User: abhigyan
 * Date: 8/9/13
 * Time: 4:59 PM
 */
public class SendAddRemoveUpsertTask extends TimerTask {

  private String name;
  private BasicPacket packet;
  private InetAddress senderAddress;
  private int senderPort;
  private int updateRequestID;
  private HashSet<Integer> primariesQueried;
  private int timeoutCount = -1;
  private long requestRecvdTime;

  public SendAddRemoveUpsertTask(BasicPacket packet, String name,
                                 InetAddress senderAddress, int senderPort, long requestRecvdTime,
                                 HashSet<Integer> primariesQueried) {
    this.name = name;
    this.packet = packet;
    this.senderAddress = senderAddress;
    this.senderPort = senderPort;
    this.primariesQueried = primariesQueried;
    this.requestRecvdTime = requestRecvdTime;
  }

  @Override
  public void run() {
    try{
      timeoutCount = timeoutCount + 1;
      if (StartLocalNameServer.debugMode) {
        GNS.getLogger().fine("ENTER name = " + getName() + " timeout = " + getTimeoutCount());
      }

      if (getTimeoutCount() > 0 && LocalNameServer.getUpdateInfo(getUpdateRequestID()) == null) {
        if (StartLocalNameServer.debugMode) {
          GNS.getLogger().fine("UpdateInfo not found. Either update complete or invalid actives. Cancel task.");
        }
        throw new CancelExecutorTaskException();
      }

      if (getTimeoutCount() > 0 && System.currentTimeMillis() - getRequestRecvdTime() > StartLocalNameServer.maxQueryWaitTime) {
        UpdateInfo updateInfo = LocalNameServer.removeUpdateInfo(getUpdateRequestID());

        if (updateInfo == null) {
          GNS.getLogger().warning("TIME EXCEEDED: UPDATE INFO IS NULL!!: " + getPacket());
          throw new CancelExecutorTaskException();
        }
        GNS.getLogger().fine("ADD FAILED no response until MAX-wait time: " + getUpdateRequestID() + " name = " + getName());
        ConfirmUpdateLNSPacket confirmPkt = getConfirmFailurePacket(getPacket());
        try {
          if (confirmPkt != null) {
            Intercessor.handleIncomingPackets(confirmPkt.toJSONObject());
          } else {
            GNS.getLogger().warning("ERROR: Confirm update is NULL. Cannot sent response to client.");
          }
        } catch (JSONException e) {
          GNS.getLogger().severe("Problem converting packet to JSON: " + e);
        }
        String updateStats = updateInfo.getUpdateFailedStats(getPrimariesQueried(), LocalNameServer.nodeID, getUpdateRequestID(), -1);
        GNS.getStatLogger().fine(updateStats);

        throw new CancelExecutorTaskException();
      }
      if (getPrimariesQueried().size() == GNS.numPrimaryReplicas) {
        getPrimariesQueried().clear();
      }
      int nameServerID = LocalNameServer.getClosestPrimaryNameServer(getName(), getPrimariesQueried());

      if (nameServerID == -1) {
        GNS.getLogger().info("ERROR: No more primaries left to query. RETURN. Primaries queried " + getPrimariesQueried());
        return;
      } else {
        getPrimariesQueried().add(nameServerID);
      }
      if (getTimeoutCount() == 0) {
        String hostAddress = null;
        if (getSenderAddress() != null) {
          hostAddress = getSenderAddress().getHostAddress();
        }
        updateRequestID = LocalNameServer.addUpdateInfo(getName(), nameServerID, getRequestRecvdTime(), 0, null);
        GNS.getLogger().info("Update Info Added: Id = " + getUpdateRequestID());
        updatePacketWithRequestID(getPacket(), getUpdateRequestID());
      }
      // create the packet that we'll send to the primary

      GNS.getLogger().info("Sending Update to Node: " + nameServerID);

      // and send it off
      try {
        JSONObject jsonToSend = getPacket().toJSONObject();
        LocalNameServer.sendToNS(jsonToSend, nameServerID);

        GNS.getLogger().info("SendAddRequest: Send to: " + nameServerID + " Name:" + getName() + " Id:" + getUpdateRequestID() +
                " Time:" + System.currentTimeMillis() + " --> " + jsonToSend.toString());
      } catch (JSONException e) {
        e.printStackTrace();
      }
    }catch (Exception e) {
      // we catch all exceptions to print stack trace because executor service will not print any error in case
      // of exception
      if (e.getClass().equals(CancelExecutorTaskException.class)) {
      // this exception is only to terminate this task from repeat execution; this is the only way we can terminate this task

        throw new RuntimeException();
      }
      GNS.getLogger().severe("Exception Exception Exception ... ");
      e.printStackTrace();
    }
  }

  // This code screams for using a super class other than BasicPacket
  private ConfirmUpdateLNSPacket getConfirmFailurePacket(BasicPacket packet) {
    ConfirmUpdateLNSPacket confirm;
    switch (packet.getType()) {
      case ADD_RECORD_LNS:
        confirm = new ConfirmUpdateLNSPacket(false, (AddRecordPacket) packet);
        return confirm;
      case REMOVE_RECORD_LNS:
        confirm = new ConfirmUpdateLNSPacket(false, (RemoveRecordPacket) packet);
        return confirm;
      case UPDATE_ADDRESS_LNS:
        confirm = ConfirmUpdateLNSPacket.createFailPacket((UpdateAddressPacket) packet);
        return confirm;
    }
    return null;
  }

  // This code screams for using a super class other than BasicPacket
  private void updatePacketWithRequestID(BasicPacket packet, int requestID) {

    switch (packet.getType()) {
      case ADD_RECORD_LNS:
        AddRecordPacket addRecordPacket = (AddRecordPacket) packet;
        addRecordPacket.setLNSRequestID(requestID);
        break;
      case REMOVE_RECORD_LNS:
        RemoveRecordPacket removeRecordPacket = (RemoveRecordPacket) packet;
        removeRecordPacket.setLNSRequestID(requestID);
        break;
      case UPDATE_ADDRESS_LNS:
        UpdateAddressPacket updateAddressPacket = (UpdateAddressPacket) packet;
        updateAddressPacket.setLNSRequestID(requestID);
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
   * @return the senderAddress
   */
  public InetAddress getSenderAddress() {
    return senderAddress;
  }

  /**
   * @return the senderPort
   */
  public int getSenderPort() {
    return senderPort;
  }

  /**
   * @return the updateRequestID
   */
  public int getUpdateRequestID() {
    return updateRequestID;
  }

  /**
   * @return the primariesQueried
   */
  public HashSet<Integer> getPrimariesQueried() {
    return primariesQueried;
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
