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

import edu.umass.cs.gnsserver.exceptions.CancelExecutorTaskException;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.gnsApp.AppReconfigurableNodeOptions;
import edu.umass.cs.gnsserver.gnsApp.packet.AddRecordPacket;
import edu.umass.cs.gnsserver.gnsApp.packet.BasicPacket;
import edu.umass.cs.gnsserver.gnsApp.packet.ConfirmUpdatePacket;
import static edu.umass.cs.gnsserver.gnsApp.packet.Packet.PacketType.*;
import edu.umass.cs.gnsserver.gnsApp.packet.RemoveRecordPacket;
import edu.umass.cs.gnsserver.gnsApp.NSResponseCode;
import edu.umass.cs.gnsserver.utils.Util;
import org.json.JSONException;
import org.json.JSONObject;
import java.util.HashSet;
import java.util.TimerTask;

/**
 * Sends three types of messages (with retries): AddRecordPacket, RemoveRecordPacket, and
 * UpdateRecordPacket to replica controllers. These messages are sent one by one to all
 * replica controllers in order of their distance until
 * (1) CPP receives a response from one of the primary replicas.
 * (2) no response is received until {@code maxQueryWaitTime}.
 * In this case, an error response is sent to client.
 *
 * User: abhigyan
 * Date: 8/9/13
 * Time: 4:59 PM
 */
public class SendAddRemoveTask extends TimerTask {

  private final String name;
  private final BasicPacket packet;
  private final int lnsRequestID;
  private final HashSet<String> replicaControllersQueried;
  private int timeoutCount = -1;
  private final long requestRecvdTime;
  private final ClientRequestHandlerInterface handler;

  /**
   *
   * @param lnsRequestID
   * @param handler
   * @param packet
   * @param name
   * @param requestRecvdTime
   */
  public SendAddRemoveTask(int lnsRequestID, ClientRequestHandlerInterface handler, BasicPacket packet, String name, long requestRecvdTime) {
    this.name = name;
    this.handler = handler;
    this.packet = packet;
    this.lnsRequestID = lnsRequestID;
    this.replicaControllersQueried = new HashSet<String>();
    this.requestRecvdTime = requestRecvdTime;
  }

  @Override
  public void run() {
    try {
      timeoutCount = timeoutCount + 1;
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().info("ENTER name = " + getName() + " timeout = " + getTimeoutCount());
      }

      if (isResponseReceived() || isMaxWaitTimeExceeded()) {
        throw new CancelExecutorTaskException();
      }

      String nameServerID = selectNS();

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
    if (handler.getRequestInfo(getLnsRequestID()) == null) {
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().info("UpdateInfo<String> not found. Either update complete or invalid actives. Cancel task.");
      }
      return true;
    }
    return false;
  }

  private boolean isMaxWaitTimeExceeded() {
    if (getTimeoutCount() > 0 && System.currentTimeMillis() - getRequestRecvdTime() > handler.getParameters().getMaxQueryWaitTime()) {
      @SuppressWarnings("unchecked")
      UpdateInfo<String> updateInfo = (UpdateInfo<String>) handler.removeRequestInfo(getLnsRequestID());

      if (updateInfo == null) {
        GNS.getLogger().warning("TIME EXCEEDED: UPDATE INFO IS NULL!!: " + getPacket());
        return true;
      }
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().info("Request FAILED no response until MAX-wait time: " + getLnsRequestID() + " name = " + getName());
      }
      try {
        ConfirmUpdatePacket<String> confirmPkt = new ConfirmUpdatePacket<String>(updateInfo.getErrorMessage(NSResponseCode.UPDATE_TIMEOUT), handler.getGnsNodeConfig());
        Update.sendConfirmUpdatePacketBackToSource(confirmPkt, handler);
      } catch (JSONException e) {
        e.printStackTrace();
        GNS.getLogger().severe("Problem converting packet to JSON: " + e);
      }

      updateInfo.setSuccess(false);
      updateInfo.setFinishTime();

      return true;
    }
    return false;
  }

  private String selectNS() {
    return handler.getClosestReplicaController(getName(), replicaControllersQueried);
  }

  private void sendToNS(String nameServerID) {

    if (nameServerID == null) {
      if (AppReconfigurableNodeOptions.debuggingEnabled) {
        GNS.getLogger().info("ERROR: No more primaries left to query. RETURN. Primaries queried "
                + Util.setOfNodeIdToString(replicaControllersQueried));
      }
      return;
    }
    @SuppressWarnings("unchecked")
    UpdateInfo<String> info = (UpdateInfo<String>) handler.getRequestInfo(lnsRequestID);
//    if (info != null) {
//      info.addEventCode(LNSEventCode.CONTACT_RC);
//    }
    replicaControllersQueried.add(nameServerID);

    if (getTimeoutCount() == 0) {
      if (handler.getParameters().isDebugMode() || AppReconfigurableNodeOptions.debuggingEnabled) {
        GNS.getLogger().info("Add/remove/upsert Info Added: Id = " + getLnsRequestID());
      }
      updatePacketWithRequestID(getPacket(), getLnsRequestID());
    }
    // create the packet that we'll send to the primary

    if (handler.getParameters().isDebugMode() || AppReconfigurableNodeOptions.debuggingEnabled) {
      GNS.getLogger().info("Sending request to node: " + nameServerID.toString());
    }

    // and send it off
    try {
      JSONObject jsonToSend = getPacket().toJSONObject();
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().info(" Send add/remove/upsert to: " + nameServerID.toString() + " Name:" + getName() + " Id:" + getLnsRequestID()
                + " Time:" + System.currentTimeMillis() + " --> " + jsonToSend.toString());
      }
      handler.sendToNS(jsonToSend, nameServerID);
    } catch (JSONException e) {
      e.printStackTrace();
    }
  }

  // Special case code like this screams for using a super class other than BasicPacket
  private void updatePacketWithRequestID(BasicPacket packet, int requestID) {
    switch (packet.getType()) {
      case ADD_RECORD:
        AddRecordPacket addRecordPacket = (AddRecordPacket) packet;
        addRecordPacket.setCCPRequestID(requestID);
        addRecordPacket.setCcpAddress(handler.getNodeAddress());
        break;
      case REMOVE_RECORD:
        RemoveRecordPacket removeRecordPacket = (RemoveRecordPacket) packet;
        removeRecordPacket.setCCPRequestID(requestID);
        removeRecordPacket.setCcpAddress(handler.getNodeAddress());
        break;
    }
  }

  /**
   * Returns the name.
   * 
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * Returns the packet.
   * 
   * @return the packet
   */
  public BasicPacket getPacket() {
    return packet;
  }

  /**
   * Returns the request id.
   * 
   * @return the lnsRequestID
   */
  public int getLnsRequestID() {
    return lnsRequestID;
  }

  /**
   * Returns the timeout count.
   * 
   * @return the timeoutCount
   */
  public int getTimeoutCount() {
    return timeoutCount;
  }

  /**
   * Returns the received time.
   * 
   * @return the requestRecvdTime
   */
  public long getRequestRecvdTime() {
    return requestRecvdTime;
  }

}
