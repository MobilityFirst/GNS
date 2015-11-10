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
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor;

import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.UpdateInfo;
import edu.umass.cs.gnsserver.exceptions.CancelExecutorTaskException;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.utils.Util;
import edu.umass.cs.reconfiguration.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;

import java.io.IOException;

import org.json.JSONException;

import java.util.HashSet;
import java.util.TimerTask;

/**
 * Sends packets to reconfigurators with retransmission.
 * 
 * @author westy
 */
public class SendReconfiguratorPacketTask extends TimerTask {

  private final String name;
  private final BasicReconfigurationPacket packet;
  private final HashSet<String> reconfiguratorsQueried;
  private int sendCount = -1;
  private int retries = 0;
  private final int maxRetries;
  private final int maxWaitTime;
  private final long startTime;
  private final ClientRequestHandlerInterface handler;

  /**
   * Creates an instance of SendReconfiguratorPacketTask.
   * 
   * @param name
   * @param packet
   * @param handler
   * @param maxWaitTime
   * @param maxRetries
   */
  public SendReconfiguratorPacketTask(String name, BasicReconfigurationPacket packet,
          ClientRequestHandlerInterface handler, 
          int maxWaitTime, int maxRetries) {
    this.name = name;
    this.handler = handler;
    this.packet = packet;
    this.reconfiguratorsQueried = new HashSet<String>();
    this.startTime = System.currentTimeMillis();
    this.maxWaitTime = maxWaitTime;
    this.maxRetries = maxRetries;
  }

  @Override
  public void run() {
    try {
      sendCount++;
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().info("??????????????????????????? Name = " + name + " packet type = " + packet.getType() 
                + " sent = " + sendCount + " retries = " + retries);
      }

      if (isResponseReceived() || isMaxWaitTimeExceeded()) {
        throw new CancelExecutorTaskException();
      }

      String nameServerID = selectReconfigurator();

      sendRequestToReconfigurator(nameServerID);

    } catch (Exception e) {
      // we catch all exceptions here because executor service will not print any exception messages
      if (e.getClass().equals(CancelExecutorTaskException.class)) {
        // this exception is only way to terminate this task from repeat execution
        throw new RuntimeException();
      }
      // FIXME: This should actually return an error result back to the caller that invoked this
      // otherwise it just results in a random timeout back at the client.
      GNS.getLogger().severe("Problem in SendReconfiguratorPacketTask: " + e);
      e.printStackTrace();
    }
  }

  private boolean isResponseReceived() {
    Integer lnsRequestID = getRequestNameToIDMapping(name);
    if (lnsRequestID == null) {
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().info("??????????????????????????? Name = " + name + " packet type = " + packet.getType() + " info not found. Operation complete. Cancel task.");
      }
      return true;
    } else {
      @SuppressWarnings("unchecked")
      UpdateInfo<String> updateInfo = (UpdateInfo<String>) handler.getRequestInfo(lnsRequestID);
      if (updateInfo == null) {
        if (handler.getParameters().isDebugMode()) {
          GNS.getLogger().info("??????????????????????????? Name = " + name + " packet type = " + packet.getType() + " UpdateInfo<String> not found. Operation complete. Cancel task.");
        }
        return true;
      }
    }
    if (handler.getParameters().isDebugMode()) {
      GNS.getLogger().info("??????????????????????????? Name = " + name + " packet type = " + packet.getType() + " no response yet.");
    }
    return false;
  }

  private boolean isMaxWaitTimeExceeded() {
    if (sendCount > 0 && System.currentTimeMillis() - startTime > maxWaitTime) {
      Integer ccpRequestID = removeRequestNameToIDMapping(name);
      if (ccpRequestID != null) {
        @SuppressWarnings("unchecked")
        UpdateInfo<String> updateInfo = (UpdateInfo<String>) handler.getRequestInfo(ccpRequestID);
        if (updateInfo == null) {
          GNS.getLogger().warning("??????????????????????????? Name = " + name + " packet type = " + packet.getType() + " TIME EXCEEDED: UPDATE INFO IS NULL!!: " + packet);
          return true;
        }
        if (handler.getParameters().isDebugMode()) {
          GNS.getLogger().info("??????????????????????????? Name = " + name + " packet type = " + packet.getType()
                  + " Request " + ccpRequestID + " FAILED; no response and MAX-wait exceeded ");
        }
        updateInfo.setSuccess(false);
        updateInfo.setFinishTime();
        return true;
      } else {
        if (handler.getParameters().isDebugMode()) {
          GNS.getLogger().info("Ignoring spurious retransmission timeout for " + name + " packet type = " + packet.getType());
        }
      }
    }
    return false;
  }

  private String selectReconfigurator() {
    String server = handler.getClosestReplicaController(name, reconfiguratorsQueried);
    if (server == null) {
      if (retries < maxRetries) {
        reconfiguratorsQueried.clear();
        server = handler.getClosestReplicaController(name, reconfiguratorsQueried);
        retries++;
        if (handler.getParameters().isDebugMode()) {
          GNS.getLogger().info("??????????????????????????? Name = " + name + " packet type = " + packet.getType() + " retry = " + retries);
        }
      }
    }
    return server;
  }

  private void sendRequestToReconfigurator(String nameServerID) {
    if (nameServerID == null) {
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().info("ERROR: No more primaries left to query. RETURN. Primaries queried "
                + Util.setOfNodeIdToString(reconfiguratorsQueried));
      }
      return;
    }
    reconfiguratorsQueried.add(nameServerID);
    try {
      handler.sendRequestToReconfigurator(packet, nameServerID);
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().info("??????????????????????????? Name = " + name + " packet type = " + packet.getType() + " SENT TO " + nameServerID);
      }
    } catch (IOException | JSONException e) {
      GNS.getLogger().severe("Problem sending packet to " + nameServerID + ": " + e);
      e.printStackTrace();
    }
  }
  
  private Integer getRequestNameToIDMapping(String serviceName) {
    Integer ccpRequestId = null;
    if (packet.getType().equals(ReconfigurationPacket.PacketType.CREATE_SERVICE_NAME)) {
      //GNS.getLogger().info("PACKET TYPE: " + packet.getType());
      ccpRequestId = handler.getCreateRequestNameToIDMapping(name);
    } else if (packet.getType().equals(ReconfigurationPacket.PacketType.DELETE_SERVICE_NAME)) {
      //GNS.getLogger().info("PACKET TYPE: " + packet.getType());
      ccpRequestId = handler.getDeleteRequestNameToIDMapping(name);
    } else if (packet.getType().equals(ReconfigurationPacket.PacketType.REQUEST_ACTIVE_REPLICAS)) {
      //GNS.getLogger().info("PACKET TYPE: " + packet.getType());
      ccpRequestId = handler.getActivesRequestNameToIDMapping(name);
    } else {
      GNS.getLogger().warning("BAD PACKET TYPE: " + packet.getType());
    }
    return ccpRequestId;
  }

  private Integer removeRequestNameToIDMapping(String serviceName) {
    Integer ccpRequestID = null;
    if (packet.getType().equals(ReconfigurationPacket.PacketType.CREATE_SERVICE_NAME)) {
      ccpRequestID = handler.removeCreateRequestNameToIDMapping(serviceName);
    } else if (packet.getType().equals(ReconfigurationPacket.PacketType.DELETE_SERVICE_NAME)) {
      ccpRequestID = handler.removeDeleteRequestNameToIDMapping(serviceName);
    } else if (packet.getType().equals(ReconfigurationPacket.PacketType.REQUEST_ACTIVE_REPLICAS)) {
      ccpRequestID = handler.removeActivesRequestNameToIDMapping(serviceName);
    } else {
      GNS.getLogger().warning("BAD PACKET TYPE: " + packet.getType());
    }
    return ccpRequestID;
  }

}
