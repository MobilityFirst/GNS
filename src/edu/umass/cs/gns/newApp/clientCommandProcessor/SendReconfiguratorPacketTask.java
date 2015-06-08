/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Abhigyan.
 */
package edu.umass.cs.gns.newApp.clientCommandProcessor;

import edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport.UpdateInfo;
import edu.umass.cs.gns.exceptions.CancelExecutorTaskException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.util.Util;
import edu.umass.cs.reconfiguration.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;

import java.io.IOException;

import org.json.JSONException;

import java.util.HashSet;
import java.util.TimerTask;

public class SendReconfiguratorPacketTask<NodeIDType> extends TimerTask {

  private final String name;
  private final BasicReconfigurationPacket packet;
  private final HashSet<NodeIDType> reconfiguratorsQueried;
  private int sendCount = -1;
  private int retries = 0;
  private static final int MAX_RETRIES = 10;
  private final long startTime;
  private final EnhancedClientRequestHandlerInterface<NodeIDType> handler;

  public SendReconfiguratorPacketTask(String name, BasicReconfigurationPacket packet,
          EnhancedClientRequestHandlerInterface<NodeIDType> handler) {
    this.name = name;
    this.handler = handler;
    this.packet = packet;
    this.reconfiguratorsQueried = new HashSet<NodeIDType>();
    this.startTime = System.currentTimeMillis();
  }

  @Override
  public void run() {
    try {
      sendCount++;
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().info("??????????????????????????? Name = " + name + " packet type = " + packet.getType() + " sent = " + sendCount + " retries = " + retries);
      }

      if (isResponseReceived() || isMaxWaitTimeExceeded()) {
        throw new CancelExecutorTaskException();
      }

      NodeIDType nameServerID = selectReconfigurator();

      sendRequestToReconfigurator(nameServerID);

    } catch (Exception e) {
      // we catch all exceptions here because executor service will not print any exception messages
      if (e.getClass().equals(CancelExecutorTaskException.class)) {
        // this exception is only way to terminate this task from repeat execution
        throw new RuntimeException();
      }
      GNS.getLogger().severe("Problem in SendReconfiguratorPacketTask: " + e);
      e.printStackTrace();
    }
  }

  private boolean isResponseReceived() {
    Integer lnsRequestID = null;
    if (packet.getType().equals(ReconfigurationPacket.PacketType.CREATE_SERVICE_NAME)) {
      //GNS.getLogger().info("PACKET TYPE: " + packet.getType());
      lnsRequestID = handler.getCreateRequestNameToIDMapping(name);
    } else if (packet.getType().equals(ReconfigurationPacket.PacketType.DELETE_SERVICE_NAME)) {
      //GNS.getLogger().info("PACKET TYPE: " + packet.getType());
      lnsRequestID = handler.getDeleteRequestNameToIDMapping(name);
    } else if (packet.getType().equals(ReconfigurationPacket.PacketType.REQUEST_ACTIVE_REPLICAS)) {
      //GNS.getLogger().info("PACKET TYPE: " + packet.getType());
      lnsRequestID = handler.getActivesRequestNameToIDMapping(name);
    } else {
      GNS.getLogger().warning("BAD PACKET TYPE: " + packet.getType());
    }
    if (lnsRequestID == null) {
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().info("??????????????????????????? Name = " + name + " packet type = " + packet.getType() + " info not found. Operation complete. Cancel task.");
      }
      return true;
    } else {
      UpdateInfo updateInfo = (UpdateInfo) handler.getRequestInfo(lnsRequestID);
      if (updateInfo == null) {
        if (handler.getParameters().isDebugMode()) {
          GNS.getLogger().info("??????????????????????????? Name = " + name + " packet type = " + packet.getType() + " UpdateInfo not found. Operation complete. Cancel task.");
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
    if (sendCount > 0 && System.currentTimeMillis() - startTime > handler.getParameters().getMaxQueryWaitTime()) {
      Integer lnsRequestID = null;
      if (packet.getType().equals(ReconfigurationPacket.PacketType.CREATE_SERVICE_NAME)) {
        lnsRequestID = handler.removeCreateRequestNameToIDMapping(name);
      } else if (packet.getType().equals(ReconfigurationPacket.PacketType.DELETE_SERVICE_NAME)) {
        lnsRequestID = handler.removeDeleteRequestNameToIDMapping(name);
      } else if (packet.getType().equals(ReconfigurationPacket.PacketType.REQUEST_ACTIVE_REPLICAS)) {
        lnsRequestID = handler.removeActivesRequestNameToIDMapping(name);
      } else {
        GNS.getLogger().warning("BAD PACKET TYPE: " + packet.getType());
      }
      //Integer lnsRequestID = handler.removeCreateRequestNameToIDMapping(name);
      if (lnsRequestID != null) {
        UpdateInfo updateInfo = (UpdateInfo) handler.getRequestInfo(lnsRequestID);
        if (updateInfo == null) {
          GNS.getLogger().warning("??????????????????????????? Name = " + name + " packet type = " + packet.getType() + " TIME EXCEEDED: UPDATE INFO IS NULL!!: " + packet);
          return true;
        }
        if (handler.getParameters().isDebugMode()) {
          GNS.getLogger().info("??????????????????????????? Name = " + name + " packet type = " + packet.getType()
                  + " Request FAILED no response until MAX-wait time: " + lnsRequestID);
        }
        updateInfo.setSuccess(false);
        updateInfo.setFinishTime();
        //updateInfo.addEventCode(LNSEventCode.MAX_WAIT_ERROR);
        return true;
      } else {
        if (handler.getParameters().isDebugMode()) {
          GNS.getLogger().info("Ignoring spurious retransmission timeout for " + name + " packet type = " + packet.getType());
        }
      }
    }
    return false;
  }

  private NodeIDType selectReconfigurator() {
    NodeIDType server = handler.getClosestReplicaController(name, reconfiguratorsQueried);
    if (server == null) {
      if (retries < MAX_RETRIES) {
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

  private void sendRequestToReconfigurator(NodeIDType nameServerID) {
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

}
