/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Abhigyan.
 */
package edu.umass.cs.gns.newApp.clientCommandProcessor;

//import edu.umass.cs.gns.clientCommandProcessor.LNSEventCode;
import edu.umass.cs.gns.clientCommandProcessor.UpdateInfo;
import edu.umass.cs.gns.exceptions.CancelExecutorTaskException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.gns.util.Util;
import java.io.IOException;
import org.json.JSONException;
import java.util.HashSet;
import java.util.TimerTask;

public class SendReconfiguratorPacketTask<NodeIDType> extends TimerTask {

  private final String name;
  private final BasicReconfigurationPacket packet;
  private final HashSet<NodeIDType> reconfiguratorsQueried;
  private int timeoutCount = -1;
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
      timeoutCount = timeoutCount + 1;
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().info("Send Run: Name = " + name + " timeout = " + timeoutCount);
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
    Integer lnsRequestID;
    if ((lnsRequestID = handler.getRequestNameToIDMapping(name)) == null) {
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().info("RequestNameToIDMapping not found. Operation complete. Cancel task.");
      }
      return true;
    } else {
      UpdateInfo updateInfo = (UpdateInfo) handler.getRequestInfo(lnsRequestID);
      if (updateInfo == null) {
        if (handler.getParameters().isDebugMode()) {
          GNS.getLogger().info("UpdateInfo not found. Operation complete. Cancel task.");
        }
        return true;
      }
    }
    return false;
  }

  private boolean isMaxWaitTimeExceeded() {
    if (timeoutCount > 0 && System.currentTimeMillis() - startTime > handler.getParameters().getMaxQueryWaitTime()) {
      Integer lnsRequestID = handler.removeRequestNameToIDMapping(name);
      if (lnsRequestID != null) {
        UpdateInfo updateInfo = (UpdateInfo) handler.getRequestInfo(lnsRequestID);
        if (updateInfo == null) {
          GNS.getLogger().warning("TIME EXCEEDED: UPDATE INFO IS NULL!!: " + packet);
          return true;
        }
        if (handler.getParameters().isDebugMode()) {
          GNS.getLogger().info("Request FAILED no response until MAX-wait time: " + lnsRequestID
                  + " name = " + name);
        }
        updateInfo.setSuccess(false);
        updateInfo.setFinishTime();
        //updateInfo.addEventCode(LNSEventCode.MAX_WAIT_ERROR);
        return true;
      } else {
        if (handler.getParameters().isDebugMode()) {
          GNS.getLogger().info("Ignoring spurious retransmission timeout for " + name);
        }
      }
    }
    return false;
  }

  private NodeIDType selectReconfigurator() {
    return handler.getClosestReplicaController(name, reconfiguratorsQueried);
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
    } catch (IOException | JSONException e) {
      GNS.getLogger().severe("Problem sending packet to " + nameServerID + ": " + e);
      e.printStackTrace();
    }

  }

}
