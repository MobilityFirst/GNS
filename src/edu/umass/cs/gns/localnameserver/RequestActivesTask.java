/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.exceptions.CancelExecutorTaskException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.RequestActiveReplicas;
import java.io.IOException;
import java.net.InetSocketAddress;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.Set;
import java.util.TimerTask;

/**
 * Send request to primary (replica controllers) to obtain set of actives (including retransmissions).
 *
 * The repeat execution is cancelled in two cases:
 * (1) local name server receives a response from one of the replica controllers.
 * (2) no response is received until max wait time. in this case, we send error messages for all pending requests
 * for this name.
 *
 */
public class RequestActivesTask extends TimerTask {

  /**
   * number of messages sent to replica controllers
   */
  private int numAttempts = 0;
  private final String name;
  private final Set<InetSocketAddress> nameServersQueried;
  private final int requestID;
  private final RequestHandlerInterface handler;
  private final long startTime;

  public RequestActivesTask(String name, int requestID, RequestHandlerInterface handler) {
    this.name = name;
    this.nameServersQueried = new HashSet<InetSocketAddress>();
    this.requestID = requestID;
    this.startTime = System.currentTimeMillis();
    this.handler = handler;
  }

  @Override
  public void run() {
    try {
      numAttempts++;
      // check whether actives received
      if (handler.getPendingTasks().isReplyReceived(requestID)) {
        if (handler.isDebugMode()) {
          GNS.getLogger().info("Reply received for requestID " + requestID);
        }
        throw new CancelExecutorTaskException();
      }

      if (numAttempts > GNS.numPrimaryReplicas) {
        GNS.getLogger().warning("Error: No actives received for name: " + name + " after " + numAttempts + " attempts.");
        if (System.currentTimeMillis() - startTime > LocalNameServer.MAX_QUERY_WAIT_TIME) {
          // max number of attempts have been made,
          GNS.getLogger().severe("Error: No actives received for name  " + name + " after " + numAttempts
                  + " attempts.");
          handler.getPendingTasks().sendErrorMsgForName(name, requestID, handler);
          throw new CancelExecutorTaskException();
        }
      }

      // next primary to be queried
      InetSocketAddress primaryAddress = handler.getClosestServer(
              handler.getNodeConfig().getReplicatedReconfigurators(name), nameServersQueried);
      if (primaryAddress == null) {
        // we clear this set to resend requests to the same set of name servers
        nameServersQueried.clear();
        primaryAddress = handler.getClosestServer(
                handler.getNodeConfig().getReplicatedReconfigurators(name), nameServersQueried);
        if (primaryAddress == null) {
          GNS.getLogger().severe("No primary NS available. name = " + name);
          throw new CancelExecutorTaskException();
        }

      }
      nameServersQueried.add(primaryAddress);
      // send packet to primary
      sendActivesRequestPacketToPrimary(name, primaryAddress);
    } catch (Exception e) { // we catch all possible exceptions because executor service does not print message on exception
      if (e.getClass().equals(CancelExecutorTaskException.class)) {
        throw new RuntimeException();
      }
      // all exceptions other than CancelExecutorTaskException are logged.
      GNS.getLogger().severe("Unexpected exception in main active request loop: " + e);
      e.printStackTrace();
    }
  }

  /**
   * Send a request to primary to send actives.
   *
   * @param name name for which actives are requested
   * @param primaryID ID of name server
   * @param requestID requestID for <code>RequestActivesPacket</code>
   */
  private void sendActivesRequestPacketToPrimary(String name, InetSocketAddress primaryAddress) {
    JSONObject sendJson;
    try {
      RequestActiveReplicas packet = new RequestActiveReplicas(null, name, 0);
      sendJson = packet.toJSONObject();
      handler.getTcpTransport().sendToAddress(primaryAddress, sendJson);
      if (handler.isDebugMode()) {
        GNS.getLogger().info("Send RequestActivesPacket for " + name + " to " + primaryAddress.toString());
      }
    } catch (JSONException|IOException e) {
      GNS.getLogger().severe("Problem sending RequestActives packet for " + name + ": " + e);
    } 

  }

}
