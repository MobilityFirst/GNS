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
import edu.umass.cs.reconfiguration.reconfigurationpackets.RequestActiveReplicas;

import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.TimerTask;

/**
 * Send request to primary (replica controllers) to obtain set of actives (including retransmissions).
 *
 * The repeat execution is cancelled in two cases:
 * (1) local name server receives a response from one of the replica controllers.
 * (2) no response is received until max wait time. in this case, we send error messages for all pending requests
 * for this name.
 *
 * @see edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.PendingTasks
 * @see edu.umass.cs.gnsserver.gnsApp.packet.RequestActivesPacket
 *
 * @author abhigyan
 */
public class RequestActivesTask extends TimerTask {

  /**
   * number of messages sent to replica controllers
   */
  private int numAttempts = 0;
  private String name;
  private HashSet<String> nameServersQueried;
  private int requestID;
  ClientRequestHandlerInterface handler;

  private long startTime;

  /**
   *
   * @param name
   * @param requestID
   * @param handler
   */
  public RequestActivesTask(String name, int requestID, ClientRequestHandlerInterface handler) {
    this.name = name;
    this.nameServersQueried = new HashSet<String>();
    this.requestID = requestID;
    this.startTime = System.currentTimeMillis();
    this.handler = handler;
  }

  @Override
  public void run() {
    try {

      numAttempts++;
      // check whether actives received
      if (PendingTasks.isReplyReceived(requestID)) {
        if (handler.getParameters().isDebugMode()) {
          GNS.getLogger().info("Reply received for requestID " + requestID);
        }

        throw new CancelExecutorTaskException();
      }

      if (numAttempts > GNS.numPrimaryReplicas) {
        GNS.getLogger().warning("Error: No actives received for name: " + name + " after " + numAttempts + " attempts.");
        if (System.currentTimeMillis() - startTime > AppReconfigurableNodeOptions.maxQueryWaitTime) {
          // max number of attempts have been made,
          GNS.getLogger().severe("Error: No actives received for name  " + name + " after " + numAttempts
                  + " attempts.");
          PendingTasks.sendErrorMsgForName(name, requestID,
                  //LNSEventCode.RC_NO_RESPONSE_ERROR, 
                  handler);
          throw new CancelExecutorTaskException();
        }
      }

      // next primary to be queried
      String primaryID = handler.getClosestReplicaController(name, nameServersQueried);
      if (primaryID == null) {
        // we clear this set to resend requests to the same set of name servers
        nameServersQueried.clear();
        primaryID = handler.getClosestReplicaController(name, nameServersQueried);
        if (primaryID == null) {
          GNS.getLogger().severe("No primary NS available. name = " + name);
          throw new CancelExecutorTaskException();
        }

      }
      nameServersQueried.add(primaryID);
      // send packet to primary
      sendActivesRequestPacketToPrimary(name, primaryID, requestID);
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
  private void sendActivesRequestPacketToPrimary(String name, String primaryID, int requestID) {
    JSONObject sendJson;
    try {
      RequestActiveReplicas packet = new RequestActiveReplicas(null, name, 0);
      ((ClientRequestHandlerInterface) handler).addActivesRequestNameToIDMapping(name, requestID);
      sendJson = packet.toJSONObject();
      handler.sendToNS(sendJson, primaryID);
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().info("Send RequestActivesPacket for " + name + " to " + primaryID.toString());
      }
    } catch (JSONException e) {
      GNS.getLogger().severe("JSON Exception in sending RequestActives packet for " + name + ": " + e);
    }

  }

}
