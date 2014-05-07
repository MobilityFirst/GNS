package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.exceptions.CancelExecutorTaskException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nsdesign.packet.*;
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
 * @see edu.umass.cs.gns.localnameserver.PendingTasks
 * @see edu.umass.cs.gns.packet.RequestActivesPacket
 */
public class RequestActivesTask extends TimerTask {

  /**number of messages sent to replica controllers*/
  private int numAttempts = 0;
  private String name;
  private HashSet<Integer> nameServersQueried;
  private int requestID;

  private long startTime;
  public RequestActivesTask(String name, int requestID) {
    this.name = name;
    this.nameServersQueried = new HashSet<Integer>();
    this.requestID = requestID;
    this.startTime = System.currentTimeMillis();
  }

  @Override
  public void run()  {
    try {

      numAttempts++;
      // check whether actives received
      if (PendingTasks.isReplyReceived(requestID))  {
        GNS.getLogger().fine("Reply received for requestID " + requestID);

        throw  new CancelExecutorTaskException();
      }

      if (numAttempts > GNS.numPrimaryReplicas) {
        GNS.getLogger().warning("Error: No actives received for name: " + name + " after " + numAttempts + " attempts.");
        if (System.currentTimeMillis() - startTime > StartLocalNameServer.maxQueryWaitTime) {
          // max number of attempts have been made,
          GNS.getLogger().severe("Error: No actives received for name  " + name + " after " + numAttempts +
                  " attempts.");
          PendingTasks.sendErrorMsgForName(name, requestID);
          throw  new CancelExecutorTaskException();
        }
      }

      // next primary to be queried
      int primaryID = LocalNameServer.getClosestReplicaController(name, nameServersQueried);
      if (primaryID == -1) {
        // we clear this set to resend requests to the same set of name servers
        nameServersQueried.clear();
        primaryID = LocalNameServer.getClosestReplicaController(name, nameServersQueried);
        if (primaryID == -1) {
          GNS.getLogger().severe("No primary NS available. name = " + name);
          throw  new CancelExecutorTaskException();
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
   * send request to primary to send actives
   * @param name name for which actives are requested
   * @param primaryID ID of name server
   * @param requestID requestID for <code>RequestActivesPacket</code>
   */
  private void sendActivesRequestPacketToPrimary(String name, int primaryID, int requestID) {

    RequestActivesPacket packet = new RequestActivesPacket(name, LocalNameServer.getNodeID(), requestID);
    try
    {
      JSONObject sendJson = packet.toJSONObject();
      LocalNameServer.sendToNS(sendJson,primaryID);
      if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Send Active Request Packet to Primary. " + primaryID
              + "\tname\t" + name);
    } catch (JSONException e)
    {
      if (StartLocalNameServer.debugMode) GNS.getLogger().fine("JSON Exception in sending packet. name\t" + name);
      e.printStackTrace();
    }

  }


}

