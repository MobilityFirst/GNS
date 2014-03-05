package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.exceptions.CancelExecutorTaskException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.packet.RequestActivesPacket;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.TimerTask;


/**
 * Send request to primary (replica controllers) to obtain set of actives.
 *
 * The repeat execution is cancelled when cache is found to contain valid set of active name servers.
 * (or after waiting twice the query timeout period.
 *
 *
 */
public class RequestActivesTask extends TimerTask
{
  int count = 0;

  String name;

  HashSet<Integer> nameServersQueried;

  public RequestActivesTask(String name) {
    this.name = name;
    nameServersQueried = new HashSet<Integer>();
  }

  @Override
  public void run()
  {
    try {

      count ++;
      // check whether actives Received
      synchronized (PendingTasks.allTasks) {
        if (PendingTasks.allTasks.containsKey(name) == false) {
          PendingTasks.requestActivesOngoing.remove(name);
          throw  new CancelExecutorTaskException();
        }

        if (LocalNameServer.isValidNameserverInCache(name)) {
          PendingTasks.requestActivesOngoing.remove(name);
          PendingTasks.runPendingRequestsForName(name);
          throw  new CancelExecutorTaskException();
        }
      }

      // max number of attempts have been made,
      if (count > GNS.numPrimaryReplicas) {

        GNS.getLogger().warning("Error: No actives received for name: " + name + " after " + count + " attempts.");

        if (count > 2*StartLocalNameServer.maxQueryWaitTime/StartLocalNameServer.queryTimeout) {
          GNS.getLogger().severe("Error: No actives received for name. Requests failed. " + name + " after " + count +
                  " attempts.");
          try {
            PendingTasks.sendErrorMsgForName(name);
          } catch (JSONException e) {
            e.printStackTrace();
          }
          synchronized (PendingTasks.allTasks) {
            PendingTasks.requestActivesOngoing.remove(name);
          }
          throw  new CancelExecutorTaskException();
        }
      }
      // next primary to be queried
      int primaryID = LocalNameServer.getClosestPrimaryNameServer(name, nameServersQueried);
      if (primaryID == -1) {
        nameServersQueried.clear();
        primaryID = LocalNameServer.getClosestPrimaryNameServer(name, nameServersQueried);
        if (primaryID == -1) {
          GNS.getLogger().severe("No primary NS available. name = " + name);
          throw  new CancelExecutorTaskException();
        }

      }
      nameServersQueried.add(primaryID);
      // send packet to primary
      sendActivesRequestPacketToPrimary(name, primaryID);
    } catch (Exception e) {
      if (e.getClass().equals(CancelExecutorTaskException.class)) {
        throw new RuntimeException();
      }
      GNS.getLogger().severe("Unexpected exception in main active request loop: " + e);
      e.printStackTrace();
    }
  }



  /**
   * send request to primary to send actives
   * @param name
   * @param primaryID
   */
  private static void sendActivesRequestPacketToPrimary(String name, int primaryID) {

    RequestActivesPacket packet = new RequestActivesPacket(name, LocalNameServer.getNodeID());

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

  /**
   * Recvd reply from primary with current actives, update the cache.
   * @param json
   * @throws JSONException
   */
  public static void handleActivesRequestReply(JSONObject json) throws JSONException {
    RequestActivesPacket requestActivesPacket = new RequestActivesPacket(json);
    if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Recvd request actives packet: " + requestActivesPacket +
            " name\t" + requestActivesPacket.getName());
    if (requestActivesPacket.getActiveNameServers() == null ||
            requestActivesPacket.getActiveNameServers().size() == 0) {
      GNS.getLogger().fine("Null set of actives received for name " + requestActivesPacket.getName()  +
              " sending error");
      PendingTasks.sendErrorMsgForName(requestActivesPacket.getName());
      return;
    }

    if (LocalNameServer.containsCacheEntry(requestActivesPacket.getName())) {
      LocalNameServer.updateCacheEntry(requestActivesPacket);
      if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Updating cache Name:" +
              requestActivesPacket.getName() + " Actives: " + requestActivesPacket.getActiveNameServers());
    } else {
      LocalNameServer.addCacheEntry(requestActivesPacket);
      if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Adding to cache Name:" +
              requestActivesPacket.getName()+ " Actives: " + requestActivesPacket.getActiveNameServers());
    }

    PendingTasks.runPendingRequestsForName(requestActivesPacket.getName());

  }


}

