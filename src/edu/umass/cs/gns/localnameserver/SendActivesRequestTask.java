package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.packet.RequestActivesPacket;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.TimerTask;

public class SendActivesRequestTask extends TimerTask
{
  int count = 0;
  String name;
  //NameRecordKey recordKey;
  HashSet<Integer> nameServersQueried;

  public SendActivesRequestTask(String name//, NameRecordKey recordKey
  ) {
    this.name = name;
    //this.recordKey = recordKey;
    nameServersQueried = new HashSet<Integer>();
  }


  @Override
  public void run()
  {
    try {

      count ++;
      // check whether actives Received
      if (LocalNameServer.isValidNameserverInCache(name)) {
        throw  new MyException();
      }
      // max number of attempts have been made,
      if (count > GNS.numPrimaryReplicas) {
        try {
          GNS.getLogger().severe("No actives received for name: " + name + " sending error.");
          PendingTasks.sendErrorMsgForName(name);
        } catch (JSONException e) {
          e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        throw  new MyException();
      }
      // next primary to be queried
      int primaryID = LocalNameServer.getClosestPrimaryNameServer(name, //recordKey,
              nameServersQueried);
      if (primaryID == -1) {
        return;
      }
      nameServersQueried.add(primaryID);
      // send packet to primary
      sendActivesRequestPacketToPrimary(name, primaryID);
    } catch (Exception e) {
      if (e.getClass().equals(MyException.class)) {
        throw new RuntimeException();
      }
      GNS.getLogger().severe("Exception Exception Exception ... ");
      e.printStackTrace();
    }
  }


//  /**
//   * Create task to request actives from primaries.
//   * @param name
//   */
//  public static void requestActives(String name) {
//
//    SendActivesRequestTask task = new SendActivesRequestTask(name);
//    LocalNameServer.executorService.scheduleAtFixedRate(task, 0, StartLocalNameServer.queryTimeout, TimeUnit.MILLISECONDS);
//  }

  /**
   * send request to primary to send actives
   * @param name
   * @param primaryID
   */
  private static void sendActivesRequestPacketToPrimary(String name, int primaryID) {

    RequestActivesPacket packet = new RequestActivesPacket(name, LocalNameServer.nodeID);

    try
    {
      JSONObject sendJson = packet.toJSONObject();
//      if (sendJson) {
      LocalNameServer.sendToNS(sendJson,primaryID);

//      }
//      LNSListener.tcpTransport.sendToID(primaryID,sendJson);
//			LNSListener.udpTransport.sendPacket(packet.toJSONObject(), primaryID, GNS.PortType.UPDATE_PORT);
      if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Send Active Request Packet to Primary. " + primaryID
              + "\tname\t" + name);
    } catch (JSONException e)
    {
      if (StartLocalNameServer.debugMode) GNS.getLogger().fine("JSON Exception in sending packet. name\t" + name);
      e.printStackTrace();
    }
//    catch (IOException e) {
//      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//    }

  }

  /**
   * Recvd reply from primary with current actives, update the cache.
   * @param json
   * @throws JSONException
   */
  public static void handleActivesRequestReply(JSONObject json) throws JSONException {
    RequestActivesPacket requestActivesPacket = new RequestActivesPacket(json);
    if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Recvd request actives packet: " + requestActivesPacket + " name\t" + requestActivesPacket.getName());
    if (requestActivesPacket.getActiveNameServers() == null ||
            requestActivesPacket.getActiveNameServers().size() == 0) {
      GNS.getLogger().severe("Null set of actives received for name " + requestActivesPacket.getName()  + " sending error");
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

