package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.packet.RequestActivesPacket;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.HashSet;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

public class SendActivesRequestTask extends TimerTask
{

  String name;
  //NameRecordKey recordKey;
  HashSet<Integer> nameServersQueried;
  int MAX_ATTEMPTS = GNS.numPrimaryReplicas;

  public SendActivesRequestTask(String name//, NameRecordKey recordKey
  ) {
    this.name = name;
    //this.recordKey = recordKey;
    nameServersQueried = new HashSet<Integer>();
  }


  @Override
  public void run()
  {
    // check whether actives Received
    if (LocalNameServer.isValidNameserverInCache(name//, recordKey
    )) {
      throw  new RuntimeException();
    }
    // All primaries have been queried
    if (nameServersQueried.size() == GNS.numPrimaryReplicas) {
      //
      throw  new RuntimeException();
    }
    // next primary to be queried
    int primaryID = LocalNameServer.getClosestPrimaryNameServer(name, //recordKey,
            nameServersQueried);
    if (primaryID == -1) {
      throw  new RuntimeException();
    }
    nameServersQueried.add(primaryID);
    // send packet to primary
    sendActivesRequestPacketToPrimary(name, //recordKey,
            primaryID);
  }


  /**
   * Create task to request actives from primaries.
   * @param name
   */
  public static void requestActives(String name) {
    SendActivesRequestTask task = new SendActivesRequestTask(name);
    LocalNameServer.executorService.scheduleAtFixedRate(task, 0, GNS.DEFAULT_QUERY_TIMEOUT, TimeUnit.MILLISECONDS);
  }

  /**
   * send request to primary to send actives
   * @param name
   * @param primaryID
   */
  private static void sendActivesRequestPacketToPrimary(String name, //NameRecordKey recordKey,
                                                        int primaryID) {
    RequestActivesPacket packet = new RequestActivesPacket(name, //recordKey,
            LocalNameServer.nodeID);
    try
    {
      LNSListener.tcpTransport.sendToID(primaryID,packet.toJSONObject());
//			LNSListener.udpTransport.sendPacket(packet.toJSONObject(), primaryID, GNS.PortType.UPDATE_PORT);
      if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Send Active Request Packet to Primary. " + primaryID);
    } catch (JSONException e)
    {
      if (StartLocalNameServer.debugMode) GNS.getLogger().fine("JSON Exception in sending packet");
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

  }

  /**
   * Recvd reply from primary with current actives, update the cache.
   * @param json
   * @throws JSONException
   */
  public static void handleActivesRequestReply(JSONObject json) throws JSONException {
    RequestActivesPacket requestActivesPacket = new RequestActivesPacket(json);
    if (StartLocalNameServer.debugMode) GNS.getLogger().fine("RECVD request packet: " + requestActivesPacket);
    if (requestActivesPacket.getActiveNameServers() == null ||
            requestActivesPacket.getActiveNameServers().size() == 0) {
      PendingTasks.sendErrorMsgForName(requestActivesPacket.getName()//,requestActivesPacket.getRecordKey()
      );
      return;
    }

    if (LocalNameServer.containsCacheEntry(requestActivesPacket.getName())) {
      LocalNameServer.updateCacheEntry(requestActivesPacket);
      if (StartLocalNameServer.debugMode) GNS.getLogger().fine("LNSListenerResponse: Updating cache Name:" +
              requestActivesPacket.getName() + " Actives: " + requestActivesPacket.getActiveNameServers());
    } else {
      LocalNameServer.addCacheEntry(requestActivesPacket);
      if (StartLocalNameServer.debugMode) GNS.getLogger().fine("LNSListenerResponse: Adding to cache Name:" +
              requestActivesPacket.getName()+ " Actives: " + requestActivesPacket.getActiveNameServers());
    }

    PendingTasks.runPendingRequestsForName(
            requestActivesPacket.getName()//, requestActivesPacket.getRecordKey()
    );

  }


}

