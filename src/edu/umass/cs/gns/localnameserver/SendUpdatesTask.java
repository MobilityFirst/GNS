package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.client.Intercessor;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.ReplicationFrameworkType;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.packet.ConfirmUpdateLNSPacket;
import edu.umass.cs.gns.packet.Packet;
import edu.umass.cs.gns.packet.UpdateAddressPacket;
import edu.umass.cs.gns.util.BestServerSelection;
import edu.umass.cs.gns.util.ConfigFileInfo;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.InetAddress;
import java.util.*;

public class SendUpdatesTask extends TimerTask
{

//	int MAX_TIMEOUTS = 3;

  String name;
  //NameRecordKey nameRecordKey;
  UpdateAddressPacket updateAddressPacket;
  InetAddress senderAddress;
  int senderPort;
  int updateRequestID;
  HashSet<Integer> activesQueried;
  int timeoutCount = -1;
  long requestRecvdTime;
  int numRestarts;

  public SendUpdatesTask(UpdateAddressPacket updateAddressPacket,
                         InetAddress senderAddress, int senderPort, long requestRecvdTime,
                         HashSet<Integer> activesQueried, int numRestarts)
  {
    this.name = updateAddressPacket.getName();
    //this.nameRecordKey = updateAddressPacket.getRecordKey();
    this.updateAddressPacket = updateAddressPacket;
    this.senderAddress = senderAddress;
    this.senderPort = senderPort;
    this.activesQueried = activesQueried;
    this.requestRecvdTime = requestRecvdTime;
    this.numRestarts = numRestarts;
  }

  @Override
  public void run()
  {
    try {

      timeoutCount++;

//      if (numRestarts > StartLocalNameServer.MAX_RESTARTS) { // just some defensive code.
//        handleFailure();
//        throw  new MyException();
//      }

//    long t0 = System.currentTimeMillis();
//    if (timeoutCount == 0 && t0 - requestRecvdTime > 10) {
//      GNS.getLogger().severe(" Long delay in Startup " + (t0 - requestRecvdTime));
//    }

      if (StartLocalNameServer.debugMode) GNS.getLogger().fine("ENTER name = " + name + " timeout = " + timeoutCount);

      if (timeoutCount > 0 && LocalNameServer.getUpdateInfo(updateRequestID) == null) {

        if (StartLocalNameServer.debugMode) GNS.getLogger().fine("UpdateInfo not found. Update complete or actives invalidated. Cancel task.");
        throw  new MyException();
      }

      if (System.currentTimeMillis() - requestRecvdTime > StartLocalNameServer.maxQueryWaitTime) {
        // send failed msg to user and log error
        if (StartLocalNameServer.debugMode) GNS.getLogger().fine("UPDATE FAILED no response until MAX-wait time: " + updateRequestID + " name = " + name);
        handleFailure();
        throw  new MyException();
      }

      int nameServerID;

      if (StartLocalNameServer.replicateAll) {
        nameServerID = BestServerSelection.getSmallestLatencyNS(ConfigFileInfo.getAllNameServerIDs(), activesQueried);
      } else {
        if (LocalNameServer.isValidNameserverInCache(name) == false) {
          // remove update info from LNS
          if (timeoutCount > 0) LocalNameServer.removeUpdateInfo(updateRequestID);

//          if (numRestarts == StartLocalNameServer.MAX_RESTARTS) {
//            if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" Max restarts reached. sending error " + numRestarts + " name " + name);
//            handleFailure();
//            throw  new MyException();
//          }
          // add to pending requests task
          try {
            PendingTasks.addToPendingRequests(name, //nameRecordKey,
                    new SendUpdatesTask(updateAddressPacket, senderAddress, senderPort, requestRecvdTime, new HashSet<Integer>(), numRestarts + 1),
                    StartLocalNameServer.queryTimeout, senderAddress, senderPort,
                    ConfirmUpdateLNSPacket.createFailPacket(updateAddressPacket).toJSONObject(),
                    UpdateInfo.getUpdateFailedStats(name,new HashSet<Integer>(),LocalNameServer.nodeID,updateAddressPacket.getRequestID(),requestRecvdTime, numRestarts + 1),0);

          } catch (JSONException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
          }
          // request new actives
          //      SendActivesRequestTask.requestActives(name);
          if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Created a request actives task. " + numRestarts);
          // cancel this task
          throw  new MyException();
        }

//        if (activesQueried.size() == 0) {
//          nameServerID = getDefaultCoordinatorReplica(name, LocalNameServer.getCacheEntry(name).getActiveNameServers());
//        }
//        else
        if(StartLocalNameServer.loadDependentRedirection) {
          nameServerID = LocalNameServer.getBestActiveNameServerFromCache(name, activesQueried);
        } else if (StartLocalNameServer.replicationFramework == ReplicationFrameworkType.BEEHIVE) {
            nameServerID = LocalNameServer.getBeehiveNameServerFromCache(name, activesQueried);
        }
        else {
          nameServerID = LocalNameServer.getClosestActiveNameServerFromCache(name, activesQueried);
        }

      }

      if (nameServerID == -1) {

        if (StartLocalNameServer.debugMode) GNS.getLogger().fine("ERROR: No more actives left to query. Actives Queried " + activesQueried);
        return;
//        activesQueried.clear();
//        nameServerID = LocalNameServer.getClosestActiveNameServerFromCache(name, activesQueried);
//        if (nameServerID == -1) return;
      }

      activesQueried.add(nameServerID);

      if (timeoutCount == 0) {
        String hostAddress = null;
        if (senderAddress != null) hostAddress = senderAddress.getHostAddress();
        updateRequestID = LocalNameServer.addUpdateInfo(name, nameServerID,
                requestRecvdTime, hostAddress, senderPort, numRestarts, updateAddressPacket);
        if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Update Info Added: Id = " + updateRequestID);
      } else {
//        UpdateInfo updateInfo = LocalNameServer.getUpdateInfo(updateRequestID);
//        if (updateInfo!=null) updateInfo.setSendTime(System.currentTimeMillis());
      }
      // create the packet that we'll send to the primary
      UpdateAddressPacket pkt = new UpdateAddressPacket(Packet.PacketType.UPDATE_ADDRESS_LNS,
              updateAddressPacket.getRequestID(), updateRequestID, -1,
              name, updateAddressPacket.getRecordKey(),
              updateAddressPacket.getUpdateValue(),
              updateAddressPacket.getOldValue(),
              updateAddressPacket.getOperation(),
              LocalNameServer.nodeID, nameServerID, updateAddressPacket.getTTL());
//      pkt.setPrimaryNameServers(LocalNameServer.getPrimaryNameServers(name));

      if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Sending Update to Node: " + nameServerID);
//      GNS.getLogger().severe("\tLatencyMeasureSend\t" + updateRequestID + "\t" + nameServerID + "\t" + ConfigFileInfo.getPingLatency(nameServerID) + "\t" + System.currentTimeMillis()+ "\t");
      // and send it off
      try {
        JSONObject jsonToSend = pkt.toJSONObject();
        LocalNameServer.sendToNS(jsonToSend,nameServerID);
        UpdateInfo updateInfo = LocalNameServer.getUpdateInfo(nameServerID);
        if (updateInfo != null) updateInfo.setNameserverID(nameServerID);
//        if (StartLocalNameServer.emulatePingLatencies) {
//          LNSListener.sendPacketWithDelay(jsonToSend,nameServerID);
//        } else {
          // for small packets use UDP


//        }
        // remote status
//        StatusClient.sendTrafficStatus(LocalNameServer.nodeID, nameServerID, GNS.PortType.LNS_TCP_PORT, pkt.getType(),
//                name, updateAddressPacket.getUpdateValue().toString());
        if (StartLocalNameServer.debugMode) GNS.getLogger().fine("LNSListenerUpdate: Send to: " + nameServerID + " Name:" + name + " Id:" + updateRequestID
                + " Time:" + System.currentTimeMillis()
                + " --> " + jsonToSend.toString());
      } catch (JSONException e) {
        e.printStackTrace();
      }
//      catch (IOException e) {
//        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//      }

    }catch (Exception e) {
      if (e.getClass().equals(MyException.class)) throw new RuntimeException();
      GNS.getLogger().severe("Exception Exception Exception ... ");
      e.printStackTrace();
    }
  }
//    if (activesQueried.size() == 3) return;
//
//    int nameServerID = activesQueried.size();
////    if (activesQueried.size() == 0) (nameServerID);
//    activesQueried.add(nameServerID);
//
//    if (timeoutCount == 0) {
//      String hostAddress = null;
//      if (senderAddress != null) hostAddress = senderAddress.getHostAddress();
//      updateRequestID = LocalNameServer.addUpdateInfo(name, nameServerID,
//              requestRecvdTime, hostAddress, senderPort);
//      if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Update Info Added: Id = " + updateRequestID);
//    }
//    // create the packet that we'll send to the primary
//    UpdateAddressPacket pkt = new UpdateAddressPacket(Packet.PacketType.UPDATE_ADDRESS_LNS,
//            updateAddressPacket.getRequestID(), updateRequestID, -1,
//            name, updateAddressPacket.getRecordKey(),
//            updateAddressPacket.getUpdateValue(),
//            updateAddressPacket.getOldValue(),
//            updateAddressPacket.getOperation(),
//            LocalNameServer.nodeID, nameServerID);
////      pkt.setPrimaryNameServers(LocalNameServer.getPrimaryNameServers(name));
//
//    if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Sending Update to Node: " + nameServerID);
//
//    // and send it off
//    try {
//      JSONObject jsonToSend = pkt.toJSONObject();
//      LNSListener.tcpTransport.sendToID(nameServerID, jsonToSend);
//      // remote status
////      StatusClient.sendTrafficStatus(LocalNameServer.nodeID, nameServerID, GNS.PortType.LNS_TCP_PORT, pkt.getType(),
////              name,updateAddressPacket.getUpdateValue().toString());
//      if (StartLocalNameServer.debugMode) GNS.getLogger().fine("LNSListenerUpdate: Send to: " + nameServerID + " Name:" + name + " Id:" + updateRequestID
//              + " Time:" + System.currentTimeMillis()
//              + " --> " + jsonToSend.toString());
//    } catch (JSONException e) {
//      e.printStackTrace();
//    } catch (IOException e) {
//      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//    }

//    long t1 = System.currentTimeMillis();
//    if (t1 - t0 > 10) {
//      GNS.getLogger().severe(" long delay in SendUpdatesTask " + (t1 - t0));
//    }


  private int getDefaultCoordinatorReplica(String name, Set<Integer> activeReplicas) {


//    int nodeProduct = 1;
//    for (int x: nodeIDs) {
//      nodeProduct =  nodeProduct*x;
//    }

    Random r = new Random(name.hashCode());
    ArrayList<Integer> x1  = new ArrayList<Integer>(activeReplicas);
    Collections.sort(x1);
    Collections.shuffle(x1, r);
    return x1.get(0);
//    for (int x: x1) {
//      if (PaxosManager.isNodeUp(x)) return x;
//    }
//    return  x1.get(0);
//    return  x1.get(count);
  }
  private void handleFailure() {
    ConfirmUpdateLNSPacket confirmPkt = ConfirmUpdateLNSPacket.createFailPacket(updateAddressPacket);
    try {
      if (senderAddress != null && senderPort > 0) {
        LNSListener.udpTransport.sendPacket(confirmPkt.toJSONObject(), senderAddress, senderPort);
      } else if (StartLocalNameServer.runHttpServer) {
        Intercessor.getInstance().checkForResult(confirmPkt.toJSONObject());
      }
    } catch (JSONException e) {
      e.printStackTrace();
    }

    UpdateInfo updateInfo = LocalNameServer.removeUpdateInfo(updateRequestID);
    if (updateInfo == null) {
      if (timeoutCount == 0) GNS.getStatLogger().fine(UpdateInfo.getUpdateFailedStats(name,activesQueried,
              LocalNameServer.nodeID,updateAddressPacket.getRequestID(), requestRecvdTime, numRestarts));
//        if (StartLocalNameServer.debugMode) GNS.getLogger().fine("TIME EXCEEDED: UPDATE INFO IS NULL!!: " + updateAddressPacket);
    } else {
      GNS.getStatLogger().fine(updateInfo.getUpdateFailedStats(activesQueried, LocalNameServer.nodeID, updateAddressPacket.getRequestID()));

    }
  }


}
