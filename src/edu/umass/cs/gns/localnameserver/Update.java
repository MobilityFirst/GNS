package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.client.Intercessor;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.packet.ConfirmUpdateLNSPacket;
import edu.umass.cs.gns.packet.Transport;
import edu.umass.cs.gns.packet.UpdateAddressPacket;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class Update {

  static int updateCount = 0;
  static Object lock = new ReentrantLock();
  public static void handlePacketUpdateAddressLNS(JSONObject json)
          throws JSONException, UnknownHostException {
    synchronized (lock) {
      updateCount++;
//      GNS.getLogger().severe("\tUpdateCount\t" + updateCount);
    }

    UpdateAddressPacket updateAddressPacket = new UpdateAddressPacket(json);

    GNS.getLogger().fine(" UPDATE PACKET RECVD. Operation: " + updateAddressPacket.getOperation());

    if (updateAddressPacket.getOperation().isUpsert()) {
      AddRemove.handleUpsert(updateAddressPacket, InetAddress.getByName(Transport.getReturnAddress(json)), Transport.getReturnPort(json));
    } else {

      LocalNameServer.incrementUpdateRequest(updateAddressPacket.getName()); // important: used to count votes for names.

      InetAddress senderAddress = null;
      int senderPort = -1;
      senderPort = Transport.getReturnPort(json);
      if (Transport.getReturnAddress(json) != null) {
        senderAddress = InetAddress.getByName(Transport.getReturnAddress(json));
      }
      SendUpdatesTask updateTask = new SendUpdatesTask(updateAddressPacket, senderAddress, senderPort,
              System.currentTimeMillis(), new HashSet<Integer>(), 0);
      LocalNameServer.executorService.scheduleAtFixedRate(updateTask, 0, StartLocalNameServer.queryTimeout, TimeUnit.MILLISECONDS);
    }
  }


//  static int numUpdateResponse = 0;



  public static void handlePacketConfirmUpdateLNS(JSONObject json) throws UnknownHostException, JSONException {
    ConfirmUpdateLNSPacket confirmPkt = new ConfirmUpdateLNSPacket(json);
//    numUpdateResponse++;

    if (StartLocalNameServer.debugMode) {
      GNS.getLogger().fine("ConfirmUpdateLNS recvd: ResponseNum: " + " --> " + confirmPkt.toString());
    }

    if (confirmPkt.isSuccess()) {
      int lnsRequestID = confirmPkt.getLNSRequestID();

      // Experiment-related code in which all active replicas sent confirmation to the local name servers.
//      int nameServerID = confirmPkt.getRequestID();
//      double pingLatency = ConfigFileInfo.getPingLatency(nameServerID);
//      long curTime = System.currentTimeMillis();
//      GNS.getLogger().severe("\tLatencyMeasureRecv\t" + lnsRequestID + "\t" + nameServerID + "\t" + pingLatency + "\t" + curTime + "\t");

      UpdateInfo updateInfo = LocalNameServer.removeUpdateInfo(confirmPkt.getLNSRequestID());
      if (updateInfo == null) {
        if (StartLocalNameServer.debugMode) {
          GNS.getLogger().fine("Update confirm return info not found.");
        }
      } else {
        // update the cache BEFORE we send back the confirmation
        LocalNameServer.updateCacheEntry(confirmPkt, updateInfo.getName(), null);
        // send the confirmation back to the originator of the update
        if (StartLocalNameServer.debugMode) {
          GNS.getLogger().info("LNSListenerUpdate CONFIRM UPDATE (ns " + LocalNameServer.nodeID + ") to "
                  + updateInfo.senderAddress + ":" + updateInfo.senderPort + " : " + json.toString());
        }
        if (updateInfo.senderAddress != null && updateInfo.senderAddress.length() > 0 && updateInfo.senderPort > 0) {
          LNSListener.udpTransport.sendPacket(json,
                  InetAddress.getByName(updateInfo.senderAddress), updateInfo.senderPort);
        } else if (StartLocalNameServer.runHttpServer) {
          Intercessor.getInstance().checkForResult(json);
        }

        if (LocalNameServer.r.nextDouble() <= StartLocalNameServer.outputSampleRate) {
//          if (StartLocalNameServer.debugMode) {
//            GNS.getLogger().info(msg);
//          }
          GNS.getStatLogger().info(updateInfo.getUpdateStats(confirmPkt, updateInfo.getName()));
//          if (updateInfo.getLatency() > 30) {
//
////            GNS.getStatLogger().info(msg);
//          }
        }
      }
    } else {
      // if update failed, invalidate active name servers

      // SendUpdatesTask will create a task to get new actives
      UpdateInfo updateInfo = LocalNameServer.removeUpdateInfo(confirmPkt.getLNSRequestID());
      if (updateInfo == null) {
        return;
      }


      LocalNameServer.invalidateActiveNameServer(updateInfo.getName());

      UpdateAddressPacket updateAddressPacket = updateInfo.updateAddressPacket;

//      GNS.getStatLogger().fine(updateInfo.getUpdateFailedStats(new HashSet<Integer>(), LocalNameServer.nodeID,
//              updateAddressPacket.getRequestID()));

//      if (updateInfo.getNumRestarts() == StartLocalNameServer.MAX_RESTARTS) {
//        GNS.getLogger().severe("Max restarts. sending error ... name " + updateInfo.getName());
//        ConfirmUpdateLNSPacket confirmPkt1 = ConfirmUpdateLNSPacket.createFailPacket(updateAddressPacket);
//        try {
//          if (updateInfo.senderAddress != null && updateInfo.senderPort > 0) {
//            LNSListener.udpTransport.sendPacket(confirmPkt1.toJSONObject(),
//                    InetAddress.getByName(updateInfo.senderAddress), updateInfo.senderPort);
//          } else if (StartLocalNameServer.runHttpServer) {
//            Intercessor.getInstance().checkForResult(confirmPkt1.toJSONObject());
//          }
//        } catch (JSONException e) {
//          e.printStackTrace();
//        }
//
////        UpdateInfo updateInfo = LocalNameServer.removeUpdateInfo(updateRequestID);
//        if (updateInfo == null) {
//          GNS.getStatLogger().fine(UpdateInfo.getUpdateFailedStats(updateInfo.getName(),new HashSet<Integer>(),
//                  LocalNameServer.nodeID,updateAddressPacket.getRequestID(),updateInfo.getSendTime()));
////        if (StartLocalNameServer.debugMode) GNS.getLogger().fine("TIME EXCEEDED: UPDATE INFO IS NULL!!: " + updateAddressPacket);
//        } else {
//          GNS.getStatLogger().fine(updateInfo.getUpdateFailedStats(new HashSet<Integer>(), LocalNameServer.nodeID,
//                  updateAddressPacket.getRequestID()));
//        }
//        return;
//      }

      GNS.getLogger().info("\tInvalid Active Name Server.\tName\t" + updateInfo.getName() + "\tRequest new actives.\t");

      InetAddress address = null;

      if (updateInfo.getSenderAddress() != null) address = InetAddress.getByName(updateInfo.getSenderAddress());

      SendUpdatesTask task = new SendUpdatesTask(updateAddressPacket, address, updateInfo.senderPort,
              updateInfo.getSendTime(),new HashSet<Integer>(), updateInfo.getNumRestarts() + 1);

      String failedStats = UpdateInfo.getUpdateFailedStats(updateInfo.getName(),new HashSet<Integer>(),
              LocalNameServer.nodeID,updateAddressPacket.getRequestID(),updateInfo.getSendTime(),
              updateInfo.getNumRestarts() + 1);

      long delay = StartLocalNameServer.queryTimeout;
      if (updateInfo.getNumRestarts() == 0) delay = 0;
      PendingTasks.addToPendingRequests(updateInfo.getName(),task, StartLocalNameServer.queryTimeout,
              address,updateInfo.senderPort,
              ConfirmUpdateLNSPacket.createFailPacket(updateAddressPacket).toJSONObject(), failedStats, delay);



    }

  }
}