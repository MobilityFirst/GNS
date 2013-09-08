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

public class Update {

  public static void handlePacketUpdateAddressLNS(JSONObject json)
          throws JSONException, UnknownHostException {


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
      SendUpdatesTask updateTask = new SendUpdatesTask(updateAddressPacket,
              senderAddress, senderPort, System.currentTimeMillis(), new HashSet<Integer>());
      LocalNameServer.executorService.scheduleAtFixedRate(updateTask, 0, StartLocalNameServer.queryTimeout, TimeUnit.MILLISECONDS);
    }
  }
//  static int numUpdateResponse = 0;

  public static void handlePacketConfirmUpdateLNS(JSONObject json) throws UnknownHostException, JSONException {
    ConfirmUpdateLNSPacket confirmPkt = new ConfirmUpdateLNSPacket(json);
//    numUpdateResponse++;

    if (StartLocalNameServer.debugMode) {
      GNS.getLogger().fine("ConfirmUpdateLNS recvd: ResponseNum: "
              + " --> " + confirmPkt.toString());
    }

    if (confirmPkt.isSuccess()) {

      UpdateInfo updateInfo = LocalNameServer.removeUpdateInfo(confirmPkt.getLNSRequestID());
      if (updateInfo == null) {
        if (StartLocalNameServer.debugMode) {
          GNS.getLogger().fine("Update confirm return info not found.");
        }
      } else {
        // send the confirmation back to the originator of the update
        if (StartLocalNameServer.debugMode) {
          GNS.getLogger().severe("LNSListenerUpdate CONFIRM UPDATE (ns " + LocalNameServer.nodeID + ") to "
                  + updateInfo.senderAddress + ":" + updateInfo.senderPort + " : " + json.toString());
        }
        if (updateInfo.senderAddress != null && updateInfo.senderAddress.length() > 0 && updateInfo.senderPort > 0) {
          LNSListener.udpTransport.sendPacket(json,
                  InetAddress.getByName(updateInfo.senderAddress), updateInfo.senderPort);
        } else if (StartLocalNameServer.runHttpServer) {
          Intercessor.getInstance().checkForResult(json);
        }
//        updateInfo.getName();
//          if (StartLocalNameServer.debugMode) GNS.getLogger().fine("this is the key: " + confirmPkt.getRecordKey().toString());
        LocalNameServer.updateCacheEntry(confirmPkt, updateInfo.getName(), null);

        // record some stats
//        LocalNameServer.incrementUpdateResponse(confirmPkt.getName());

        if (LocalNameServer.r.nextDouble() < StartLocalNameServer.outputSampleRate) {
          String msg = updateInfo.getUpdateStats(confirmPkt, updateInfo.getName());
          if (StartLocalNameServer.debugMode) {
            GNS.getLogger().info(msg);
          }
          GNS.getStatLogger().info(msg);
//          if (updateInfo.getLatency() > 30) {
//
////            GNS.getStatLogger().info(msg);
//          }
        }
      }
    } else {
      // if update failed, invalidate active name servers
      // SendUpdatesTask will create a task to get new actives
      // TODO: create SendActivesRequestTask here and delete update info.
      UpdateInfo updateInfo = LocalNameServer.getUpdateInfo(confirmPkt.getRequestID());
      if (updateInfo == null) return;
      LocalNameServer.invalidateActiveNameServer(updateInfo.getName());
      GNS.getLogger().fine(" Update Request Sent To An Invalid Active Name Server. ERROR!! Actives Invalidated");

    }

  }
}