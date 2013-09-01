package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.client.Intercessor;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.packet.*;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.TimerTask;

/**
 * Sends three types of messages (with retries): AddRecordPacket, RemoveRecordPacket, and
 * UpdateAddressPacket with upsert.  These messages are sent one by one to all primaries in order of their distance.
 * If no response is received until {@code maxQueryWaitTime}, an error response is sent to client.
 * User: abhigyan
 * Date: 8/9/13
 * Time: 4:59 PM
 * To change this template use File | Settings | File Templates.
 */
public class SendAddRemoveUpsertTask extends TimerTask{

  String name;
  BasicPacket packet;
  InetAddress senderAddress;
  int senderPort;
  int updateRequestID;
  HashSet<Integer> primariesQueried;
  int timeoutCount = -1;
  long requestRecvdTime;

  public SendAddRemoveUpsertTask(BasicPacket packet, String name,
                                 InetAddress senderAddress, int senderPort, long requestRecvdTime,
                                 HashSet<Integer> primariesQueried)
  {
    this.name = name;
    this.packet = packet;
    this.senderAddress = senderAddress;
    this.senderPort = senderPort;
    this.primariesQueried = primariesQueried;
    this.requestRecvdTime = requestRecvdTime;
  }

  @Override
  public void run()
  {
    timeoutCount++;
    if (StartLocalNameServer.debugMode) GNS.getLogger().fine("ENTER name = " + name + " timeout = " + timeoutCount);

    if (timeoutCount > 0 && LocalNameServer.getUpdateInfo(updateRequestID) == null) {
      if (StartLocalNameServer.debugMode) GNS.getLogger().fine("UpdateInfo not found. Either update complete or invalid actives. Cancel task.");
      throw  new RuntimeException();
//      return;
    }

    if (timeoutCount > 0 && System.currentTimeMillis() - requestRecvdTime > StartLocalNameServer.maxQueryWaitTime) {
      UpdateInfo updateInfo = LocalNameServer.removeUpdateInfo(updateRequestID);

      if (updateInfo == null) {
        if (StartLocalNameServer.debugMode)
          GNS.getLogger().fine("TIME EXCEEDED: UPDATE INFO IS NULL!!: " + packet);
        throw  new RuntimeException();
      }
      if (StartLocalNameServer.debugMode) GNS.getLogger().fine("ADD FAILED no response until MAX-wait time: " + updateRequestID + " name = " + name);
      ConfirmUpdateLNSPacket confirmPkt = getConfirmPacket(packet);
      try {
        if (confirmPkt!= null) {
          if (updateInfo.senderAddress != null && updateInfo.senderAddress.length() > 0 && updateInfo.senderPort > 0) {
            LNSListener.udpTransport.sendPacket(confirmPkt.toJSONObject(),
                    InetAddress.getByName(updateInfo.senderAddress), updateInfo.senderPort);
          } else if (StartLocalNameServer.runHttpServer) {
            Intercessor.getInstance().checkForResult(confirmPkt.toJSONObject());
          }
        } else {
          if (StartLocalNameServer.debugMode) GNS.getLogger().fine("ERROR: Confirm update is NULL. Cannot sent response to client.");
        }
      } catch (JSONException e) {
        e.printStackTrace();
      } catch (UnknownHostException e) {
        e.printStackTrace();
      }
      String updateStats = updateInfo.getUpdateFailedStats(primariesQueried, LocalNameServer.nodeID, updateRequestID);
      if (StartLocalNameServer.debugMode) GNS.getLogger().fine(updateStats);
      GNS.getStatLogger().fine(updateStats);

      throw  new RuntimeException();
    }
    if (primariesQueried.size() == GNS.numPrimaryReplicas) primariesQueried.clear();
    int nameServerID = LocalNameServer.getClosestPrimaryNameServer(name, primariesQueried);

    if (nameServerID == -1) {
      if (StartLocalNameServer.debugMode) GNS.getLogger().fine("ERROR: No more primaries left to query. RETURN. Primaries queried " + primariesQueried);
      return;
    }
    else {
      primariesQueried.add(nameServerID);
    }
    if (timeoutCount == 0) {
      String hostAddress = null;
      if (senderAddress != null) hostAddress = senderAddress.getHostAddress();
      updateRequestID = LocalNameServer.addUpdateInfo(name, nameServerID,
              requestRecvdTime, hostAddress, senderPort);
      if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Update Info Added: Id = " + updateRequestID);
      updatePacketWithRequestID(packet, updateRequestID);
    }
    // create the packet that we'll send to the primary

    if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Sending Update to Node: " + nameServerID);

    // and send it off
    try {
      JSONObject jsonToSend = packet.toJSONObject();
      LNSListener.tcpTransport.sendToID(nameServerID, jsonToSend);
      // remote status
//        StatusClient.sendTrafficStatus(LocalNameServer.nodeID, nameServerID, GNS.PortType.UPDATE_PORT, pkt.getType(), name,
//                //nameRecordKey.getName(),
//                updateAddressPacket.getUpdateValue().toString());
      if (StartLocalNameServer.debugMode) GNS.getLogger().fine("SendAddRequest: Send to: " + nameServerID +
              " Name:" + name + " Id:" + updateRequestID + " Time:" + System.currentTimeMillis() + " --> " + jsonToSend.toString());
    } catch (JSONException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

  }


  private ConfirmUpdateLNSPacket getConfirmPacket(BasicPacket packet) {
    ConfirmUpdateLNSPacket confirm;
    switch (packet.getType()) {
      case ADD_RECORD_LNS:
        confirm = new ConfirmUpdateLNSPacket(0, false, (AddRecordPacket) packet);
        return  confirm;
      case REMOVE_RECORD_LNS:
        confirm = new ConfirmUpdateLNSPacket(0, false, (RemoveRecordPacket)packet);
        return  confirm;
      case UPDATE_ADDRESS_LNS:
        confirm = ConfirmUpdateLNSPacket.createFailPacket((UpdateAddressPacket)packet, 0);
        return confirm;
    }
    return null;
  }


  private void updatePacketWithRequestID(BasicPacket packet, int requestID) {

    switch (packet.getType()) {
      case ADD_RECORD_LNS:
        AddRecordPacket addRecordPacket = (AddRecordPacket) packet;
        addRecordPacket.setLNSRequestID(requestID);
        break;
      case REMOVE_RECORD_LNS:
        RemoveRecordPacket removeRecordPacket = (RemoveRecordPacket) packet;
        removeRecordPacket.setLNSRequestID(requestID);
        break;
      case UPDATE_ADDRESS_LNS:
        UpdateAddressPacket updateAddressPacket = (UpdateAddressPacket) packet;
        updateAddressPacket.setLNSRequestID(requestID);
        break;
    }

  }



}
