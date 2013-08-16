package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.packet.*;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;

public class AddRemove {

  /**
   *
   * @param json
   * @throws JSONException
   * @throws UnknownHostException
   */
  public static void handlePacketAddRecordLNS(JSONObject json) throws JSONException, UnknownHostException {

    AddRecordPacket addRecordPacket = new AddRecordPacket(json);
    InetAddress senderAddress = null;
    int senderPort = -1;
    senderPort = Transport.getReturnPort(json);
    if (Transport.getReturnAddress(json) != null) {
      senderAddress = InetAddress.getByName(Transport.getReturnAddress(json));
    }

    addRecordPacket.setPrimaryNameServers(LocalNameServer.getPrimaryNameServers(addRecordPacket.getName()));

    SendAddRemoveUpsertTask addTask = new SendAddRemoveUpsertTask(addRecordPacket, addRecordPacket.getName(),
            senderAddress, senderPort, System.currentTimeMillis(), new HashSet<Integer>());
    LocalNameServer.timer.schedule(addTask, 0, StartLocalNameServer.queryTimeout);
                        addRecordPacket.getLocalNameServerID();
    if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" Add  Task Scheduled. " +
            "Name: " + addRecordPacket.getName() + " Request: " + addRecordPacket.getRequestID());
  }

  /**
   *
   * @param updateAddressPacket
   * @param address
   * @param port
   * @throws JSONException
   */
  static void handleUpsert(UpdateAddressPacket updateAddressPacket, InetAddress address, int port) throws JSONException {
    updateAddressPacket.setPrimaryNameServers(LocalNameServer.getPrimaryNameServers(updateAddressPacket.getName()));

//    updateAddressPacket.setLocalNameServerId(LocalNameServer.nodeID);

    SendAddRemoveUpsertTask upsertTask = new SendAddRemoveUpsertTask(updateAddressPacket, updateAddressPacket.getName(),
            address, port, System.currentTimeMillis(), new HashSet<Integer>());
    LocalNameServer.timer.schedule(upsertTask, 0, StartLocalNameServer.queryTimeout);

    if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" Upsert Task Scheduled. " +
            "Name: " + updateAddressPacket.getName() + " Request: " + updateAddressPacket.getRequestID());

  }

  /**
   *
   * @param json
   * @throws JSONException
   * @throws NoSuchAlgorithmException
   * @throws UnsupportedEncodingException
   */
  public static void handlePacketRemoveRecordLNS(JSONObject json)
          throws JSONException, NoSuchAlgorithmException, UnsupportedEncodingException, UnknownHostException {

    RemoveRecordPacket removeRecord = new RemoveRecordPacket(json);
    InetAddress senderAddress = null;
    int senderPort = -1;
    senderPort = Transport.getReturnPort(json);
    if (Transport.getReturnAddress(json) != null) {
      senderAddress = InetAddress.getByName(Transport.getReturnAddress(json));
    }

    removeRecord.setPrimaryNameServers(LocalNameServer.getPrimaryNameServers(removeRecord.getName()));

    SendAddRemoveUpsertTask task = new SendAddRemoveUpsertTask(removeRecord, removeRecord.getName(),
            senderAddress, senderPort, System.currentTimeMillis(), new HashSet<Integer>());
    LocalNameServer.timer.schedule(task, 0, StartLocalNameServer.queryTimeout);

    if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" Remove  Task Scheduled. " +
            "Name: " + removeRecord.getName() + " Request: " + removeRecord.getRequestID());
  }

  /**
   *
   * @param json
   * @throws JSONException
   * @throws UnknownHostException
   */
  public static void handlePacketConfirmAddLNS(JSONObject json) throws JSONException, UnknownHostException {
    ConfirmUpdateLNSPacket confirmAddPacket = new ConfirmUpdateLNSPacket(json);
    UpdateInfo addInfo = LocalNameServer.removeUpdateInfo(confirmAddPacket.getLNSRequestID());
    if (addInfo == null) {
      GNS.getLogger().warning("Add confirmation return info not found.: lns request id = " + confirmAddPacket.getLNSRequestID());
    } else {
      // update our cache BEFORE we confirm
      LocalNameServer.updateCacheEntry(confirmAddPacket);
      // send it back to the orginator of the request
      JSONObject jsonConfirm = confirmAddPacket.toJSONObject();
      GNS.getLogger().fine("LNSListenerUpdate CONFIRM ADD (lns " + LocalNameServer.nodeID + ") to "
              + addInfo.senderAddress + ":" + addInfo.senderPort + " : " + jsonConfirm.toString());
      if (addInfo.senderAddress != null && addInfo.senderPort > 0) {
        LNSListener.udpTransport.sendPacket(json, InetAddress.getByName(addInfo.senderAddress), addInfo.senderPort);
      }

    }
  }

  /**
   *
   * @param json
   * @throws JSONException
   * @throws UnknownHostException
   */
  public static void handlePacketConfirmRemoveLNS(JSONObject json) throws JSONException, UnknownHostException {
    ConfirmUpdateLNSPacket confirmRemovePacket = new ConfirmUpdateLNSPacket(json);
    UpdateInfo removeInfo = LocalNameServer.removeUpdateInfo(confirmRemovePacket.getLNSRequestID());
    if (removeInfo == null) {
      GNS.getLogger().warning("Remove confirmation return info not found.");
    } else {
      // send it back to the orginator of the request
      JSONObject jsonConfirm = confirmRemovePacket.toJSONObject();
      // update our cache BEFORE we confirm
      LocalNameServer.updateCacheEntry(confirmRemovePacket);
      GNS.getLogger().fine("LNSListenerUpdate CONFIRM REMOVE (lns " + LocalNameServer.nodeID + ") to "
              + removeInfo.senderAddress + ":" + removeInfo.senderPort + " : " + jsonConfirm.toString());
      if (removeInfo.senderAddress != null && removeInfo.senderPort > 0) {
        LNSListener.udpTransport.sendPacket(json, InetAddress.getByName(removeInfo.senderAddress), removeInfo.senderPort);
      }
      // update our cache

    }
  }
}