package edu.umass.cs.gnrs.localnameserver;

import edu.umass.cs.gnrs.main.GNS;
import edu.umass.cs.gnrs.nameserver.NameRecordKey;
import edu.umass.cs.gnrs.packet.*;
import edu.umass.cs.gnrs.util.ConfigFileInfo;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;

public class AddRemove {

  public static void handlePacketAddRecordLNS(JSONObject json) throws JSONException {
    NameRecordKey nameRecordKey;
    String name;
    int nameServerId;
    AddRecordPacket addRecordPacket = new AddRecordPacket(json);
    nameRecordKey = addRecordPacket.getRecordKey();
    name = addRecordPacket.getName();
    ArrayList<String> value = addRecordPacket.getValue();

    GNS.getLogger().fine("LNSListenerUpdate ADD (lns " + LocalNameServer.nodeID + ") : " + name + "/" + nameRecordKey.toString() + ", " + value);
    nameServerId = LocalNameServer.getClosestPrimaryNameServer(name, null);
    addRecordPacket.setPrimaryNameServers(LocalNameServer.getPrimaryNameServers(name));
    // some local bookkeeping so we know where to send the confirmation
    int addResquestId = LocalNameServer.addUpdateInfo(name, nameServerId, System.currentTimeMillis(),
            Transport.getReturnAddress(json), Transport.getReturnPort(json));
    addRecordPacket.setLNSRequestID(addResquestId);
    JSONObject jsonAddRecord = addRecordPacket.toJSONObject();
    LNSListener.udpTransport.sendPacket(jsonAddRecord, nameServerId, ConfigFileInfo.getUpdatePort(nameServerId));
      GNS.getLogger().fine("Sent add record packet to node: " + nameServerId + " Packet is: " + jsonAddRecord);
  }

  public static void handleUpsert(UpdateAddressPacket updateAddressPacket, InetAddress address, int port) throws JSONException {
    NameRecordKey nameRecordKey;
    String name;
    int nameServerId;

    nameRecordKey = updateAddressPacket.getRecordKey();
    name = updateAddressPacket.getName();
    ArrayList<String> value = updateAddressPacket.getUpdateValue();

    GNS.getLogger().fine("LNSListenerUpdate UPSERT (lns " + LocalNameServer.nodeID + ") : " + name + "/" + nameRecordKey.toString() + ", " + value);
    nameServerId = LocalNameServer.getClosestPrimaryNameServer(name, null);
    updateAddressPacket.setPrimaryNameServers((HashSet<Integer>) LocalNameServer.getPrimaryNameServers(name));
    // some local bookkeeping so we know where to send the confirmation
    int addResquestId = LocalNameServer.addUpdateInfo(name, nameServerId, System.currentTimeMillis(),
            address.getHostAddress(), port);
    updateAddressPacket.setLocalNameServerId(LocalNameServer.nodeID);
    updateAddressPacket.setLNSRequestID(addResquestId);
//        updateAddressPacket.
    JSONObject jsonAddRecord = updateAddressPacket.toJSONObject();
    LNSListener.udpTransport.sendPacket(jsonAddRecord, nameServerId, ConfigFileInfo.getUpdatePort(nameServerId));
    GNS.getLogger().fine(" UPSERT sent to primary: " + nameServerId);
  }

  public static void handlePacketRemoveRecordLNS(JSONObject json) throws JSONException, NoSuchAlgorithmException, UnsupportedEncodingException {
    NameRecordKey nameRecordKey;
    String name;
    int nameServerId;
    RemoveRecordPacket removeRecordPacket = new RemoveRecordPacket(json);
    // not used        
    //nameRecordKey = removeRecordPacket.getRecordKey();
    name = removeRecordPacket.getName();


    nameServerId = LocalNameServer.getClosestPrimaryNameServer(name, null);
    removeRecordPacket.setPrimaryNameServers((HashSet<Integer>) LocalNameServer.getPrimaryNameServers(name));
    GNS.getLogger().fine("LNSListenerUpdate REMOVE (lns " + LocalNameServer.nodeID + ") : "
            + name + " Sent to primary: " + nameServerId);
    // some local bookkeeping so we know where to send the confirmation
    int removeRequestId = LocalNameServer.addUpdateInfo(name, nameServerId, System.currentTimeMillis(),
            Transport.getReturnAddress(json), Transport.getReturnPort(json));
    removeRecordPacket.setLNSRequestID(removeRequestId);
    JSONObject jsonRemoveRecord = removeRecordPacket.toJSONObject();
    LNSListener.udpTransport.sendPacket(jsonRemoveRecord, nameServerId, ConfigFileInfo.getUpdatePort(nameServerId));
  }

  public static void handlePacketConfirmAddLNS(JSONObject json) throws JSONException, UnknownHostException {
    ConfirmUpdateLNSPacket confirmAddPacket = new ConfirmUpdateLNSPacket(json);
    UpdateInfo addInfo = LocalNameServer.removeUpdateInfo(confirmAddPacket.getLNSRequestID());
    if (addInfo == null) {
      GNS.getLogger().warning("Add confirmation return info not found.");
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