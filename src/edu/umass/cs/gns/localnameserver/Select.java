package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.client.Intercessor;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nameserver.NameRecord;
import edu.umass.cs.gns.packet.SelectRequestPacket;
import edu.umass.cs.gns.packet.SelectResponsePacket;
import edu.umass.cs.gns.packet.Transport;
import edu.umass.cs.gns.util.ConfigFileInfo;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Handles sending and receiving of queries.
 * 
 * @author westy
 */
public class Select {

  public static void handlePacketSelectRequest(JSONObject incomingJSON) throws JSONException, UnknownHostException {

    SelectRequestPacket packet = new SelectRequestPacket(incomingJSON);

    InetAddress address = null;
    int port = Transport.getReturnPort(incomingJSON);
    if (port > 0 && Transport.getReturnAddress(incomingJSON) != null) {
      address = InetAddress.getByName(Transport.getReturnAddress(incomingJSON));
    }
    Set<Integer> serverIds = ConfigFileInfo.getAllNameServerIDs();
    int queryId = LocalNameServer.addQueryInfo(packet.getKey(), packet, address, port, serverIds);
    packet.setLnsQueryId(queryId);
    JSONObject outgoingJSON = packet.toJSONObject();
    GNS.getLogger().fine("LNS" + LocalNameServer.nodeID + " transmitting QueryRequest " + outgoingJSON + " to " + serverIds);
    for (int nsid : serverIds) {
      try {
        if (!LNSListener.tcpTransport.sendToID(nsid, outgoingJSON)) {
          GNS.getLogger().severe("Failed to transmit QueryRequest to NS" + nsid);
        }
      } catch (IOException e) {
        GNS.getLogger().severe("Error during attempt to transmit QueryRequest to NS" + nsid + ":" + e);
      }
    }
  }

  public static void handlePacketSelectResponse(JSONObject json) throws JSONException {
    GNS.getLogger().fine("LNS" + LocalNameServer.nodeID + " recvd QueryResponse: " + json);
    SelectResponsePacket packet = new SelectResponsePacket(json);
    GNS.getLogger().fine("LNS" + LocalNameServer.nodeID + " recvd from NS" + packet.getNameServer());
    SelectInfo info = LocalNameServer.getQueryInfo(packet.getLnsQueryId());
    GNS.getLogger().fine("LNS" + LocalNameServer.nodeID + " located query info:" + info.serversYetToRespond());
    // upfdate our results list
    if (SelectResponsePacket.ResponseCode.NOERROR.equals(packet.getResponseCode())) {
      JSONArray jsonArray = packet.getJsonArray();
      int length = jsonArray.length();
      GNS.getLogger().fine("LNS" + LocalNameServer.nodeID + " processing " + length + " records");
      // org.json sucks... should have converted a long tine ago
      for (int i = 0; i < length; i++) {
        JSONObject record = jsonArray.getJSONObject(i);
        String name = record.getString(NameRecord.NAME.getName());
        if (info.addNewResponse(name, record)) {
          GNS.getLogger().fine("LNS" + LocalNameServer.nodeID + " added record for " + name);
        } else {
          GNS.getLogger().fine("LNS" + LocalNameServer.nodeID + " DID NOT ADD record for " + name);
        }
      }
    } else { // error response
      GNS.getLogger().fine("LNS" + LocalNameServer.nodeID + " processing error response: " + packet.getErrorMessage());
    }
    GNS.getLogger().fine("LNS" + LocalNameServer.nodeID + " removing server " + packet.getNameServer());
    // and remove the NS ID from the list to keep track of who has responded
    info.removeServerID(packet.getNameServer());
    GNS.getLogger().fine("LNS" + LocalNameServer.nodeID + " servers yet to respond:" + info.serversYetToRespond());
    // if we're done send a response back to the client
    if (info.allServersResponded()) {
      SelectResponsePacket response = SelectResponsePacket.makeSuccessPacket(packet.getId(), -1, -1,
              new JSONArray(info.getResponses()));
      if (info.getSenderAddress() != null && info.getSenderPort() > 0) {
        LNSListener.udpTransport.sendPacket(response.toJSONObject(), info.getSenderAddress(), info.getSenderPort());
      } else if (StartLocalNameServer.runHttpServer) {
        Intercessor.getInstance().checkForResult(response.toJSONObject());
      }
      LocalNameServer.removeQueryInfo(packet.getLnsQueryId());
    }
  }
}