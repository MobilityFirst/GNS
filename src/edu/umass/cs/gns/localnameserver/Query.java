package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.client.Intercessor;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nameserver.NameRecord;
import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.packet.QueryRequestPacket;
import edu.umass.cs.gns.packet.QueryResponsePacket;
import edu.umass.cs.gns.packet.Transport;
import edu.umass.cs.gns.util.ConfigFileInfo;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class Query {

  public static void handlePacketQueryRequest(JSONObject json) throws JSONException, UnknownHostException {

    QueryRequestPacket packet = new QueryRequestPacket(json);

    InetAddress address = null;
    int port = Transport.getReturnPort(json);
    if (port > 0 && Transport.getReturnAddress(json) != null) {
      address = InetAddress.getByName(Transport.getReturnAddress(json));
    }
    Set<Integer> serverIds = ConfigFileInfo.getAllNameServerIDs();
    int queryId = LocalNameServer.addQueryInfo(packet.getKey(), packet, address, port, serverIds);
    packet.setLnsQueryId(queryId);
    GNS.getLogger().info("LNS" + LocalNameServer.nodeID + " transmitting QueryRequest " + json + " to " + serverIds);
    for (int nsid : serverIds) {
      try {
        if (!LNSListener.tcpTransport.sendToID(nsid, json)) {
          GNS.getLogger().severe("Failed to transmit QueryRequest to NS" + nsid);
        }
      } catch (IOException e) {
        GNS.getLogger().severe("Error during attempt to transmit  QueryRequest to NS" + nsid + ":" + e);
      }
    }
  }

  public static void handlePacketQueryResponse(JSONObject json) throws JSONException {
    GNS.getLogger().info("LNS" + LocalNameServer.nodeID + " recvd QueryResponse: " + json);
    QueryResponsePacket packet = new QueryResponsePacket(json);
    QueryInfo info = LocalNameServer.getQueryInfo(packet.getLnsQueryId());
    // upfdate our results list
    JSONArray jsonArray = packet.getJsonArray();
    int length = jsonArray.length();
    // org.json sucks... should have converted a long tine ago
    for (int i = 0; i < length; i++) {
      JSONObject record = jsonArray.getJSONObject(i);
      String name = record.getString(NameRecord.NAME.getName());
      GNS.getLogger().info("LNS" + LocalNameServer.nodeID + " got record for " + name);
      info.addNewResponse(name, record);
    }
    // and remove the NS ID from the list to keep track of who has responded
    info.removeServerID(packet.getNameServer());
    GNS.getLogger().info("LNS" + LocalNameServer.nodeID + " servers yet to respond:" + info.serversYetToRespond());
    // if we're done send a response back to the client
    if (info.allServersResponded()) {
      QueryResponsePacket response = new QueryResponsePacket(packet.getId(), -1, -1, new JSONArray(info.getResponses()));
      if (info.getSenderAddress() != null && info.getSenderPort() > 0) {
        LNSListener.udpTransport.sendPacket(response.toJSONObject(), info.getSenderAddress(), info.getSenderPort());
      } else if (StartLocalNameServer.runHttpServer) {
        Intercessor.getInstance().checkForResult(response.toJSONObject());
      }
      LocalNameServer.removeQueryInfo(packet.getLnsQueryId());
    }
  }
}