package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.packet.QueryRequestPacket;
import edu.umass.cs.gns.packet.Transport;
import edu.umass.cs.gns.util.ConfigFileInfo;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Set;
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
    LNSListener.tcpTransport.sendToIDs(serverIds, json);

  }

  public static void handlePacketQueryResponse(JSONObject json) throws JSONException {
  }
}