package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nameserver.NSNodeConfig;
import edu.umass.cs.gns.nio.ByteStreamToJSONObjects;
import edu.umass.cs.gns.nio.NioServer2;
import edu.umass.cs.gns.nio.PacketDemultiplexer;
import edu.umass.cs.gns.packet.DNSPacket;
import edu.umass.cs.gns.packet.Packet;
import edu.umass.cs.gns.packet.Transport;
import edu.umass.cs.gns.util.ConfigFileInfo;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.TimerTask;

/**
 * Listens on a UDP port for requests from end-users, and responses from name servers.
 *
 * @author abhigyan
 *
 */
public class LNSListener extends Thread {

  public static Transport udpTransport;
  public static NioServer2 tcpTransport;

  public LNSListener() throws IOException {
    super("LNSListener");
    udpTransport = new Transport(LocalNameServer.nodeID, ConfigFileInfo.getLNSUdpPort(LocalNameServer.nodeID));
//    tcpTransport = new NioServer2(LocalNameServer.nodeID,new ByteStreamToJSONObjects(this));

    tcpTransport = new NioServer2(LocalNameServer.nodeID, new ByteStreamToJSONObjects(new LNSPacketDemultiplexer()), new NSNodeConfig());
    new Thread(tcpTransport).start();


  }

  @Override
  public void run() {

    while (true) {

      JSONObject json = udpTransport.readPacket();
      demultiplexLNSPackets(json);
    }

  }

  /**
   * This class de-multiplexes all packets received at this local name server.
   *
   * @param json
   */
  public static void demultiplexLNSPackets(JSONObject json) {

    try {

      if (StartLocalNameServer.debugMode) {
        GNS.getLogger().finer("LOCAL NAME SERVER RECVD PACKET: " + json);
      }


      switch (Packet.getPacketType(json)) {
        case DNS:
          DNSPacket dnsPacket = new DNSPacket(json);
          Packet.PacketType incomingPacketType = Packet.getDNSPacketType(dnsPacket);
          switch (incomingPacketType) {
            // Lookup
            case DNS:
              Lookup.handlePacketLookupRequest(json, dnsPacket);
              break;
            case DNS_RESPONSE:
              Lookup.handlePacketLookupResponse(json, dnsPacket);
              break;
            case DNS_ERROR_RESPONSE:
              Lookup.handlePacketLookupErrorResponse(json, dnsPacket);
              break;
          }
          break;
        // Add/remove 
        case ADD_RECORD_LNS:
          AddRemove.handlePacketAddRecordLNS(json);
          break;
        case REMOVE_RECORD_LNS:
          AddRemove.handlePacketRemoveRecordLNS(json);
          break;
        case CONFIRM_ADD_LNS:
          AddRemove.handlePacketConfirmAddLNS(json);
          break;
        case CONFIRM_REMOVE_LNS:
          AddRemove.handlePacketConfirmRemoveLNS(json);
          break;
        // Update
        case UPDATE_ADDRESS_LNS:
          Update.handlePacketUpdateAddressLNS(json);
          break;
        case CONFIRM_UPDATE_LNS:
          Update.handlePacketConfirmUpdateLNS(json);
          break;
        // Others
        case NAMESERVER_SELECTION:
          NameServerVoteThread.handleNameServerSelection(json);
          break;
        case REQUEST_ACTIVES:
          SendActivesRequestTask.handleActivesRequestReply(json);
          break;
        case TINY_QUERY:
          LNSRecvTinyQuery.logQueryResponse(json);
          // LNSRecvTinyQuery.recvdQueryResponse(new TinyQuery(json));
          break;

      }
    } catch (JSONException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (NoSuchAlgorithmException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (UnsupportedEncodingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (UnknownHostException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}

class LNSPacketDemultiplexer extends PacketDemultiplexer {

  @Override
  public void handleJSONObjects(ArrayList jsonObjects) {
    for (Object o : jsonObjects) {
      LocalNameServer.executorService.submit(new MyTask((JSONObject) o));
    }
  }
}

class MyTask extends TimerTask {

  JSONObject json;

  public MyTask(JSONObject jsonObject) {
    this.json = jsonObject;
  }

  @Override
  public void run() {
//    long t0 = System.currentTimeMillis();
    LNSListener.demultiplexLNSPackets(json);
//    long t1 = System.currentTimeMillis();
//    if (t1 - t0 > 10) {
//      GNS.getLogger().severe("LNS-long-response\t" + (t1-t0)+"\t"+ System.currentTimeMillis());
//    }
  }
}
