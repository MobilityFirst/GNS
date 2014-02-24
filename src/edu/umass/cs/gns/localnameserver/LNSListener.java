package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nameserver.GNSNodeConfig;
import edu.umass.cs.gns.nio.*;
import edu.umass.cs.gns.packet.DNSPacket;
import edu.umass.cs.gns.packet.NameServerLoadPacket;
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
import java.util.Random;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

/**
 * Listens on a UDP port for requests from end-users, and responses from name servers.
 *
 * @author abhigyan
 *
 */
public class LNSListener extends Thread {

  public static Transport udpTransport;

  public static NioServer tcpTransport;

  public LNSListener() throws IOException {
    super("LNSListener");
    GNS.getLogger().info("LNS Node " + LocalNameServer.nodeID + " starting LNSListenerUDP on port " 
            + ConfigFileInfo.getLNSUdpPort(LocalNameServer.nodeID));
    udpTransport = new Transport(LocalNameServer.nodeID, ConfigFileInfo.getLNSUdpPort(LocalNameServer.nodeID));
//    udpTransport.s
//    tcpTransport = new NioServer(LocalNameServer.nodeID,new ByteStreamToJSONObjects(this));


//    ByteStreamToJSONObjects worker = new ByteStreamToJSONObjects(new LNSPacketDemultiplexer());
////    new Thread(worker).start();
//    tcpTransport = new NioServer(LocalNameServer.nodeID, worker, new GNSNodeConfig());
//    new Thread(tcpTransport).start();

    tcpTransport = new NioServer(LocalNameServer.nodeID, new ByteStreamToJSONObjects(new LNSPacketDemultiplexer()), new GNSNodeConfig());
    new Thread(tcpTransport).start();

//    JSONMessageWorker worker = new JSONMessageWorker(new LNSPacketDemultiplexer());
//    tcpTransport = new GNSNIOTransport(LocalNameServer.nodeID, new GNSNodeConfig(), worker);
//    new Thread(tcpTransport).start();

    GNSNodeConfig nodeConfig = new GNSNodeConfig();
    GNS.getLogger().info("Debug mode: node 0 address: " + nodeConfig.getNodeAddress(0));
    GNS.getLogger().info("Debug mode: node 1 address: " + nodeConfig.getNodeAddress(1));

//    tcpTransport = new NioServer(LocalNameServer.nodeID, new ByteStreamToJSONObjects(new LNSPacketDemultiplexer()), new GNSNodeConfig());


  }

  @Override
  public void run() {
    while (true) {
      JSONObject json = udpTransport.readPacket();
//      GNS.getLogger().fine("Recvd packet at LNS ..." );
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
        // Update
        case UPDATE_ADDRESS_LNS:
          Update.handlePacketUpdateAddressLNS(json);
          break;
        case CONFIRM_UPDATE_LNS:
          Update.handlePacketConfirmUpdateLNS(json);
          break;
        case NAME_SERVER_LOAD:
          handleNameServerLoadPacket(json);
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
        // Others
        case NAMESERVER_SELECTION:
          NameServerVoteThread.handleNameServerSelection(json);
          break;
        case REQUEST_ACTIVES:
          RequestActivesTask.handleActivesRequestReply(json);
          break;
        case SELECT_REQUEST:
          Select.handlePacketSelectRequest(json);
          break;
        case SELECT_RESPONSE:
          Select.handlePacketSelectResponse(json);
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

  private static void handleNameServerLoadPacket(JSONObject json) throws JSONException {

    NameServerLoadPacket nsLoad = new NameServerLoadPacket(json);
    LocalNameServer.nameServerLoads.put(nsLoad.getNsID(), nsLoad.getLoadValue());


  }

  public static void sendPacketWithDelay(JSONObject json, int destID) {
    Random r = new Random();
    double latency = ConfigFileInfo.getPingLatency(destID)
            * (1 + r.nextDouble() * StartLocalNameServer.variation);
    long timerDelay = (long) latency;

    LocalNameServer.executorService.schedule(new SendQueryWithDelayLNS(json, destID), timerDelay, TimeUnit.MILLISECONDS);
  }
}

class LNSPacketDemultiplexer extends PacketDemultiplexer {

  @Override
  public void handleJSONObjects(ArrayList jsonObjects) {
    for (Object o : jsonObjects) {
      LocalNameServer.executorService.submit(new LNSTask((JSONObject) o));
    }
  }
}

class LNSTask extends TimerTask {

  JSONObject json;

  public LNSTask(JSONObject jsonObject) {
    this.json = jsonObject;
  }

  @Override
  public void run() {

    try {
      LNSListener.demultiplexLNSPackets(json);
    } catch (Exception e) {
      GNS.getLogger().severe("Exception handling packets: " + e.getMessage());
      e.printStackTrace();
    }

  }
}
