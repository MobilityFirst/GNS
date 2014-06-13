package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.clientsupport.UpdateOperation;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nio.BasicPacketDemultiplexer;
import edu.umass.cs.gns.nio.GNSNIOTransport;
import edu.umass.cs.gns.nio.JSONMessageExtractor;
import edu.umass.cs.gns.nsdesign.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.packet.*;
import edu.umass.cs.gns.util.NSResponseCode;
import edu.umass.cs.gns.util.NameRecordKey;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.util.Util;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.*;

/**
 *
 * Created by abhigyan on 6/13/14.
 */
public class TESTLocalNameServer {

  private static HashMap<Integer, GNSNIOTransport> nsNiots = new HashMap<>();

  private static Timer t = new Timer();

  public static void main(String[] args) throws IOException, JSONException, InterruptedException {
    String configFile = "scripts/8nodeslocal/name-server-info";
    GNS.fileLoggingLevel = GNS.consoleOutputLevel = "SEVERE";
    StartLocalNameServer.experimentMode = false;
    StartLocalNameServer.debugMode = false;
    StartLocalNameServer.startLNSConfigFile(8, configFile, null, null);

    GNSNodeConfig gnsNodeConfig = new GNSNodeConfig(configFile, 0);

    for (int nameServerID: gnsNodeConfig.getNameServerIDs()) {
      JSONMessageExtractor worker = new JSONMessageExtractor(new TestPacketDemux(nameServerID));
      GNSNIOTransport gnsnioTransport = new GNSNIOTransport(nameServerID, new GNSNodeConfig(configFile, nameServerID), worker);
      new Thread(gnsnioTransport).start();
      nsNiots.put(nameServerID, gnsnioTransport);
    }
    int numRequests = 1000;
    for (int i = 0; i < numRequests; i++) {
      // send 1 lookup with invalid response
      new LNSPacketDemultiplexer(LocalNameServer.getRequestHandler()).handleJSONObject(new DNSPacket(-1, i, "abcd", NameRecordKey.EdgeRecord, null, null, null).toJSONObjectQuestion());

      // send 1 update with invalid response
      ResultValue newValue = new ResultValue();
      newValue.add(Util.randomString(10));
      UpdatePacket updateAddressPacket = new UpdatePacket(-1, 0, 0, "abcd", NameRecordKey.EdgeRecord,
              newValue, null, -1, UpdateOperation.REPLACE_ALL, i, i, GNS.DEFAULT_TTL_SECONDS, null, null, null);
      new LNSPacketDemultiplexer(LocalNameServer.getRequestHandler()).handleJSONObject(updateAddressPacket.toJSONObject());
      Thread.sleep(5);
    }
    int numNames = 1000;
    for (int i = 0; i < numNames; i++) {
      // send 1 lookup with invalid response
      new LNSPacketDemultiplexer(LocalNameServer.getRequestHandler()).handleJSONObject(new DNSPacket(-1, i, "abcd"+i, NameRecordKey.EdgeRecord, null, null, null).toJSONObjectQuestion());

      // send 1 update with invalid response
      ResultValue newValue = new ResultValue();
      newValue.add(Util.randomString(10));
      UpdatePacket updateAddressPacket = new UpdatePacket(-1, 0, 0, "abcd"+i, NameRecordKey.EdgeRecord,
              newValue, null, -1, UpdateOperation.REPLACE_ALL, i, i, GNS.DEFAULT_TTL_SECONDS, null, null, null);
      new LNSPacketDemultiplexer(LocalNameServer.getRequestHandler()).handleJSONObject(updateAddressPacket.toJSONObject());
      Thread.sleep(5);
    }

    Thread.sleep(30000);
    System.exit(2);
  }

  private static Set<Integer> getActiveNameServers(String name) {
    Set<Integer> activeNameServers = new HashSet<>();
    activeNameServers.add(0);
    activeNameServers.add(1);
    activeNameServers.add(2);
    return activeNameServers;
  }

  private static long getDelay() {
    return new Random().nextInt(2000);
  }

  public static void handleLookup(int nodeID, JSONObject json) throws JSONException, IOException {
    DNSPacket dnsPacket = new DNSPacket(json);
    dnsPacket.getHeader().setResponseCode(NSResponseCode.ERROR_INVALID_ACTIVE_NAMESERVER);
    dnsPacket.getHeader().setQRCode(DNSRecordType.RESPONSE);
    send(nodeID, dnsPacket.getLnsId(), dnsPacket.toJSONObjectForErrorResponse(), getDelay());
  }

  public static void handleRequestActives(int nodeID, JSONObject json) throws JSONException, IOException {
    RequestActivesPacket requestActives = new RequestActivesPacket(json);
    requestActives.setActiveNameServers(getActiveNameServers(requestActives.getName()));
    send(nodeID, requestActives.getLNSID(), requestActives.toJSONObject(), getDelay());
  }

  private static void send(final int nsID, final int lnsID, final JSONObject json, long delay) {
    if (delay > 0) {
      t.schedule(new TimerTask() {
        @Override
        public void run() {
          try {
            nsNiots.get(nsID).sendToID(lnsID, json);
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }, delay);
    } else {
      try {
        nsNiots.get(nsID).sendToID(lnsID, json);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

}

class TestPacketDemux extends BasicPacketDemultiplexer {
  private int nodeID;
  public TestPacketDemux(int nodeID) {
    this.nodeID = nodeID;
  }

  @Override
  public boolean handleJSONObject(JSONObject jsonObject) {
    GNS.getLogger().fine("Json object received: " + jsonObject);
    try {
      Packet.PacketType type = Packet.getPacketType(jsonObject);
      switch (type) {
        case REQUEST_ACTIVES:
          TESTLocalNameServer.handleRequestActives(nodeID, jsonObject);
          break;
        case DNS:
          TESTLocalNameServer.handleLookup(nodeID, jsonObject);
          break;
      }
    } catch (JSONException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return true;
  }
}
