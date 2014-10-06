package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.clientsupport.UpdateOperation;
import edu.umass.cs.gns.database.ColumnFieldType;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.gns.nio.JSONNIOTransport;
import edu.umass.cs.gns.nio.JSONMessageExtractor;
import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.packet.*;
import edu.umass.cs.gns.util.NSResponseCode;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.util.Util;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;

/**
 *
 * Created by abhigyan on 6/13/14.
 */
public class TESTLocalNameServer {

  private static final HashMap<Object, JSONNIOTransport> nsNiots = new HashMap<>();

  private static final Timer t = new Timer();

  public static void main(String[] args) throws IOException, JSONException, InterruptedException {
    String configFile = "scripts/8nodeslocal/name-server-info";
    GNS.fileLoggingLevel = GNS.consoleOutputLevel = "SEVERE";
    StartLocalNameServer.experimentMode = false;
    StartLocalNameServer.debuggingEnabled = false;
    StartLocalNameServer.startLNSConfigFile(InetAddress.getLocalHost().getHostName(), 24398, configFile, null, null);

    GNSNodeConfig gnsNodeConfig = new GNSNodeConfig(configFile, GNSNodeConfig.BOGUS_NULL_NAME_SERVER_ID);

    for (Object nameServerID: gnsNodeConfig.getNodeIDs()) {
      JSONMessageExtractor worker = new JSONMessageExtractor(new TestPacketDemux(nameServerID));
      JSONNIOTransport gnsnioTransport = new JSONNIOTransport(nameServerID, new GNSNodeConfig(configFile, nameServerID), worker);
      new Thread(gnsnioTransport).start();
      nsNiots.put(nameServerID, gnsnioTransport);
    }
    int numRequests = 1000;
    for (int i = 0; i < numRequests; i++) {
      // send 1 lookup with invalid response
      new LNSPacketDemultiplexer(LocalNameServer.getRequestHandler()).handleJSONObject(new DNSPacket(GNSNodeConfig.INVALID_NAME_SERVER_ID, i, "abcd", 
              "EdgeRecord", null,
              ColumnFieldType.LIST_STRING, null, null, null).toJSONObjectQuestion());

      // send 1 update with invalid response
      ResultValue newValue = new ResultValue();
      newValue.add(Util.randomString(10));
      UpdatePacket updateAddressPacket = new UpdatePacket(GNSNodeConfig.INVALID_NAME_SERVER_ID, 0, 0, "abcd", "EdgeRecord",
              newValue, null, -1, null, UpdateOperation.SINGLE_FIELD_REPLACE_ALL, null, Integer.toString(i), GNS.DEFAULT_TTL_SECONDS, null, null, null);
      new LNSPacketDemultiplexer(LocalNameServer.getRequestHandler()).handleJSONObject(updateAddressPacket.toJSONObject());
      Thread.sleep(5);
    }
    int numNames = 1000;
    for (int i = 0; i < numNames; i++) {
      // send 1 lookup with invalid response
      new LNSPacketDemultiplexer(LocalNameServer.getRequestHandler()).handleJSONObject(new DNSPacket(GNSNodeConfig.INVALID_NAME_SERVER_ID, i, "abcd"+i, 
              "EdgeRecord", null,
              ColumnFieldType.LIST_STRING, null, null, null).toJSONObjectQuestion());

      // send 1 update with invalid response
      ResultValue newValue = new ResultValue();
      newValue.add(Util.randomString(10));
      UpdatePacket updateAddressPacket = new UpdatePacket(GNSNodeConfig.INVALID_NAME_SERVER_ID, 0, 0, "abcd"+i, "EdgeRecord",
              newValue, null, -1, null, UpdateOperation.SINGLE_FIELD_REPLACE_ALL, null, Integer.toString(i), GNS.DEFAULT_TTL_SECONDS, null, null, null);
      new LNSPacketDemultiplexer(LocalNameServer.getRequestHandler()).handleJSONObject(updateAddressPacket.toJSONObject());
      Thread.sleep(5);
    }

    Thread.sleep(30000);
    System.exit(2);
  }

  private static Set<Object> getActiveNameServers(String name) {
    Set<Object> activeNameServers = new HashSet<>();
    activeNameServers.add(Integer.toString(0));
    activeNameServers.add(Integer.toString(1));
    activeNameServers.add(Integer.toString(2));
    return activeNameServers;
  }

  private static long getDelay() {
    return new Random().nextInt(2000);
  }

  public static void handleLookup(Object nodeID, JSONObject json) throws JSONException, IOException {
    DNSPacket dnsPacket = new DNSPacket(json);
    dnsPacket.getHeader().setResponseCode(NSResponseCode.ERROR_INVALID_ACTIVE_NAMESERVER);
    dnsPacket.getHeader().setQRCode(DNSRecordType.RESPONSE);
    send(nodeID, dnsPacket.getLnsAddress(), dnsPacket.toJSONObjectForErrorResponse(), getDelay());
  }

  public static void handleRequestActives(Object nodeID, JSONObject json) throws JSONException, IOException {
    RequestActivesPacket requestActives = new RequestActivesPacket(json);
    requestActives.setActiveNameServers(getActiveNameServers(requestActives.getName()));
    send(nodeID, requestActives.getLnsAddress(), requestActives.toJSONObject(), getDelay());
  }

  private static void send(final Object nsID, final InetSocketAddress lnsAddress, final JSONObject json, long delay) {
    if (delay > 0) {
      t.schedule(new TimerTask() {
        @Override
        public void run() {
          try {
            nsNiots.get(nsID).sendToAddress(lnsAddress, json);
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }, delay);
    } else {
      try {
        nsNiots.get(nsID).sendToAddress(lnsAddress, json);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}

class TestPacketDemux extends AbstractPacketDemultiplexer {
  private Object nodeID;
  public TestPacketDemux(Object nodeID) {
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
