package edu.umass.cs.gns.test.nioclient;

import edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport.IntercessorInterface;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.gns.nio.InterfaceNodeConfig;
import edu.umass.cs.gns.nio.JSONMessageExtractor;
import edu.umass.cs.gns.nio.JSONNIOTransport;
import edu.umass.cs.gns.nio.NIOTransport;
import edu.umass.cs.gns.newApp.packet.AddRecordPacket;
import edu.umass.cs.gns.newApp.packet.ConfirmUpdatePacket;
import edu.umass.cs.gns.newApp.packet.DNSPacket;
import edu.umass.cs.gns.newApp.packet.Packet;
import static edu.umass.cs.gns.newApp.packet.Packet.PacketType.ADD_RECORD;
import static edu.umass.cs.gns.newApp.packet.Packet.PacketType.ADD_CONFIRM;
import static edu.umass.cs.gns.newApp.packet.Packet.PacketType.REMOVE_CONFIRM;
import static edu.umass.cs.gns.newApp.packet.Packet.PacketType.UPDATE_CONFIRM;
import static edu.umass.cs.gns.newApp.packet.Packet.PacketType.DNS;
import static edu.umass.cs.gns.newApp.packet.Packet.PacketType.REMOVE_RECORD;
import static edu.umass.cs.gns.newApp.packet.Packet.PacketType.UPDATE;
import edu.umass.cs.gns.newApp.packet.RemoveRecordPacket;
import edu.umass.cs.gns.newApp.packet.UpdatePacket;
import edu.umass.cs.gns.util.UniqueIDHashMap;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.json.JSONArray;

/**
 * This class is the intermediate between a DBClient and a local name server. DBClient is an alternate interface to
 * use GNS, which we expect to be used only for testing purposes.
 *
 * The functioning of this class is described as follows. It stores the original
 * client request packet which contains information about IP/port on which client is listening as well as a client-
 * specific request ID. As client-specific request ID may not be unique across multiple clients, it generates a unique
 * ID for each request, inserts this unique ID in each request before forwarding the request to a local name server.
 * After local name server sends a response, it retrieves the original client request, puts the client-specific request
 * ID in the response, and transmits the response on the IP/port on which the client is listening.
 *
 * Created by abhigyan on 6/19/14.
 */
@SuppressWarnings("unchecked")
@Deprecated
public class DBClientIntercessor extends AbstractPacketDemultiplexer implements IntercessorInterface {

  private ConcurrentHashMap<Integer, JSONObject> reqIDToJSON = new ConcurrentHashMap<>(); // map from request ID to packets

  private UniqueIDHashMap uniqueIDHashMap = new UniqueIDHashMap(); // map from request ID to packets

  private InterfaceNodeConfig nodeConfig;
  private NIOTransport nioTransport;

  private AbstractPacketDemultiplexer lnsDemux;

  public DBClientIntercessor(int ID, final int port, AbstractPacketDemultiplexer lnsDemux) throws IOException {
    this.lnsDemux = lnsDemux;
    this.nodeConfig = new InterfaceNodeConfig<Integer>() {
      @Override
      public boolean nodeExists(Integer ID) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Set<Integer> getNodeIDs() {
        throw new UnsupportedOperationException();
      }

      @Override
      public InetAddress getNodeAddress(Integer ID) {
        try {
          return InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
          e.printStackTrace();
        }
        return null;
      }

      @Override
      public int getNodePort(Integer ID) {
        return port;
      }

      @Override
      public Integer valueOf(String nodeAsString) {
        return Integer.valueOf(nodeAsString);
      }

      @Override
      public Set<Integer> getValuesFromStringSet(Set<String> strNodes) {
        throw new UnsupportedOperationException("Not supported yet.");
      }

      @Override
      public Set<Integer> getValuesFromJSONArray(JSONArray array) throws JSONException {
        throw new UnsupportedOperationException("Not supported yet.");
      }
      
    };
    this.nioTransport = new NIOTransport(ID, nodeConfig, new JSONMessageExtractor(this));
    new Thread(nioTransport).start();
  }

  public InterfaceNodeConfig getNodeConfig() {
    return nodeConfig;
  }

  // incoming packets from client
  @Override
  public boolean handleJSONObject(JSONObject incomingJson) {

    int intercessorRequestID = uniqueIDHashMap.put(incomingJson);

    GNS.getLogger().fine("Intercessor received request ... " + incomingJson);
    JSONObject outgoingJson = null;
    try {
      switch (Packet.getPacketType(incomingJson)) {
        case ADD_RECORD:
          AddRecordPacket addRecordPacket = new AddRecordPacket(incomingJson, nodeConfig);
          addRecordPacket.setRequestID(intercessorRequestID);
          outgoingJson = addRecordPacket.toJSONObject();
          break;
        case REMOVE_RECORD:
          RemoveRecordPacket removeRecordPacket = new RemoveRecordPacket(incomingJson, nodeConfig);
          removeRecordPacket.setRequestID(intercessorRequestID);
          outgoingJson = removeRecordPacket.toJSONObject();
          break;
        case UPDATE:
          UpdatePacket updatePacket = new UpdatePacket(incomingJson, nodeConfig);
          updatePacket.setRequestID(intercessorRequestID);
          outgoingJson = updatePacket.toJSONObject();
          break;
        case DNS:
          DNSPacket dnsPacket = new DNSPacket(incomingJson, nodeConfig);
          dnsPacket.getHeader().setId(intercessorRequestID);
          outgoingJson = dnsPacket.toJSONObjectQuestion();
          break;

      }
    } catch (JSONException e) {
      e.printStackTrace();
    }
    if (outgoingJson != null) {
      lnsDemux.handleJSONObject(outgoingJson);
    }
    return true;
  }

  // incoming packets from LNS
  @Override
  public void handleIncomingPacket(JSONObject incomingJson) {
    if (StartLocalNameServer.debuggingEnabled) {
      GNS.getLogger().fine("Intercessor received response ... " + incomingJson);
    }
    int origReqID;
    JSONObject origJson = null; // json sent by client
    JSONObject outgoingJson = null;
    try {
      switch (Packet.getPacketType(incomingJson)) {
        case ADD_CONFIRM:
          origJson = (JSONObject) uniqueIDHashMap.remove(new ConfirmUpdatePacket(incomingJson, nodeConfig).getRequestID());
          if (origJson == null) {
            break;
          }
          origReqID = new AddRecordPacket(origJson, nodeConfig).getRequestID();
          ConfirmUpdatePacket confirmPkt = new ConfirmUpdatePacket(incomingJson, nodeConfig);
          confirmPkt.setRequestID(origReqID);
          outgoingJson = confirmPkt.toJSONObject();
          break;
        case REMOVE_CONFIRM:
          origJson = (JSONObject) uniqueIDHashMap.remove(new ConfirmUpdatePacket(incomingJson, nodeConfig).getRequestID());
          if (origJson == null) {
            break;
          }
          origReqID = new RemoveRecordPacket(origJson, nodeConfig).getRequestID();
          confirmPkt = new ConfirmUpdatePacket(incomingJson, nodeConfig);
          confirmPkt.setRequestID(origReqID);
          outgoingJson = confirmPkt.toJSONObject();
          break;
        case UPDATE_CONFIRM:
          origJson = (JSONObject) uniqueIDHashMap.remove(new ConfirmUpdatePacket(incomingJson, nodeConfig).getRequestID());
          if (origJson == null) {
            break;
          }
          origReqID = new UpdatePacket(origJson, nodeConfig).getRequestID();
          confirmPkt = new ConfirmUpdatePacket(incomingJson, nodeConfig);
          confirmPkt.setRequestID(origReqID);
          outgoingJson = confirmPkt.toJSONObject();
          break;
        case DNS:
          origJson = (JSONObject) uniqueIDHashMap.remove(new DNSPacket(incomingJson, nodeConfig).getQueryId());
          if (origJson == null) {
            break;
          }
          origReqID = new DNSPacket(origJson, nodeConfig).getQueryId();
          DNSPacket dnsPacket = new DNSPacket(incomingJson, nodeConfig);
          dnsPacket.getHeader().setId(origReqID);
          outgoingJson = dnsPacket.toJSONObject();
          break;
        default:
          break;
      }

      if (origJson != null && outgoingJson != null) {
        String ip = origJson.getString(JSONNIOTransport.DEFAULT_IP_FIELD);
        int port = origJson.getInt(DBClient.DEFAULT_PORT_FIELD);
        sendJSONToClient(InetAddress.getByName(ip), port, outgoingJson);
      }
    } catch (JSONException | IOException e) {
      e.printStackTrace();
    }
  }

  private void sendJSONToClient(InetAddress inetAddress, int port, JSONObject jsonObject) throws IOException {
    String headeredMsg = JSONMessageExtractor.prependHeader(jsonObject.toString());
    this.nioTransport.send(new InetSocketAddress(inetAddress, port), headeredMsg.getBytes());
    GNS.getLogger().fine("NIO sent response to client ...");
  }

  public static void main(String[] args) throws IOException, JSONException, InterruptedException {
    /**
     * This is a test for DBClientIntercessor and DBClient using a fake local name server. *
     */

    // restrict logging level to INFO to see only meaningful messages
    GNS.consoleOutputLevel = GNS.fileLoggingLevel  = "INFO";
    GNS.getLogger().info("Starting test with fake local name server .. ");
    int lnsPort = 21323;
    int clientPort = 31323;
    TestPacketDemultiplexer tdm = new TestPacketDemultiplexer();
    DBClientIntercessor intercessor = new DBClientIntercessor(0, lnsPort, tdm);
    tdm.setIntercessor(intercessor);
    // this creates a DBClient after some time.
    ClientSample clientSample = new ClientSample(InetAddress.getLocalHost(), lnsPort, clientPort, true);
    clientSample.startClient();

    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    GNS.getLogger().fine("... quitting.");
    System.exit(0);
  }

}
