package edu.umass.cs.gns.test.nioclient;

import edu.umass.cs.gns.localnameserver.*;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.*;
import edu.umass.cs.gns.nsdesign.packet.*;
import edu.umass.cs.gns.util.UniqueIDHashMap;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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
public class DBClientIntercessor extends BasicPacketDemultiplexer implements IntercessorInterface {

  private ConcurrentHashMap<Integer, JSONObject> reqIDToJSON = new ConcurrentHashMap<>(); // map from request ID to packets

  private UniqueIDHashMap uniqueIDHashMap = new UniqueIDHashMap(); // map from request ID to packets

  private NIOTransport nioTransport;

  private BasicPacketDemultiplexer lnsDemux;

  public DBClientIntercessor(int ID, final int port, BasicPacketDemultiplexer lnsDemux) throws IOException {
    this.lnsDemux = lnsDemux;

    this.nioTransport  = new NIOTransport(ID, new NodeConfig() {
      @Override
      public boolean containsNodeInfo(int ID) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Set<Integer> getNodeIDs() {
        throw new UnsupportedOperationException();
      }

      @Override
      public InetAddress getNodeAddress(int ID) {
        try {
          return InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
          e.printStackTrace();
        }
        return null;
      }

      @Override
      public int getNodePort(int ID) {
        return port;
      }
    }, new JSONMessageExtractor(this));
    new Thread(nioTransport).start();
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
          AddRecordPacket addRecordPacket = new AddRecordPacket(incomingJson);
          addRecordPacket.setRequestID(intercessorRequestID);
          outgoingJson = addRecordPacket.toJSONObject();
          break;
        case REMOVE_RECORD:
          RemoveRecordPacket removeRecordPacket = new RemoveRecordPacket(incomingJson);
          removeRecordPacket.setRequestID(intercessorRequestID);
          outgoingJson = removeRecordPacket.toJSONObject();
          break;
        case UPDATE:
          UpdatePacket updatePacket = new UpdatePacket(incomingJson);
          updatePacket.setRequestID(intercessorRequestID);
          outgoingJson = updatePacket.toJSONObject();
          break;
        case DNS:
          DNSPacket dnsPacket = new DNSPacket(incomingJson);
          dnsPacket.getHeader().setId(intercessorRequestID);
          outgoingJson = dnsPacket.toJSONObjectQuestion();
          break;

      }
    } catch (JSONException e) {
      e.printStackTrace();
    }
    if (outgoingJson != null) lnsDemux.handleJSONObject(outgoingJson);
    return true;
  }

  // incoming packets from LNS
  @Override
  public void handleIncomingPacket(JSONObject incomingJson) {
    GNS.getLogger().fine("Intercessor received response ... " + incomingJson);
    int origReqID;
    JSONObject origJson = null; // json sent by client
    JSONObject outgoingJson = null;
    try {
      switch (Packet.getPacketType(incomingJson)) {
        case CONFIRM_ADD:
          origJson = (JSONObject) uniqueIDHashMap.remove(new ConfirmUpdatePacket(incomingJson).getRequestID());
          origReqID = new AddRecordPacket(origJson).getRequestID();
          ConfirmUpdatePacket confirmPkt = new ConfirmUpdatePacket(incomingJson);
          confirmPkt.setRequestID(origReqID);
          outgoingJson = confirmPkt.toJSONObject();
          break;
        case CONFIRM_REMOVE:
          origJson = (JSONObject) uniqueIDHashMap.remove(new ConfirmUpdatePacket(incomingJson).getRequestID());
          origReqID = new RemoveRecordPacket(origJson).getRequestID();
          confirmPkt = new ConfirmUpdatePacket(incomingJson);
          confirmPkt.setRequestID(origReqID);
          outgoingJson = confirmPkt.toJSONObject();
          break;
        case CONFIRM_UPDATE:
          origJson = (JSONObject) uniqueIDHashMap.remove(new ConfirmUpdatePacket(incomingJson).getRequestID());
          origReqID = new UpdatePacket(origJson).getRequestID();
          confirmPkt = new ConfirmUpdatePacket(incomingJson);
          confirmPkt.setRequestID(origReqID);
          outgoingJson = confirmPkt.toJSONObject();
          break;
        case DNS:
          origJson = (JSONObject) uniqueIDHashMap.remove(new DNSPacket(incomingJson).getQueryId());
          origReqID = new DNSPacket(origJson).getQueryId();
          DNSPacket dnsPacket = new DNSPacket(incomingJson);
          dnsPacket.getHeader().setId(origReqID);
          outgoingJson = dnsPacket.toJSONObject();
          break;
        default:
          break;
      }

      if (origJson != null && outgoingJson != null) {
        String ip = origJson.getString(GNSNIOTransport.DEFAULT_IP_FIELD);
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

  public static void main(String[] args) throws IOException {
    /** This is a test for DBClientIntercessor and DBClient using a fake local name server. **/

    // restrict logging level to INFO to see only meaningful messages
    GNS.consoleOutputLevel = GNS.statConsoleOutputLevel = GNS.fileLoggingLevel = GNS.statFileLoggingLevel = "INFO";
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



