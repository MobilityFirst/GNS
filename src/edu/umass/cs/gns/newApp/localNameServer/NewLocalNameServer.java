package edu.umass.cs.gns.newApp.localNameServer;

import edu.umass.cs.gns.clientsupport.Admintercessor;
import edu.umass.cs.gns.clientsupport.Intercessor;
import edu.umass.cs.gns.clientsupport.UpdateOperation;
import edu.umass.cs.gns.localnameserver.LNSListenerAdmin;
import edu.umass.cs.gns.localnameserver.httpserver.GnsHttpServer;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.RequestHandlerParameters;
import edu.umass.cs.gns.reconfiguration.examples.*;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.gns.nio.GenericMessagingTask;
import edu.umass.cs.gns.nio.JSONMessenger;
import edu.umass.cs.gns.nio.JSONNIOTransport;
import edu.umass.cs.gns.nio.nioutils.PacketDemultiplexerDefault;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.packet.UpdatePacket;
import edu.umass.cs.gns.ping.PingManager;
import edu.umass.cs.gns.protocoltask.ProtocolExecutor;
import edu.umass.cs.gns.reconfiguration.InterfaceReconfigurableNodeConfig;
import edu.umass.cs.gns.reconfiguration.RequestParseException;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.DeleteServiceName;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.gns.util.MyLogger;
import edu.umass.cs.gns.util.ValuesMap;
import java.net.InetSocketAddress;

/**
 * @author Westy
 * @param <NodeIDType>
 */
public class NewLocalNameServer<NodeIDType> {

  private final InetSocketAddress nodeAddress;
  private final InterfaceReconfigurableNodeConfig<NodeIDType> nodeConfig;
  private final JSONMessenger<NodeIDType> messenger;
  /**
   * Handles the client support processing for the local name server.
   */
  private final Intercessor<NodeIDType> intercessor;

  /**
   * Handles administrative client support for the local name server.
   */
  private final Admintercessor<NodeIDType> admintercessor;
  /**
   * Ping manager object for pinging other nodes and updating ping latencies.
   */
  private final PingManager<NodeIDType> pingManager;
  /**
   * We also keep a pointer to the lnsListenerAdmin so we can shut it down.
   */
  private final LNSListenerAdmin<NodeIDType> lnsListenerAdmin;

  EnhancedClientRequestHandlerInterface<NodeIDType> requestHandler;

 
  private final Logger log = Logger.getLogger(getClass().getName());

  NewLocalNameServer(InetSocketAddress nodeAddress, GNSNodeConfig<NodeIDType> gnsNodeConfig,
          JSONMessenger<NodeIDType> messenger) throws IOException {
    AbstractPacketDemultiplexer demultiplexer = new NewLNSPacketDemultiplexer();
    this.intercessor = new Intercessor<>(nodeAddress, gnsNodeConfig, demultiplexer);
    this.admintercessor = new Admintercessor<>();
    this.nodeAddress = nodeAddress;
    this.nodeConfig = gnsNodeConfig;
    this.messenger = messenger;
    messenger.addPacketDemultiplexer(demultiplexer);
    RequestHandlerParameters parameters = new RequestHandlerParameters();
    //
    parameters.setDebugMode(true);
    //
    this.requestHandler = new NewClientRequestHandler<>(intercessor, admintercessor, nodeAddress,
            gnsNodeConfig, messenger, parameters);
    ((NewLNSPacketDemultiplexer) demultiplexer).setHandler(requestHandler);
    // Start HTTP server
    GnsHttpServer.runHttp(requestHandler);
    // Start Ping servers
    GNS.getLogger().info("LNS running at " + nodeAddress + " started Ping server on port " + GNS.DEFAULT_LNS_PING_PORT);
    this.pingManager = new PingManager<>(null, gnsNodeConfig);
    pingManager.startPinging();
    //
    // After starting PingManager because it accesses PingManager.
    (this.lnsListenerAdmin = new LNSListenerAdmin<>(requestHandler, pingManager)).start();

    // The Admintercessor needs to use the LNSListenerAdmin;
    this.admintercessor.setListenerAdmin(lnsListenerAdmin);

  }

//  private CreateServiceName makeCreateNameRequest(String name, String state) {
//    CreateServiceName create = new CreateServiceName(null, name, 0, state);
//    return create;
//  }
//
//  private DeleteServiceName makeDeleteNameRequest(String name, String state) {
//    DeleteServiceName delete = new DeleteServiceName(null, name, 0);
//    return delete;
//  }
  private int requestId = 0; // just for shits and giggles

  private UpdatePacket makeUpdateRequest(String name, String value) throws JSONException {
    UpdatePacket packet = new UpdatePacket(null, requestId++, -1, name, null, null, null, -1,
            new ValuesMap(new JSONObject(value)), UpdateOperation.USER_JSON_REPLACE,
            nodeAddress, "", 0, null, null, null);
    return packet;
  }

//  private NodeIDType getRandomReplica() {
//    int index = (int) (this.nodeConfig.getActiveReplicas().size() * Math.random());
//    return (NodeIDType) (this.nodeConfig.getActiveReplicas().toArray()[index]);
//  }
//
//  private NodeIDType getRandomRCReplica() {
//    int index = (int) (this.nodeConfig.getReconfigurators().size() * Math.random());
//    return (NodeIDType) (this.nodeConfig.getReconfigurators().toArray()[index]);
//  }
//
//  private NodeIDType getFirstReplica() {
//    return this.nodeConfig.getActiveReplicas().iterator().next();
//  }
//
//  private NodeIDType getFirstRCReplica() {
//    return this.nodeConfig.getReconfigurators().iterator().next();
//  }
  private void sendUpdateRequest(UpdatePacket req) throws JSONException, IOException, RequestParseException {
    NodeIDType id = (TestConfig.serverSelectionPolicy == TestConfig.ServerSelectionPolicy.FIRST ? requestHandler.getFirstReplica() : requestHandler.getRandomReplica());
    log.log(Level.INFO, MyLogger.FORMAT[7].replace(" ", ""), new Object[]{"Sending ", req.getRequestType(), " to ", id, ":", this.nodeConfig.getNodeAddress(id), ":", this.nodeConfig.getNodePort(id), ": ", req});
    req.setNameServerID(id); // necessary to get a confirmation back
    this.sendRequest(id, req.toJSONObject());
  }

  private void sendRequest(CreateServiceName req) throws JSONException, IOException {
    NodeIDType id = (TestConfig.serverSelectionPolicy == TestConfig.ServerSelectionPolicy.FIRST ? requestHandler.getFirstRCReplica() : requestHandler.getRandomRCReplica());
    log.log(Level.INFO, MyLogger.FORMAT[7].replace(" ", ""), new Object[]{"Sending ", req.getSummary(), " to ", id, ":", this.nodeConfig.getNodeAddress(id), ":", this.nodeConfig.getNodePort(id), ": ", req});
    this.sendRequest(id, req.toJSONObject());
  }

  private void sendRequest(DeleteServiceName req) throws JSONException, IOException {
    NodeIDType id = (TestConfig.serverSelectionPolicy == TestConfig.ServerSelectionPolicy.FIRST ? requestHandler.getFirstRCReplica() : requestHandler.getRandomRCReplica());
    log.log(Level.INFO, MyLogger.FORMAT[7].replace(" ", ""), new Object[]{"Sending ", req.getSummary(), " to ", id, ":", this.nodeConfig.getNodeAddress(id), ":", this.nodeConfig.getNodePort(id), ": ", req});
    this.sendRequest(id, req.toJSONObject());
  }

  private void sendRequest(NodeIDType id, JSONObject json) throws JSONException, IOException {
    this.messenger.send(new GenericMessagingTask<NodeIDType, Object>(id, json));
  }

  public InetSocketAddress getAddress() {
    return nodeAddress;
  }

  public static void main(String[] args) throws IOException {
    InetSocketAddress address = new InetSocketAddress("127.0.0.1", 24398);
    String filename = Config.WESTY_GNS_DIR_PATH + "/conf/name-server-info";
    GNSNodeConfig nodeConfig = new GNSNodeConfig(filename, true);
    NewLocalNameServer localNameServer = null;
    JSONMessenger messenger = new JSONMessenger<String>(
            (new JSONNIOTransport(address, nodeConfig, new PacketDemultiplexerDefault(),
                    true)).enableStampSenderPort());
    localNameServer = new NewLocalNameServer(address, nodeConfig, messenger);
    //runTestAndStop(localNameServer);
  }

  private static void runTestAndStop(NewLocalNameServer localNameServer) {
    try {
      int numRequests = 2;
      String namePrefix = "name";
      String updateValue = "{"
              + "  \"name\": \"John\","
              + "  \"number\": \"%d\"}";
      String initValue = "initial_value";

      localNameServer.sendRequest(localNameServer.requestHandler.makeCreateNameRequest(namePrefix + 0, initValue));

      Thread.sleep(2000);
      for (int i = 0; i < numRequests; i++) {
        localNameServer.sendUpdateRequest(localNameServer.makeUpdateRequest(namePrefix + 0, String.format(updateValue, i)));
        Thread.sleep(2000);
      }
      localNameServer.sendRequest(localNameServer.requestHandler.makeDeleteNameRequest(namePrefix + 0));

      //localNameServer.messenger.stop();
    } catch (IOException ioe) {
      ioe.printStackTrace();
    } catch (JSONException je) {
      je.printStackTrace();
    } catch (InterruptedException ie) {
      ie.printStackTrace();
    } catch (RequestParseException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    System.exit(0);
  }
}
