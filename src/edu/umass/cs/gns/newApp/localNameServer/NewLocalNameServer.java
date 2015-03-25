package edu.umass.cs.gns.newApp.localNameServer;

import edu.umass.cs.gns.clientsupport.Admintercessor;
import edu.umass.cs.gns.clientsupport.Intercessor;
import edu.umass.cs.gns.localnameserver.LNSListenerAdmin;
import edu.umass.cs.gns.localnameserver.httpserver.GnsHttpServer;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.RequestHandlerParameters;
import java.io.IOException;
import java.util.logging.Logger;


import edu.umass.cs.gns.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.gns.nio.JSONMessenger;
import edu.umass.cs.gns.nio.JSONNIOTransport;
import edu.umass.cs.gns.nio.nioutils.PacketDemultiplexerDefault;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.ping.PingManager;
import edu.umass.cs.gns.reconfiguration.InterfaceReconfigurableNodeConfig;
import java.net.InetSocketAddress;

/**
 * 
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

  public InetSocketAddress getAddress() {
    return nodeAddress;
  }

  public static void main(String[] args) throws IOException {
    InetSocketAddress address = new InetSocketAddress("127.0.0.1", 24398);
    String filename = Config.WESTY_GNS_DIR_PATH + "/conf/name-server-info";
    GNSNodeConfig nodeConfig = new GNSNodeConfig(filename, true);
    JSONMessenger messenger = new JSONMessenger<String>(
            (new JSONNIOTransport(address, nodeConfig, new PacketDemultiplexerDefault(),
                    true)).enableStampSenderPort());
    NewLocalNameServer localNameServer = new NewLocalNameServer(address, nodeConfig, messenger);
  }
}
