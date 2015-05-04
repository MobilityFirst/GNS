package edu.umass.cs.gns.newApp.clientCommandProcessor;

import edu.umass.cs.gns.clientsupport.Admintercessor;
import edu.umass.cs.gns.clientsupport.Intercessor;
import edu.umass.cs.gns.clientCommandProcessor.CCPListenerAdmin;
import edu.umass.cs.gns.clientCommandProcessor.httpserver.GnsHttpServer;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.RequestHandlerParameters;
import java.io.IOException;
import java.util.logging.Logger;
import edu.umass.cs.gns.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.gns.nio.JSONMessenger;
import edu.umass.cs.gns.nio.JSONNIOTransport;
import edu.umass.cs.gns.nio.nioutils.PacketDemultiplexerDefault;
import edu.umass.cs.gns.nodeconfig.GNSConsistentReconfigurableNodeConfig;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.pingNew.PingManager;
import edu.umass.cs.gns.reconfiguration.InterfaceReconfigurableNodeConfig;
import edu.umass.cs.gns.util.NetworkUtils;
import java.net.InetSocketAddress;

/**
 *
 * @author Westy
 * @param <NodeIDType>
 */
public class NewClientCommandProcessor<NodeIDType> {

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
  private final CCPListenerAdmin<NodeIDType> lnsListenerAdmin;

  EnhancedClientRequestHandlerInterface<NodeIDType> requestHandler;

  private final Logger log = Logger.getLogger(getClass().getName());

  NewClientCommandProcessor(InetSocketAddress nodeAddress, GNSNodeConfig<NodeIDType> gnsNodeConfig,
          JSONMessenger<NodeIDType> messenger) throws IOException {
    AbstractPacketDemultiplexer demultiplexer = new NewCCPPacketDemultiplexer();
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
    ((NewCCPPacketDemultiplexer) demultiplexer).setHandler(requestHandler);
    // Start HTTP server
    GnsHttpServer.runHttp(requestHandler);
    // Start Ping servers
    GNS.getLogger().info("CCP running at " + nodeAddress + " started Ping server on port " + GNS.DEFAULT_LNS_PING_PORT);
    this.pingManager = new PingManager<>(null, new GNSConsistentReconfigurableNodeConfig(gnsNodeConfig));
    pingManager.startPinging();
    //
    // After starting PingManager because it accesses PingManager.
    (this.lnsListenerAdmin = new CCPListenerAdmin<>(requestHandler, pingManager)).start();

    // The Admintercessor needs to use the CCPListenerAdmin;
    this.admintercessor.setListenerAdmin(lnsListenerAdmin);

  }

  public InetSocketAddress getAddress() {
    return nodeAddress;
  }
  
  private static void startClientCommandProcessor(String host, int port, String nodeConfigFilename) throws IOException {
    InetSocketAddress address = new InetSocketAddress(host, port);
    String filename = nodeConfigFilename;
    GNSNodeConfig nodeConfig = new GNSNodeConfig(filename, true);
    JSONMessenger messenger = new JSONMessenger<String>(
            (new JSONNIOTransport(address, nodeConfig, new PacketDemultiplexerDefault(),
                    true)).enableStampSenderPort());
    NewClientCommandProcessor localNameServer = new NewClientCommandProcessor(address, nodeConfig, messenger);
  }

  public static void main(String[] args) throws IOException {
    if (args.length == 0) { // special case for testing
      startClientCommandProcessor(NetworkUtils.getLocalHostLANAddress().getHostAddress(), 
              GNS.DEFAULT_LNS_TCP_PORT, Config.WESTY_GNS_DIR_PATH + "/conf/name-server-info");
    } else if (args.length == 3) {
      startClientCommandProcessor(args[0], Integer.parseInt(args[1]), args[2]);
    } else {
      System.out.println("Usage: java -cp GNS.jar edu.umass.cs.gns.newApp.clientCommandProcessor.NewClientCommandProcessor <host> <port> <nodeConfigFile>");
    }
  }

  
}
