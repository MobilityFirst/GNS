package edu.umass.cs.gns.newApp.clientCommandProcessor;

import edu.umass.cs.gns.clientsupport.Admintercessor;
import edu.umass.cs.gns.clientsupport.Intercessor;
import edu.umass.cs.gns.clientCommandProcessor.CCPListenerAdmin;
import edu.umass.cs.gns.clientCommandProcessor.gnamed.DnsTranslator;
import edu.umass.cs.gns.clientCommandProcessor.gnamed.UdpDnsServer;
import edu.umass.cs.gns.clientCommandProcessor.httpserver.GnsHttpServer;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.util.ParametersAndOptions;
import static edu.umass.cs.gns.util.ParametersAndOptions.HELP;
import static edu.umass.cs.gns.newApp.clientCommandProcessor.ClientCommandProcessorOptions.*;
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
import edu.umass.cs.gns.nsdesign.Shutdownable;
import edu.umass.cs.gns.ping.PingManager;
import edu.umass.cs.gns.reconfiguration.InterfaceReconfigurableNodeConfig;
import edu.umass.cs.gns.util.NetworkUtils;
import java.net.BindException;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Westy
 * @param <NodeIDType>
 */
public class ClientCommandProcessor<NodeIDType> implements Shutdownable {

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

  /**
   * We keep a pointer to the udpDnsServer so we can shut it down.
   */
  private UdpDnsServer udpDnsServer;

  /**
   * We keep a pointer to the dnsTranslator so we can shut it down.
   */
  private DnsTranslator dnsTranslator;

  private final Logger log = Logger.getLogger(getClass().getName());

  ClientCommandProcessor(InetSocketAddress nodeAddress, GNSNodeConfig<NodeIDType> gnsNodeConfig,
          JSONMessenger<NodeIDType> messenger, Map<String, String> options) throws IOException {
    AbstractPacketDemultiplexer demultiplexer = new CCPPacketDemultiplexer();
    this.intercessor = new Intercessor<>(nodeAddress, gnsNodeConfig, demultiplexer);
    this.admintercessor = new Admintercessor<>();
    this.nodeAddress = nodeAddress;
    this.nodeConfig = gnsNodeConfig;
    this.messenger = messenger;
    messenger.addPacketDemultiplexer(demultiplexer);
    RequestHandlerParameters parameters = new RequestHandlerParameters();
    //
    parameters.setDebugMode(options.containsKey(DEBUG));
    //
    this.requestHandler = new NewClientRequestHandler<>(intercessor, admintercessor, nodeAddress,
            gnsNodeConfig, messenger, parameters);
    ((CCPPacketDemultiplexer) demultiplexer).setHandler(requestHandler);
    // Start HTTP server
    GnsHttpServer.runHttp(requestHandler);
    // Start Ping servers
    GNS.getLogger().info("CCP running at " + nodeAddress + " started Ping server on port " + GNS.DEFAULT_CPP_PING_PORT);
    this.pingManager = new PingManager<>(null, new GNSConsistentReconfigurableNodeConfig(gnsNodeConfig));
    pingManager.startPinging();
    //
    // After starting PingManager because it accesses PingManager.
    (this.lnsListenerAdmin = new CCPListenerAdmin<>(requestHandler, pingManager)).start();

    // The Admintercessor needs to use the CCPListenerAdmin;
    this.admintercessor.setListenerAdmin(lnsListenerAdmin);

    try {
      if (options.containsKey(DNS_GNS_ONLY)) {
        //if (StartLocalNameServer.dnsGnsOnly) {
        dnsTranslator = new DnsTranslator(Inet4Address.getByName("0.0.0.0"), 53, requestHandler);
        dnsTranslator.start();
      } else if (options.containsKey(DNS_ONLY)) {
        //} else if (StartLocalNameServer.dnsOnly) {
        if (options.get(GNS_SERVER_IP) == null) {
          //if (StartLocalNameServer.gnsServerIP == null) {
          GNS.getLogger().severe("FAILED TO START DNS SERVER: GNS Server IP must be specified");
          return;
        }
        GNS.getLogger().info("GNS Server IP" + options.get(GNS_SERVER_IP));
        //GNS.getLogger().warning("gns server IP" + StartLocalNameServer.gnsServerIP);
        udpDnsServer = new UdpDnsServer(Inet4Address.getByName("0.0.0.0"), 53, "8.8.8.8",
                options.get(GNS_SERVER_IP), requestHandler);
        udpDnsServer.start();
      } else {
        udpDnsServer = new UdpDnsServer(Inet4Address.getByName("0.0.0.0"), 53, "8.8.8.8", null, requestHandler);
        udpDnsServer.start();
      }
    } catch (BindException e) {
      GNS.getLogger().warning("Not running DNS Service because it needs root permission! "
              + "If you want DNS run the CPP using sudo.");
    }
  }

  public InetSocketAddress getAddress() {
    return nodeAddress;
  }

  private static void startClientCommandProcessor(String host, int port,
          String nodeConfigFilename, Map<String, String> options) throws IOException {
    InetSocketAddress address = new InetSocketAddress(host, port);
    String filename = nodeConfigFilename;
    GNSNodeConfig nodeConfig = new GNSNodeConfig(filename, true);
    JSONMessenger messenger = new JSONMessenger<String>(
            (new JSONNIOTransport(address, nodeConfig, new PacketDemultiplexerDefault(),
                    true)).enableStampSenderPort());
    ClientCommandProcessor localNameServer = new ClientCommandProcessor(address, nodeConfig,
            messenger, options);
  }

  public static void main(String[] args) throws IOException {
    Map<String, String> options
            = ParametersAndOptions.getParametersAsHashMap(ClientCommandProcessor.class.getCanonicalName(),
                    ClientCommandProcessorOptions.getAllOptions(), args);
    if (options.containsKey(HELP)) {
      ParametersAndOptions.printUsage(ClientCommandProcessor.class.getCanonicalName(),
              ClientCommandProcessorOptions.getAllOptions());
      System.exit(0);
    }
    if (args.length == 0) { // special case for testing
      startClientCommandProcessor(NetworkUtils.getLocalHostLANAddress().getHostAddress(),
              GNS.DEFAULT_CCP_TCP_PORT, Config.WESTY_GNS_DIR_PATH + "/conf/name-server-info", options);
    } else {
      startClientCommandProcessor(options.get(HOST),
              Integer.parseInt(options.get(PORT)),
              options.get(NS_FILE),
              options);
    }
  }

  @Override
  public void shutdown() {
    if (udpDnsServer != null) {
      udpDnsServer.shutdown();
    }
    if (dnsTranslator != null) {
      dnsTranslator.shutdown();
    }
//    if (nodeConfig != null) {
//      nodeConfig.shutdown();
//    }
    if (pingManager != null) {
      pingManager.shutdown();
    }
    if (lnsListenerAdmin != null) {
      lnsListenerAdmin.shutdown();
    }
  }

}
