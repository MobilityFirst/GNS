package edu.umass.cs.gns.newApp.clientCommandProcessor;

import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.Admintercessor;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.Intercessor;
import edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport.CCPListenerAdmin;
import edu.umass.cs.gns.gnamed.DnsTranslator;
import edu.umass.cs.gns.gnamed.UdpDnsServer;
import edu.umass.cs.gns.httpserver.GnsHttpServer;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.util.ParametersAndOptions;
import static edu.umass.cs.gns.util.ParametersAndOptions.HELP;
import static edu.umass.cs.gns.newApp.clientCommandProcessor.ClientCommandProcessorOptions.*;
import edu.umass.cs.gns.main.RequestHandlerParameters;

import java.io.IOException;
import java.util.logging.Logger;

import edu.umass.cs.gns.nodeconfig.GNSConsistentReconfigurableNodeConfig;
import edu.umass.cs.gns.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.util.Shutdownable;
import edu.umass.cs.gns.ping.PingManager;
import edu.umass.cs.gns.util.NetworkUtils;
import edu.umass.cs.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.JSONNIOTransport;
import edu.umass.cs.nio.nioutils.PacketDemultiplexerDefault;
import edu.umass.cs.reconfiguration.InterfaceReconfigurableNodeConfig;

import java.net.BindException;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.util.Map;

/**
 *
 * @author Westy
 * @param <NodeIDType>
 */
public class ClientCommandProcessor<NodeIDType> implements Shutdownable {

  private final InetSocketAddress nodeAddress;
  private final InterfaceReconfigurableNodeConfig<NodeIDType> nodeConfig;
  private JSONMessenger<NodeIDType> messenger;
  /**
   * Handles the client support processing for the local name server.
   */
  private Intercessor<NodeIDType> intercessor;

  /**
   * Handles administrative client support for the local name server.
   */
  private Admintercessor<NodeIDType> admintercessor;
  /**
   * Ping manager object for pinging other nodes and updating ping latencies.
   */
  private PingManager<NodeIDType> pingManager;
  /**
   * We also keep a pointer to the lnsListenerAdmin so we can shut it down.
   */
  private CCPListenerAdmin<NodeIDType> lnsListenerAdmin;

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

  ClientCommandProcessor(String nsFile, String host, int port,
          boolean debug,
          NodeIDType replicaID,
          boolean dnsGnsOnly,
          boolean dnsOnly,
          String gnsServerIP
  ) throws IOException {
    this(new InetSocketAddress(host, port), new GNSNodeConfig<NodeIDType>(nsFile, true),
            debug, replicaID, dnsGnsOnly, dnsOnly, gnsServerIP);
  }

  public ClientCommandProcessor(InetSocketAddress nodeAddress, 
          GNSNodeConfig<NodeIDType> gnsNodeConfig,
          boolean debug,
          NodeIDType replicaID,
          boolean dnsGnsOnly,
          boolean dnsOnly,
          String gnsServerIP) throws IOException {

    if (debug) {
      System.out.println("******** DEBUGGING IS ENABLED IN THE CCP *********");
    }
    AbstractPacketDemultiplexer demultiplexer = new CCPPacketDemultiplexer<NodeIDType>();
    this.intercessor = new Intercessor<>(nodeAddress, gnsNodeConfig, demultiplexer);
    this.admintercessor = new Admintercessor<>();
    this.nodeAddress = nodeAddress;
    this.nodeConfig = gnsNodeConfig;

    RequestHandlerParameters parameters = new RequestHandlerParameters();
    try {
      this.messenger = new JSONMessenger<NodeIDType>(
              (new JSONNIOTransport(nodeAddress, gnsNodeConfig, new PacketDemultiplexerDefault(),
                      true)).enableStampSenderPort());
      //
      messenger.addPacketDemultiplexer(demultiplexer);
      //
      parameters.setDebugMode(debug);
    //parameters.setDebugMode(options.containsKey(DEBUG));
      //
      this.requestHandler = new NewClientRequestHandler<>(intercessor, admintercessor, nodeAddress,
              replicaID,
              //options.get(ClientCommandProcessorOptions.AR_ID),
              gnsNodeConfig, messenger, parameters);
      ((CCPPacketDemultiplexer) demultiplexer).setHandler(requestHandler);
      // Start HTTP server
      GnsHttpServer.runHttp(requestHandler);
      // Start Ping servers
      GNS.getLogger().info("CCP running at " + nodeAddress + " started Ping server on port " + GNS.DEFAULT_CCP_PING_PORT);
      this.pingManager = new PingManager<>(null, new GNSConsistentReconfigurableNodeConfig<NodeIDType>(gnsNodeConfig));
      pingManager.startPinging();
    //
      // After starting PingManager because it accesses PingManager.
      (this.lnsListenerAdmin = new CCPListenerAdmin<>(requestHandler, pingManager)).start();

      // The Admintercessor needs to use the CCPListenerAdmin;
      this.admintercessor.setListenerAdmin(lnsListenerAdmin);

      try {
        if (dnsGnsOnly) {
          //if (options.containsKey(DNS_GNS_ONLY)) {
          dnsTranslator = new DnsTranslator(Inet4Address.getByName("0.0.0.0"), 53, requestHandler);
          dnsTranslator.start();
        } else if (dnsOnly) {
          //} else if (options.containsKey(DNS_ONLY)) {
          if (gnsServerIP == null) {
            //if (options.get(GNS_SERVER_IP) == null) {
            GNS.getLogger().severe("FAILED TO START DNS SERVER: GNS Server IP must be specified");
            return;
          }
          GNS.getLogger().info("GNS Server IP" + gnsServerIP);
          //GNS.getLogger().warning("gns server IP" + StartLocalNameServer.gnsServerIP);
          udpDnsServer = new UdpDnsServer(Inet4Address.getByName("0.0.0.0"), 53, "8.8.8.8",
                  gnsServerIP, requestHandler);
          udpDnsServer.start();
        } else {
          udpDnsServer = new UdpDnsServer(Inet4Address.getByName("0.0.0.0"), 53, "8.8.8.8", null, requestHandler);
          udpDnsServer.start();
        }
      } catch (BindException e) {
        GNS.getLogger().warning("Not running DNS Service because it needs root permission! "
                + "If you want DNS run the CCP using sudo.");
      }
    } catch (BindException e) {
      GNS.getLogger().severe("Failed to create nio server at " + nodeAddress + ": " + e + " ;exiting.");
      System.exit(-1);
    }
  }

  public InetSocketAddress getAddress() {
    return nodeAddress;
  }

  public EnhancedClientRequestHandlerInterface<NodeIDType> getRequestHandler() {
    return requestHandler;
  }

  public static void startClientCommandProcessor(Map<String, String> options) throws IOException {
    ClientCommandProcessor<String> clientCommandProcessor
            = new ClientCommandProcessor<String>(
                    //address, nodeConfig, messenger,
                    options.get(NS_FILE),
                    options.containsKey(HOST) ? options.get(HOST) : NetworkUtils.getLocalHostLANAddress().getHostAddress(),
                    options.containsKey(PORT) ? Integer.parseInt(options.get(PORT)) : GNS.DEFAULT_CCP_TCP_PORT,
                    options.containsKey(DEBUG),
                    options.get(ClientCommandProcessorOptions.AR_ID),
                    options.containsKey(DNS_GNS_ONLY),
                    options.containsKey(DNS_ONLY),
                    options.get(GNS_SERVER_IP)
            );
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
    startClientCommandProcessor(options);
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
