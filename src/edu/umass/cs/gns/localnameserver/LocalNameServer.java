/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.clientsupport.Intercessor;
import edu.umass.cs.gns.localnameserver.gnamed.DnsTranslator;
import edu.umass.cs.gns.localnameserver.gnamed.UdpDnsServer;
import edu.umass.cs.gns.localnameserver.httpserver.GnsHttpServer;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.RequestHandlerParameters;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nsdesign.Shutdownable;
import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.replicationframework.ReplicationFrameworkType;
import edu.umass.cs.gns.ping.PingManager;
import edu.umass.cs.gns.test.StartExperiment;
import edu.umass.cs.gns.test.nioclient.DBClientIntercessor;
import java.io.IOException;
import java.net.BindException;
import java.net.Inet4Address;
import java.net.InetSocketAddress;

/**
 * This class represents the functions of a Local Name Server.
 *
 * @author abhigyan
 * @param <NodeIDType>
 */
public class LocalNameServer<NodeIDType> implements Shutdownable {

  // FIXME: Future code cleanup note: The ClientRequestHandlerInterface and the IntercessorInterface
  // are closely related. Both encapsulate some functionality in the LocalNameServer that we might want to 
  // be able to abstract out (maybe to a Nameserver someday). There should be a way to combine them further.
  // One tanglible goal is to remove all references to static LocalNameServer calls in the code.
  /**
   * Implements handling of client requests, comms and caching.
   */
  private static ClientRequestHandlerInterface requestHandler;

  /**
   * A local name server forwards the final response for all requests to intercessor.
   */
  private static IntercessorInterface intercessor;
  
  public static Intercessor getIntercessor() {
    return (Intercessor)intercessor;
  }
  
  public static IntercessorInterface getIntercessorInterface() {
    return intercessor;
  }

  /**
   * Ping manager object for pinging other nodes and updating ping latencies in
   */
  // this one is static because it has a get method that is static
  private static PingManager pingManager;

  /**
   * We keep a pointer to the gnsNodeConfig so we can shut it down.
   */
  private GNSNodeConfig gnsNodeConfig;

  /**
   * We keep a pointer to the udpDnsServer so we can shut it down.
   */
  private UdpDnsServer udpDnsServer;

  /**
   * We keep a pointer to the dnsTranslator so we can shut it down.
   */
  private DnsTranslator dnsTranslator;
  
  private LNSListenerAdmin lnsListenerAdmin;
 
  public static PingManager getPingManager() {
    return pingManager;
  }

  /**
   **
   * Constructs a local name server and assigns it a node id.
   *
   * @throws IOException
   */
  public LocalNameServer(InetSocketAddress nodeAddress, GNSNodeConfig<NodeIDType> gnsNodeConfig) throws IOException, InterruptedException {
    System.out.println("Log level: " + GNS.getLogger().getLevel().getName());
    // keep a copy of this so we can shut it down later
    this.gnsNodeConfig = gnsNodeConfig;
    GNS.getLogger().info("GNS Version: " + GNS.readBuildVersion());
    RequestHandlerParameters parameters = new RequestHandlerParameters(StartLocalNameServer.debuggingEnabled,
            StartLocalNameServer.experimentMode,
            StartLocalNameServer.emulatePingLatencies,
            StartLocalNameServer.variation,
            StartLocalNameServer.adaptiveTimeout,
            StartLocalNameServer.outputSampleRate,
            StartLocalNameServer.queryTimeout,
            StartLocalNameServer.maxQueryWaitTime,
            StartLocalNameServer.cacheSize,
            StartLocalNameServer.loadDependentRedirection,
            StartLocalNameServer.replicationFramework
    );

    GNS.getLogger().info("Parameter values: " + parameters.toString());
    this.requestHandler = new BasicClientRequestHandler(nodeAddress, gnsNodeConfig, parameters);

    if (!parameters.isExperimentMode()) {
      // intercessor for regular GNS use
      this.intercessor = new Intercessor(requestHandler);
    } else {
      // intercessor for four simple DB operations: add, remove, write, read only.
      this.intercessor = new DBClientIntercessor(-1, GNS.DEFAULT_LNS_DBCLIENT_PORT,
              new LNSPacketDemultiplexer(requestHandler));
    }

    if (!parameters.isExperimentMode()) { // creates exceptions with multiple local name servers on a machine
      GnsHttpServer.runHttp();
    }

    if (!parameters.isEmulatePingLatencies()) {
      // we emulate latencies based on ping latency given in config file,
      // and do not want ping latency values to be updated by the ping module.
      GNS.getLogger().info("LNS running at " + LocalNameServer.getNodeAddress() + " started Ping server on port " + GNS.DEFAULT_LNS_PING_PORT);
      this.pingManager = new PingManager(PingManager.LOCALNAMESERVERID, gnsNodeConfig);
      pingManager.startPinging();
    }

    // After starting PingManager because it accesses PingManager.
    (this.lnsListenerAdmin = new LNSListenerAdmin()).start();

    if (parameters.getReplicationFramework() == ReplicationFrameworkType.LOCATION) {
      new NameServerVoteThread(StartLocalNameServer.voteIntervalMillis, requestHandler).start();
    }

    if (parameters.isExperimentMode()) {
      GNS.getLogger().info("Starting experiment ..... ");
      new StartExperiment().startMyTest(null, StartLocalNameServer.workloadFile, StartLocalNameServer.updateTraceFile,
              requestHandler);
    }

    try {
      if (StartLocalNameServer.dnsGnsOnly) {
        dnsTranslator = new DnsTranslator(Inet4Address.getByName("0.0.0.0"), 53);
        dnsTranslator.start();
      } else if (StartLocalNameServer.dnsOnly) {
        if (StartLocalNameServer.gnsServerIP == null) {
          GNS.getLogger().severe("FAILED TO START DNS SERVER: GNS Server IP is missing or invalid");
          return;
        }
        GNS.getLogger().warning("gns server IP" + StartLocalNameServer.gnsServerIP);
        udpDnsServer = new UdpDnsServer(Inet4Address.getByName("0.0.0.0"), 53, "8.8.8.8", StartLocalNameServer.gnsServerIP);
        udpDnsServer.start();
      } else {
        udpDnsServer = new UdpDnsServer(Inet4Address.getByName("0.0.0.0"), 53, "8.8.8.8", null);
        udpDnsServer.start();
      }
    } catch (BindException e) {
      GNS.getLogger().warning("Not running DNS Service because it needs root permission! If you want DNS run the LNS using sudo.");
    }

  }

  /**
   * Returns the host nodeAddress of this LN server.
   *
   * @return
   */
  public static InetSocketAddress getNodeAddress() {
    return requestHandler.getNodeAddress();
  }

  public static GNSNodeConfig getGnsNodeConfig() {
    return requestHandler.getGnsNodeConfig();
  }

  // CACHE METHODS
  public static void invalidateCache() {
    requestHandler.invalidateCache();
  }

  /**
   * Prints local name server cache (and sorts it for convenience)
   */
  public static String getCacheLogString(String preamble) {
    return requestHandler.getCacheLogString(preamble);
  }

  @Override
  public void shutdown() {
    if (udpDnsServer != null) {
      udpDnsServer.shutdown();
    }
    if (dnsTranslator != null) {
      dnsTranslator.shutdown();
    }
    if (gnsNodeConfig != null) {
      gnsNodeConfig.shutdown();
    }
    if (pingManager != null) {
      pingManager.shutdown();
    }
    if (lnsListenerAdmin != null) {
      lnsListenerAdmin.shutdown();
    }
  }

}
