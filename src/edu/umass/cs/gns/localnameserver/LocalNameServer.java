/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.gns.nio.InterfaceJSONNIOTransport;
import edu.umass.cs.gns.nio.JSONMessageExtractor;
import edu.umass.cs.gns.nio.JSONMessenger;
import edu.umass.cs.gns.nio.JSONNIOTransport;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.Shutdownable;
import edu.umass.cs.gns.util.NetworkUtils;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.logging.Logger;

/**
 *
 * @author westy
 */
public class LocalNameServer implements RequestHandlerInterface, Shutdownable {

  public static final int REQUEST_ACTIVES_QUERY_TIMEOUT = 1000; 
  public static final int MAX_QUERY_WAIT_TIME = 4000;
 
  private static final Logger LOG = Logger.getLogger(LocalNameServer.class.getName());
  
  public final static int DEFAULT_LNS_TCP_PORT = 24398;
  
  private static final ConcurrentMap<Integer, LNSRequestInfo> outstandingRequests = new ConcurrentHashMap<>(10, 0.75f, 3);

  private final ScheduledThreadPoolExecutor executorService = new ScheduledThreadPoolExecutor(5);
   
  private InterfaceJSONNIOTransport tcpTransport;
  private final LNSNodeConfig nodeConfig;
  private final InetSocketAddress nodeAddress;
  private final AbstractPacketDemultiplexer demultiplexer;
  private boolean debuggingEnabled = true;
  
  public LocalNameServer(InetSocketAddress nodeAddress, LNSNodeConfig gnsNodeConfig) {
    this.nodeAddress = nodeAddress;
    this.nodeConfig = gnsNodeConfig;
    this.demultiplexer = new LNSPacketDemultiplexer(this);
    try {
      this.tcpTransport = initTransport(demultiplexer);
    } catch (IOException e) {
      LOG.info("Unabled to start LNS listener: " + e);
    }
  }

  private InterfaceJSONNIOTransport initTransport(AbstractPacketDemultiplexer demultiplexer) throws IOException {
    LOG.info("Starting LNS listener on " + nodeAddress);
    JSONNIOTransport gnsNiot = new JSONNIOTransport(nodeAddress, nodeConfig, new JSONMessageExtractor(demultiplexer));
    new Thread(gnsNiot).start();
    // id is null here because we're the LNS
    return new JSONMessenger<>(gnsNiot);
  }

  @Override
  public void shutdown() {
    tcpTransport.stop();
    nodeConfig.shutdown();
    demultiplexer.stop();
  }
  
  public static void main(String[] args) {
    try {
    String nodeFilename;
    if (args.length == 1) {
       nodeFilename = args[0];
    } else { // special case for testing
      nodeFilename = Config.WESTY_GNS_DIR_PATH + "/conf/name-server-info";
    }
    InetSocketAddress address = new InetSocketAddress(NetworkUtils.getLocalHostLANAddress().getHostAddress(),
             DEFAULT_LNS_TCP_PORT);
    LNSNodeConfig nodeConfig = new LNSNodeConfig(nodeFilename);
    new LocalNameServer(address, nodeConfig);
    } catch (IOException e) {
      System.out.println("Usage: java -cp GNS.jar edu.umass.cs.gns.localnameserver <nodeConfigFile>");
    }    
  }

  @Override
  public InterfaceJSONNIOTransport getTcpTransport() {
    return tcpTransport;
  }

  @Override
  public LNSNodeConfig getNodeConfig() {
    return nodeConfig;
  }

  @Override
  public InetSocketAddress getNodeAddress() {
    return nodeAddress;
  }

  @Override
  public AbstractPacketDemultiplexer getDemultiplexer() {
    return demultiplexer;
  }

  @Override
  public boolean isDebugMode() {
    return debuggingEnabled;
  }

  @Override
  public ScheduledThreadPoolExecutor getExecutorService() {
    return executorService;
  }

  @Override
  public void addRequestInfo(int id, LNSRequestInfo requestInfo) {
    outstandingRequests.put(id, requestInfo);
  }

  @Override
  public LNSRequestInfo removeRequestInfo(int id) {
    return outstandingRequests.remove(id);
  }

  @Override
  public LNSRequestInfo getRequestInfo(int id) {
    return outstandingRequests.get(id);
  }
  
    
  
}
