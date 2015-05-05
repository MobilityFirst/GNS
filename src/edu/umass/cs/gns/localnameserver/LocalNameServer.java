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
import edu.umass.cs.gns.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.Shutdownable;
import edu.umass.cs.gns.util.NetworkUtils;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.logging.Logger;

/**
 *
 * @author westy
 */
public class LocalNameServer<NodeIDType> implements Shutdownable {

  private static final Logger LOG = Logger.getLogger(LocalNameServer.class.getName());
  
  public final static int DEFAULT_LNS_TCP_PORT = 24398;

  private InterfaceJSONNIOTransport<NodeIDType> tcpTransport;
  private final GNSNodeConfig<NodeIDType> nodeConfig;
  private final InetSocketAddress nodeAddress;
  private final AbstractPacketDemultiplexer demultiplexer;
  
  public LocalNameServer(InetSocketAddress nodeAddress, GNSNodeConfig<NodeIDType> gnsNodeConfig) {
    this.nodeAddress = nodeAddress;
    this.nodeConfig = gnsNodeConfig;
    this.demultiplexer = new LNSPacketDemultiplexer(this);
    try {
      this.tcpTransport = initTransport(demultiplexer);
    } catch (IOException e) {
      LOG.info("Unabled to start LNS listener: " + e);
    }
  }

  private InterfaceJSONNIOTransport<NodeIDType> initTransport(AbstractPacketDemultiplexer demultiplexer) throws IOException {
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
    GNSNodeConfig nodeConfig = new GNSNodeConfig(nodeFilename, true);
    new LocalNameServer(address, nodeConfig);
    } catch (IOException e) {
      System.out.println("Usage: java -cp GNS.jar edu.umass.cs.gns.localnameserver <nodeConfigFile>");
    }    
  }

  public InterfaceJSONNIOTransport<NodeIDType> getTcpTransport() {
    return tcpTransport;
  }

  public GNSNodeConfig<NodeIDType> getNodeConfig() {
    return nodeConfig;
  }

  public InetSocketAddress getNodeAddress() {
    return nodeAddress;
  }

  public AbstractPacketDemultiplexer getDemultiplexer() {
    return demultiplexer;
  }
  
  
}
