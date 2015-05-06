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
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * An interface that supports everything that a GNS-enabled server needs to process packets.
 * 
 * @author westy
 */
public interface RequestHandlerInterface {
  
  public InterfaceJSONNIOTransport getTcpTransport();

  public LNSConsistentReconfigurableNodeConfig getNodeConfig();

  public InetSocketAddress getNodeAddress();
  
  public AbstractPacketDemultiplexer getDemultiplexer();
  
  public ScheduledThreadPoolExecutor getExecutorService();
  
  public void addRequestInfo(int id, LNSRequestInfo requestInfo);

  public LNSRequestInfo removeRequestInfo(int id);

  public LNSRequestInfo getRequestInfo(int id);
  
  public PendingTasks getPendingTasks();
  
  public boolean isDebugMode();
  
  public InetSocketAddress getClosestServer(Set<InetSocketAddress> servers);
  
  public InetSocketAddress getClosestServer(Set<InetSocketAddress> serverIds, Set<InetSocketAddress> excludeServers);
  
}
