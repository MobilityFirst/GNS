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
import edu.umass.cs.gns.protocoltask.ProtocolExecutor;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.json.JSONException;
import org.json.JSONObject;

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
  
  public boolean isDebugMode();
  
  public InetSocketAddress getClosestServer(Set<InetSocketAddress> servers);
  
  public InetSocketAddress getClosestServer(Set<InetSocketAddress> serverIds, Set<InetSocketAddress> excludeServers);
  
  public void invalidateCache();
  
  public boolean containsCacheEntry(String name);
  
  public void updateCacheEntry(String name, String value);
  
  public String getValueIfValid(String name);
  
  public void updateCacheEntry(String name, Set<InetSocketAddress> actives);
  
  public Set<InetSocketAddress> getActivesIfValid(String name);
  
  public ProtocolExecutor getProtocolExecutor();
  
  public boolean handleEvent(JSONObject json) throws JSONException;
  
  public void sendToClosestServer(Set<InetSocketAddress> actives, JSONObject packet) throws IOException;
 
}
