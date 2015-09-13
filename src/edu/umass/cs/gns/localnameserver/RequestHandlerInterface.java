/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.localnameserver.nodeconfig.LNSConsistentReconfigurableNodeConfig;
import edu.umass.cs.gns.ping.PingManager;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * An interface that supports everything that a GNS-enabled server needs to process packets.
 * 
 * @author westy
 */
public interface RequestHandlerInterface {
  
  /**
   * Returns the node config.
   * 
   * @return the node config
   */
  public LNSConsistentReconfigurableNodeConfig getNodeConfig();

  /**
   * Returns the node address.
   * 
   * @return the node address
   */
  public InetSocketAddress getNodeAddress();
  
  /**
   * Returns the demultiplexer.
   * 
   * @return the demultiplexer
   */
  public AbstractJSONPacketDemultiplexer getDemultiplexer();
  
  /**
   * Adds request info.
   * 
   * @param id
   * @param requestInfo
   */
  public void addRequestInfo(int id, LNSRequestInfo requestInfo);

  /**
   * Removes request info.
   * 
   * @param id
   * @return the removed request info
   */
  public LNSRequestInfo removeRequestInfo(int id);

  /**
   * Retrieves the request info.
   * 
   * @param id
   * @return the request info
   */
  public LNSRequestInfo getRequestInfo(int id);
  
  /**
   * Is debug mode on?
   * 
   * @return
   */
  public boolean isDebugMode();
  
  /**
   * Returns the closest active replica.
   * 
   * @param servers
   * @return
   */
  public InetSocketAddress getClosestReplica(Set<InetSocketAddress> servers);
  
  /**
   * Returns the closest active replica.
   * 
   * @param serverIds
   * @param excludeServers
   * @return
   */
  public InetSocketAddress getClosestReplica(Set<InetSocketAddress> serverIds, Set<InetSocketAddress> excludeServers);
  
  /**
   * Clears the cache.
   */
  public void invalidateCache();
  
  /**
   * Returns true if the name is in the cache.
   * 
   * @param name
   * @return
   */
  public boolean containsCacheEntry(String name);
  
  /**
   * Updates the cache entry with a new value.
   * 
   * @param name
   * @param value
   */
  public void updateCacheEntry(String name, String value);
  
  /**
   * Retrieves the value in the cache if it has not timed out.
   * 
   * @param name
   * @return
   */
  public String getValueIfValid(String name);
  
  /**
   * Updates the active replicas associated with the name in the cache.
   * 
   * @param name
   * @param actives
   */
  public void updateCacheEntry(String name, Set<InetSocketAddress> actives);
  
  /**
   * Retrieves the active replicas associated with the name if they have not timed out.
   * 
   * @param name
   * @return
   */
  public Set<InetSocketAddress> getActivesIfValid(String name);
  
  /**
   * Returns the protocol executor.
   * 
   * @return the protocol executor
   */
  public ProtocolExecutor<InetSocketAddress, ReconfigurationPacket.PacketType, String> getProtocolExecutor();
  
  /**
   * Handles an event.
   * 
   * @param json
   * @return
   * @throws JSONException
   */
  public boolean handleEvent(JSONObject json) throws JSONException;
  
  /**
   * Sends a JSON packet to the closest replica.
   * 
   * @param actives
   * @param packet
   * @throws IOException
   */
  public void sendToClosestReplica(Set<InetSocketAddress> actives, JSONObject packet) throws IOException;
  
  /**
   * Sends a JSON packet to a client.
   * 
   * @param isa
   * @param msg
   * @throws IOException
   */
  public void sendToClient(InetSocketAddress isa, JSONObject msg) throws IOException;
  
  /**
   * Returns the ping manager.
   * 
   * @return the ping manager
   */
  public PingManager<InetSocketAddress> getPingManager();
  
  /**
   * Returns the active replicas.
   * 
   * @param name
   * @return
   */
  public Set<InetSocketAddress> getReplicatedActives(String name);
 
}
