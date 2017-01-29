
package edu.umass.cs.gnsserver.localnameserver;

import edu.umass.cs.gnsserver.localnameserver.nodeconfig.LNSConsistentReconfigurableNodeConfig;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;


public interface RequestHandlerInterface {
  

  public LNSConsistentReconfigurableNodeConfig getNodeConfig();


  public InetSocketAddress getNodeAddress();
  

  public AbstractJSONPacketDemultiplexer getDemultiplexer();
  

  public void addRequestInfo(long id, LNSRequestInfo requestInfo, NIOHeader header);


  public LNSRequestInfo removeRequestInfo(long id);


  public LNSRequestInfo getRequestInfo(long id);
  

  public InetSocketAddress getClosestReplica(Set<InetSocketAddress> servers);
  

  public InetSocketAddress getClosestReplica(Set<InetSocketAddress> serverIds, Set<InetSocketAddress> excludeServers);
  

  public void invalidateCache();
  

  public boolean containsCacheEntry(String name);
  

  public void updateCacheEntry(String name, String value);
  

  public String getValueIfValid(String name);
  

  public void updateCacheEntry(String name, Set<InetSocketAddress> actives);
  

  public void invalidateCacheEntry(String name);
  

  public Set<InetSocketAddress> getActivesIfValid(String name);
  

  public ProtocolExecutor<InetSocketAddress, ReconfigurationPacket.PacketType, String> getProtocolExecutor();
  

  public boolean handleEvent(JSONObject json) throws JSONException;
  

  public void sendToClosestReplica(Set<InetSocketAddress> actives, JSONObject packet) throws IOException;
  

  public void sendToClient(InetSocketAddress isa, JSONObject msg) throws IOException;
  

  public Set<InetSocketAddress> getReplicatedActives(String name);
 
}
