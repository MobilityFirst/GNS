package edu.umass.cs.gnsserver.nodeconfig;

import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableNodeConfig;

import java.util.Set;

/**
 * @param <NodeIDType>
 */

/** An interface to translate from integer IDs to socket addresses.
 * 
 * @param <NodeIDType>
 */
public interface GNSInterfaceNodeConfig<NodeIDType> extends Stringifiable<NodeIDType>,
        ReconfigurableNodeConfig<NodeIDType> {

  @Override
  public Set<NodeIDType> getReconfigurators();

  @Override
  public Set<NodeIDType> getActiveReplicas();

  /**
   * Returns the administrator port for the given node.
   *
   * @param id
   * @return an int
   */
  public abstract int getAdminPort(NodeIDType id);

  /**
   * Returns the ping port for the given node.
   *
   * @param id
   * @return an int
   */
  public abstract int getPingPort(NodeIDType id);
  
  /**
   * Returns the Ccp for the given node.
   *
   * @param id
   * @return an int
   */
  public abstract int getCcpPort(NodeIDType id);
  
  /**
   * Returns the Ccp ping port for the given node.
   *
   * @param id
   * @return an int
   */
  public abstract int getCcpPingPort(NodeIDType id);
  
  /**
   * Returns the Ccp admin port for the given node.
   *
   * @param id
   * @return and id
   */
  public abstract int getCcpAdminPort(NodeIDType id);

  /**
   * Returns the average ping latency to the given node.
   * Returns GNSNodeConfig.INVALID_PING_LATENCY if the value cannot be determined.
   *
   * @param id
   * @return an int
   */
  public long getPingLatency(NodeIDType id);

  /**
   * Stores the average ping latency to the given node.
   *
   * @param id
   * @param responseTime
   */
  public void updatePingLatency(NodeIDType id, long responseTime);

  /**
   * Returns the version number of the NodeConfig.
   *
   * @return a long
   */
  public abstract long getVersion();

}
