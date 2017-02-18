/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsserver.nodeconfig;

import edu.umass.cs.reconfiguration.interfaces.ReconfigurableNodeConfig;
import java.util.Set;

/**
 * @param <NodeIDType>
 */

/** An interface to translate from integer IDs to socket addresses.
 * 
 * @param <NodeIDType>
 */
public interface GNSInterfaceNodeConfig<NodeIDType> extends
        ReconfigurableNodeConfig<NodeIDType> {

  /**
   *
   * @return a set of node ids
   */
  @Override
  public Set<NodeIDType> getReconfigurators();

  /**
   *
   * @return a set of node ids
   */
  @Override
  public Set<NodeIDType> getActiveReplicas();

  /**
   * Returns the administrator port for the given node.
   *
   * @param id
   * @return an int
   */
  public abstract int getServerAdminPort(NodeIDType id);
  
  /**
   * Returns the Ccp admin port for the given node.
   *
   * @param id
   * @return and id
   */
  public abstract int getCollatingAdminPort(NodeIDType id);

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
