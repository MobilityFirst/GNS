/**
 * Mobility First - Global Name Resolution Service (GNS)
 * Copyright (C) 2013 University of Massachusetts - Emmanuel Cecchet.
 * Contact: cecchet@cs.umass.edu
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. 
 *
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): ______________________.
 */

package edu.umass.cs.msocket;

import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketAddress;
import java.util.List;

/**
 * This class defines the interface for multipath operations
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public interface MultipathInterface
{

  /**
   * Provides a list of IP:port that can be used by flow paths to bind locally.
   * Calling this method multiple times overwrites the values to be used for
   * future flowpaths but it does not change the bindings of currently active
   * flow paths.
   * 
   * @param bindings list of IP:ports to pick from when creating a new flow
   *          path.
   * @throws IOException if an error occurs
   */
  public void bind(List<SocketAddress> bindings) throws IOException;

  /**
   * Return the list of current IP:port used for binding new flow paths locally
   * 
   * @return list of currently defined bindings
   */
  public List<SocketAddress> getBindings();

  /**
   * Add a flow path to the current connection. An eventual binding point can be
   * provided to force a specific path to be taken.
   * 
   * @param binding local IP:port to bind or null to use the currently defined
   *          list of bindings (if no binding is defined, the system will choose
   *          an interface automatically)
   * @return a flowpath id or null if the limit of flowpaths has been reached
   */
  public FlowPath addFlowPath(SocketAddress binding);

  /**
   * Returns the list of currently active flow paths.
   * 
   * @return a list of MSocket representing the different active flowpaths
   */
  public List<FlowPath> getActiveFlowPaths();

  /**
   * Remove a flow path from the current connection
   * 
   * @param flowpath the flowpath to remove
   * @throws IOException if the flowpath doesn't exist
   */
   public void removeFlowPath(FlowPath flowpath) throws IOException;

  /**
   * Set the maximum number of flowpath allowed on this connection. The value
   * must be at least 1.
   * 
   * @param limit maximum number of flowpaths
   */
  public void setMaxFlowPath(int limit);

  /**
   * Defines the multipath policy to use when adding a new flowpath
   * 
   * @param policy a policy from {@link MultipathPolicy}
   */
  public void setFlowPathPolicy(MultipathPolicy policy);

  /**
   * Migrate a flowpath to the new local interface provided the new IP:port
   * 
   * @param flowpath the flowpath to migrate
   * @param localAddress new local address to bind the listening socket
   * @param localPort new local port to bind the listening socket
   */
  public void migrateFlowPath(FlowPath flowpath, InetAddress localAddress, int localPort);
}